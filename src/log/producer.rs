//! Durable producer allocation and an immutable directory committed by the log root.

use super::*;

#[cfg(test)]
pub(crate) mod tests;
use index::invalid;

const RETRY_BATCHES: usize = 5;
const MAX_LEVEL: u8 = 12;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProducerRef {
    pub object: String,
    first_id: i64,
    last_id: i64,
    count: u64,
    level: u8,
    byte_length: u64,
    sha256: String,
}

impl ProducerRef {
    pub fn validate(&self, prefix: &str, topic: &str, partition: i32) -> Result<(), LogError> {
        let namespace = format!("{prefix}/topics/{topic}/{partition}/producer-state/");
        let name = self
            .object
            .strip_prefix(&namespace)
            .and_then(|s| s.strip_suffix(".json"));
        if name.is_none_or(|s| Uuid::parse_str(s).is_err())
            || self.first_id < 0
            || self.last_id < self.first_id
            || self.count == 0
            || self.count > (self.last_id - self.first_id) as u64 + 1
            || self.level > MAX_LEVEL
            || self.count
                > (PAGE_ENTRIES as u64)
                    .checked_pow(u32::from(self.level) + 1)
                    .unwrap_or(u64::MAX)
            || self.byte_length == 0
            || self.byte_length > MAX_MANIFEST_BYTES as u64
            || !valid_checksum(&self.sha256)
        {
            return Err(invalid("invalid producer page reference"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProducerState {
    pub id: i64,
    pub epoch: i16,
    #[serde(deserialize_with = "history")]
    batches: Vec<BatchIdentity>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BatchIdentity {
    sequence: i32,
    count: u32,
    base_offset: i64,
    fingerprint: String,
}

fn advance(sequence: i32, count: u32) -> i32 {
    ((sequence as u64 + u64::from(count)) % (i32::MAX as u64 + 1)) as i32
}

impl ProducerState {
    pub fn new(id: i64, epoch: i16) -> Self {
        Self {
            id,
            epoch,
            batches: Vec::new(),
        }
    }

    // Update a private snapshot only. No objects are written until every batch
    // in the partition request has passed this decision.
    pub fn accept(
        &mut self,
        epoch: i16,
        sequence: i32,
        count: u32,
        fingerprint: &str,
        next_offset: i64,
    ) -> Result<AppendResult, LogError> {
        if epoch < self.epoch {
            return Err(LogError::InvalidProducerEpoch);
        }
        if epoch > self.epoch {
            if sequence != 0 {
                return Err(LogError::OutOfOrderSequence);
            }
            self.epoch = epoch;
            self.batches.clear();
        }
        if let Some(batch) = self.batches.iter().find(|b| b.sequence == sequence) {
            if batch.count != count || batch.fingerprint != fingerprint {
                return Err(LogError::ConflictingSequence);
            }
            return Ok(AppendResult {
                base_offset: batch.base_offset,
                last_offset: batch.base_offset + i64::from(count) - 1,
            });
        }
        let expected = self
            .batches
            .last()
            .map_or(0, |b| advance(b.sequence, b.count));
        if sequence != expected {
            return Err(LogError::OutOfOrderSequence);
        }
        let end = next_offset
            .checked_add(i64::from(count))
            .ok_or(LogError::OffsetOverflow)?;
        self.batches.push(BatchIdentity {
            sequence,
            count,
            base_offset: next_offset,
            fingerprint: fingerprint.into(),
        });
        if self.batches.len() > RETRY_BATCHES {
            self.batches.remove(0);
        }
        Ok(AppendResult {
            base_offset: next_offset,
            last_offset: end - 1,
        })
    }

    fn validate(&self, next_offset: i64) -> Result<(), LogError> {
        if self.id < 0
            || self.epoch < 0
            || self.batches.is_empty()
            || self.batches.len() > RETRY_BATCHES
        {
            return Err(invalid("invalid producer state"));
        }
        let mut previous: Option<&BatchIdentity> = None;
        for batch in &self.batches {
            if batch.sequence < 0
                || batch.count == 0
                || batch.count as usize > MAX_BATCH_RECORDS
                || batch.base_offset < 0
                || batch
                    .base_offset
                    .checked_add(i64::from(batch.count))
                    .is_none_or(|end| end > next_offset)
                || !valid_checksum(&batch.fingerprint)
                || previous.is_some_and(|p| {
                    advance(p.sequence, p.count) != batch.sequence
                        || p.base_offset + i64::from(p.count) > batch.base_offset
                })
            {
                return Err(invalid("invalid producer batch history"));
            }
            previous = Some(batch);
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum ProducerPage {
    Leaf {
        #[serde(deserialize_with = "entries")]
        producers: Vec<ProducerState>,
    },
    Branch {
        #[serde(deserialize_with = "entries")]
        children: Vec<ProducerRef>,
    },
}

fn entries<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Vec<T>, D::Error> {
    deserialize_bounded::<D, T, PAGE_ENTRIES>(d)
}
fn history<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<BatchIdentity>, D::Error> {
    deserialize_bounded::<D, BatchIdentity, RETRY_BATCHES>(d)
}

impl ProducerPage {
    fn bounds(
        &self,
        prefix: &str,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(i64, i64, u64, u8), LogError> {
        match self {
            Self::Leaf { producers } => {
                if producers.is_empty() || producers.len() > PAGE_ENTRIES {
                    return Err(invalid("invalid producer leaf size"));
                }
                for p in producers {
                    p.validate(next_offset)?;
                }
                if producers.windows(2).any(|p| p[0].id >= p[1].id) {
                    return Err(invalid("unordered producer leaf"));
                }
                Ok((
                    producers[0].id,
                    producers.last().unwrap().id,
                    producers.len() as u64,
                    0,
                ))
            }
            Self::Branch { children } => {
                if children.len() < 2 || children.len() > PAGE_ENTRIES {
                    return Err(invalid("invalid producer branch size"));
                }
                let first = &children[0];
                let mut count = 0_u64;
                for child in children {
                    child.validate(prefix, topic, partition)?;
                    count = count
                        .checked_add(child.count)
                        .ok_or_else(|| invalid("producer count overflow"))?;
                    if child.level != first.level {
                        return Err(invalid("unequal producer page levels"));
                    }
                }
                if children.windows(2).any(|c| c[0].last_id >= c[1].first_id) {
                    return Err(invalid("overlapping producer pages"));
                }
                Ok((
                    first.first_id,
                    children.last().unwrap().last_id,
                    count,
                    first.level + 1,
                ))
            }
        }
    }
}

impl LogEngine {
    /// Allocate a fresh nontransactional producer ID. A lost response consumes
    /// an ID; retrying initialization must never reuse one.
    pub async fn allocate_producer_id(&self) -> Result<i64, LogError> {
        #[derive(Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Allocator {
            schema: u32,
            next_id: i64,
        }
        let object = Path::from(format!("{}/producer-ids.json", self.prefix));
        for _ in 0..MAX_CAS_ATTEMPTS {
            let (id, mode) = match self.store.get(&object).await {
                Ok(result) => {
                    let version = UpdateVersion {
                        e_tag: result.meta.e_tag.clone(),
                        version: result.meta.version.clone(),
                    };
                    let bytes = collect_bounded(result, MAX_TOPIC_METADATA_BYTES)
                        .await
                        .map_err(|e| match e {
                            BoundedReadError::Store(e) => LogError::ObjectStore(e),
                            BoundedReadError::TooLarge => {
                                invalid("producer allocator exceeds byte limit")
                            }
                        })?;
                    let state: Allocator =
                        serde_json::from_slice(&bytes).map_err(|e| invalid(e.to_string()))?;
                    if state.schema != 1 || state.next_id < 0 {
                        return Err(invalid("invalid producer allocator"));
                    }
                    (state.next_id, PutMode::Update(version))
                }
                Err(StoreError::NotFound { .. }) => (0, PutMode::Create),
                Err(e) => return Err(e.into()),
            };
            let next_id = id.checked_add(1).ok_or(LogError::OffsetOverflow)?;
            let bytes = Bytes::from(serde_json::to_vec(&Allocator { schema: 1, next_id })?);
            match self
                .store
                .put_opts(&object, bytes.into(), mode.into())
                .await
            {
                Ok(_) => return Ok(id),
                Err(StoreError::Precondition { .. } | StoreError::AlreadyExists { .. }) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(LogError::ContentionExhausted {
            attempts: MAX_CAS_ATTEMPTS,
        })
    }

    async fn read_producer_page(
        &self,
        topic: &str,
        partition: i32,
        reference: &ProducerRef,
        next_offset: i64,
    ) -> Result<ProducerPage, LogError> {
        reference.validate(&self.prefix, topic, partition)?;
        let result = self
            .store
            .get(&Path::from(reference.object.clone()))
            .await?;
        if result.meta.size != reference.byte_length {
            return Err(invalid("producer page length mismatch"));
        }
        let bytes = collect_bounded(result, reference.byte_length as usize)
            .await
            .map_err(|e| match e {
                BoundedReadError::Store(e) => LogError::ObjectStore(e),
                BoundedReadError::TooLarge => invalid("producer page exceeds reference length"),
            })?;
        if bytes.len() as u64 != reference.byte_length || sha256_hex(&bytes) != reference.sha256 {
            return Err(invalid("producer page length or checksum mismatch"));
        }
        let page: ProducerPage =
            serde_json::from_slice(&bytes).map_err(|e| invalid(e.to_string()))?;
        if page.bounds(&self.prefix, topic, partition, next_offset)?
            != (
                reference.first_id,
                reference.last_id,
                reference.count,
                reference.level,
            )
        {
            return Err(invalid("producer page reference mismatch"));
        }
        Ok(page)
    }

    async fn write_producer_page(
        &self,
        topic: &str,
        partition: i32,
        page: ProducerPage,
        next_offset: i64,
    ) -> Result<ProducerRef, LogError> {
        let (first_id, last_id, count, level) =
            page.bounds(&self.prefix, topic, partition, next_offset)?;
        let bytes = Bytes::from(serde_json::to_vec(&page)?);
        let reference = ProducerRef {
            object: format!(
                "{}/topics/{topic}/{partition}/producer-state/{}.json",
                self.prefix,
                Uuid::new_v4()
            ),
            first_id,
            last_id,
            count,
            level,
            byte_length: bytes.len() as u64,
            sha256: sha256_hex(&bytes),
        };
        reference.validate(&self.prefix, topic, partition)?;
        self.store
            .put_opts(
                &Path::from(reference.object.clone()),
                bytes.into(),
                PutMode::Create.into(),
            )
            .await?;
        Ok(reference)
    }

    pub(super) async fn producer_state(
        &self,
        topic: &str,
        partition: i32,
        root: Option<&ProducerRef>,
        id: i64,
        next_offset: i64,
    ) -> Result<Option<ProducerState>, LogError> {
        let mut cursor = root.cloned();
        while let Some(reference) = cursor {
            if id < reference.first_id || id > reference.last_id {
                return Ok(None);
            }
            match self
                .read_producer_page(topic, partition, &reference, next_offset)
                .await?
            {
                ProducerPage::Leaf { producers } => {
                    return Ok(producers.into_iter().find(|p| p.id == id));
                }
                ProducerPage::Branch { children } => {
                    cursor = children
                        .into_iter()
                        .find(|c| c.first_id <= id && id <= c.last_id)
                }
            }
        }
        Ok(None)
    }

    pub(super) async fn update_producer(
        &self,
        topic: &str,
        partition: i32,
        root: Option<&ProducerRef>,
        state: ProducerState,
        next_offset: i64,
    ) -> Result<ProducerRef, LogError> {
        let mut path = Vec::new();
        let mut cursor = root.cloned();
        let mut producers = Vec::new();
        while let Some(reference) = cursor {
            match self
                .read_producer_page(topic, partition, &reference, next_offset)
                .await?
            {
                ProducerPage::Leaf { producers: leaf } => {
                    producers = leaf;
                    break;
                }
                ProducerPage::Branch { children } => {
                    let position = children
                        .iter()
                        .position(|c| state.id <= c.last_id)
                        .unwrap_or(children.len() - 1);
                    cursor = Some(children[position].clone());
                    path.push((children, position));
                }
            }
        }
        match producers.binary_search_by_key(&state.id, |p| p.id) {
            Ok(i) => producers[i] = state,
            Err(i) => producers.insert(i, state),
        }
        let mut pages = Vec::new();
        if producers.len() > PAGE_ENTRIES {
            let right = producers.split_off(producers.len() / 2);
            pages.push(ProducerPage::Leaf { producers });
            pages.push(ProducerPage::Leaf { producers: right });
        } else {
            pages.push(ProducerPage::Leaf { producers });
        }
        loop {
            let mut refs = Vec::new();
            for page in pages {
                refs.push(
                    self.write_producer_page(topic, partition, page, next_offset)
                        .await?,
                );
            }
            let Some((mut children, position)) = path.pop() else {
                return if refs.len() == 1 {
                    Ok(refs.remove(0))
                } else {
                    self.write_producer_page(
                        topic,
                        partition,
                        ProducerPage::Branch { children: refs },
                        next_offset,
                    )
                    .await
                };
            };
            children.splice(position..=position, refs);
            pages = Vec::new();
            if children.len() > PAGE_ENTRIES {
                let right = children.split_off(children.len() / 2);
                pages.push(ProducerPage::Branch { children });
                pages.push(ProducerPage::Branch { children: right });
            } else {
                pages.push(ProducerPage::Branch { children });
            }
        }
    }

    pub(super) async fn trace_producers(
        &self,
        topic: &str,
        partition: i32,
        root: &Root,
        graph: &mut maintenance::LiveGraph,
        maximum: usize,
    ) -> Result<(), LogError> {
        let mut pending: Vec<_> = root.producers.iter().cloned().collect();
        while let Some(reference) = pending.pop() {
            graph.insert(&reference.object, maximum)?;
            if let ProducerPage::Branch { children } = self
                .read_producer_page(topic, partition, &reference, root.next_offset)
                .await?
            {
                pending.extend(children);
            }
        }
        Ok(())
    }
}
