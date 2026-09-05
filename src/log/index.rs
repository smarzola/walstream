//! Immutable offset index. The manifest root owns a bounded active tail;
//! sealed leaves and their ancestors are published before the root CAS.

use super::*;

#[cfg(test)]
mod tests;

pub(super) const INDEX_SCHEMA: u32 = 2;
pub(super) const PAGE_ENTRIES: usize = 64;
// 64^(10+1) exceeds the positive i64 offset space. A strict decreasing level
// bounds traversal even if durable references are maliciously cyclic.
const MAX_LEVEL: u8 = 10;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Root {
    pub schema: u32,
    pub revision: u64,
    pub next_offset: i64,
    pub tree: Option<PageRef>,
    #[serde(deserialize_with = "entries")]
    pub tail: Vec<Segment>,
}

impl Default for Root {
    fn default() -> Self {
        Self {
            schema: INDEX_SCHEMA,
            revision: 0,
            next_offset: 0,
            tree: None,
            tail: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PageRef {
    pub object: String,
    pub first_offset: i64,
    pub next_offset: i64,
    pub segment_count: u64,
    pub level: u8,
    pub byte_length: u64,
    pub sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Page {
    Leaf {
        #[serde(deserialize_with = "entries")]
        segments: Vec<Segment>,
    },
    Branch {
        #[serde(deserialize_with = "entries")]
        children: Vec<PageRef>,
    },
}

fn entries<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    deserialize_bounded::<D, T, PAGE_ENTRIES>(deserializer)
}

fn invalid(detail: impl Into<String>) -> LogError {
    LogError::InvalidManifest {
        detail: detail.into(),
    }
}

impl PageRef {
    fn validate(&self, prefix: &str, topic: &str, partition: i32) -> Result<(), LogError> {
        let namespace = format!("{prefix}/topics/{topic}/{partition}/index/");
        let name = self
            .object
            .strip_prefix(&namespace)
            .and_then(|s| s.strip_suffix(".json"));
        if name.is_none_or(|name| Uuid::parse_str(name).is_err())
            || self.level > MAX_LEVEL
            || self.first_offset < 0
            || self.next_offset <= self.first_offset
            || self.segment_count == 0
            || self.segment_count > (self.next_offset - self.first_offset) as u64
            || self.segment_count > self.capacity()
            || self.byte_length == 0
            || self.byte_length > MAX_MANIFEST_BYTES as u64
            || !valid_checksum(&self.sha256)
        {
            return Err(invalid("invalid index page reference"));
        }
        Ok(())
    }

    fn capacity(&self) -> u64 {
        (PAGE_ENTRIES as u64)
            .checked_pow(u32::from(self.level) + 1)
            .unwrap_or(u64::MAX)
    }

    fn full(&self) -> bool {
        self.segment_count == self.capacity()
    }
}

impl Root {
    pub fn validate(&self, prefix: &str, topic: &str, partition: i32) -> Result<(), LogError> {
        if self.schema != INDEX_SCHEMA || self.tail.len() > PAGE_ENTRIES {
            return Err(invalid("unsupported or oversized index root"));
        }
        let (start, count) = if let Some(tree) = &self.tree {
            tree.validate(prefix, topic, partition)?;
            if tree.first_offset != 0 {
                return Err(invalid("index must start at offset zero"));
            }
            (tree.next_offset, tree.segment_count)
        } else {
            (0, 0)
        };
        let next = validate_segments(&self.tail, start, prefix, topic, partition)?;
        if next != self.next_offset
            || count.checked_add(self.tail.len() as u64) != Some(self.revision)
            || (self.tail.is_empty() && self.tree.is_some())
        {
            return Err(invalid("index root range or revision is inconsistent"));
        }
        Ok(())
    }
}

impl LogEngine {
    async fn write_page(
        &self,
        topic: &str,
        partition: i32,
        page: Page,
    ) -> Result<PageRef, LogError> {
        let (first_offset, next_offset, segment_count, level) = match &page {
            Page::Leaf { segments } => {
                let first = segments
                    .first()
                    .ok_or_else(|| invalid("empty index leaf"))?;
                let next =
                    validate_segments(segments, first.base_offset, &self.prefix, topic, partition)?;
                (first.base_offset, next, segments.len() as u64, 0)
            }
            Page::Branch { children } => {
                validate_children(children, &self.prefix, topic, partition)?
            }
        };
        let bytes = Bytes::from(serde_json::to_vec(&page)?);
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(invalid("index page exceeds byte limit"));
        }
        let object = Path::from(format!(
            "{}/topics/{topic}/{partition}/index/{}.json",
            self.prefix,
            Uuid::new_v4()
        ));
        let reference = PageRef {
            object: object.to_string(),
            first_offset,
            next_offset,
            segment_count,
            level,
            byte_length: bytes.len() as u64,
            sha256: sha256_hex(&bytes),
        };
        reference.validate(&self.prefix, topic, partition)?;
        self.store
            .put_opts(&object, bytes.into(), PutMode::Create.into())
            .await?;
        Ok(reference)
    }

    async fn read_page(
        &self,
        topic: &str,
        partition: i32,
        reference: &PageRef,
    ) -> Result<Page, LogError> {
        reference.validate(&self.prefix, topic, partition)?;
        let result = self
            .store
            .get(&Path::from(reference.object.clone()))
            .await?;
        if result.meta.size != reference.byte_length {
            return Err(invalid("index page length mismatch"));
        }
        let bytes = collect_bounded(result, reference.byte_length as usize)
            .await
            .map_err(|e| match e {
                BoundedReadError::Store(e) => LogError::ObjectStore(e),
                BoundedReadError::TooLarge => invalid("index page body exceeds reference length"),
            })?;
        if bytes.len() as u64 != reference.byte_length || sha256_hex(&bytes) != reference.sha256 {
            return Err(invalid("index page length or checksum mismatch"));
        }
        let page: Page = serde_json::from_slice(&bytes).map_err(|e| invalid(e.to_string()))?;
        let actual = match &page {
            Page::Leaf { segments } => {
                if segments.len() != PAGE_ENTRIES {
                    return Err(invalid("sealed index leaf must be full"));
                }
                let end = validate_segments(
                    segments,
                    reference.first_offset,
                    &self.prefix,
                    topic,
                    partition,
                )?;
                (reference.first_offset, end, segments.len() as u64, 0)
            }
            Page::Branch { children } => {
                validate_children(children, &self.prefix, topic, partition)?
            }
        };
        if actual
            != (
                reference.first_offset,
                reference.next_offset,
                reference.segment_count,
                reference.level,
            )
        {
            return Err(invalid(
                "index page does not match parent range, count, or level",
            ));
        }
        Ok(page)
    }

    // Build just the new right edge when a full subtree receives another leaf.
    async fn index_path(
        &self,
        topic: &str,
        partition: i32,
        mut leaf: PageRef,
        level: u8,
    ) -> Result<PageRef, LogError> {
        while leaf.level < level {
            leaf = self
                .write_page(
                    topic,
                    partition,
                    Page::Branch {
                        children: vec![leaf],
                    },
                )
                .await?;
        }
        Ok(leaf)
    }

    async fn insert_leaf(
        &self,
        topic: &str,
        partition: i32,
        tree: PageRef,
        leaf: PageRef,
    ) -> Result<PageRef, LogError> {
        if tree.full() {
            if tree.level == MAX_LEVEL {
                return Err(LogError::OffsetOverflow);
            }
            let right = self.index_path(topic, partition, leaf, tree.level).await?;
            return self
                .write_page(
                    topic,
                    partition,
                    Page::Branch {
                        children: vec![tree, right],
                    },
                )
                .await;
        }
        let Page::Branch { mut children } = self.read_page(topic, partition, &tree).await? else {
            return Err(invalid("incomplete sealed leaf"));
        };
        let last = children
            .pop()
            .ok_or_else(|| invalid("empty index branch"))?;
        if last.full() {
            let right = self.index_path(topic, partition, leaf, last.level).await?;
            children.extend([last, right]);
        } else {
            children.push(Box::pin(self.insert_leaf(topic, partition, last, leaf)).await?);
        }
        self.write_page(topic, partition, Page::Branch { children })
            .await
    }

    pub(super) async fn seal_tail(
        &self,
        topic: &str,
        partition: i32,
        root: &mut Root,
    ) -> Result<(), LogError> {
        let leaf = self
            .write_page(
                topic,
                partition,
                Page::Leaf {
                    segments: std::mem::take(&mut root.tail),
                },
            )
            .await?;
        root.tree = Some(match root.tree.take() {
            Some(tree) => self.insert_leaf(topic, partition, tree, leaf).await?,
            None => leaf,
        });
        Ok(())
    }

    pub(super) async fn migrate_manifest(
        &self,
        topic: &str,
        partition: i32,
        legacy: Manifest,
    ) -> Result<Root, LogError> {
        let mut root = Root::default();
        for segment in legacy.segments {
            if root.tail.len() == PAGE_ENTRIES {
                self.seal_tail(topic, partition, &mut root).await?;
            }
            root.tail.push(segment);
        }
        root.revision = legacy.revision;
        root.next_offset = legacy.next_offset;
        Ok(root)
    }

    pub(super) async fn indexed_segments(
        &self,
        topic: &str,
        partition: i32,
        root: Root,
        selection: &mut Selection,
    ) -> Result<(), LogError> {
        let mut stack: Vec<PageRef> = root.tree.into_iter().collect();
        while let Some(reference) = stack.pop() {
            if reference.next_offset <= selection.offset {
                continue;
            }
            match self.read_page(topic, partition, &reference).await? {
                Page::Leaf { segments } => {
                    for segment in segments {
                        if !selection.push(segment)? {
                            return Ok(());
                        }
                    }
                }
                Page::Branch { children } => {
                    stack.extend(
                        children
                            .into_iter()
                            .rev()
                            .filter(|r| r.next_offset > selection.offset),
                    );
                }
            }
        }
        for segment in root.tail {
            if !selection.push(segment)? {
                break;
            }
        }
        Ok(())
    }
}

fn validate_children(
    children: &[PageRef],
    prefix: &str,
    topic: &str,
    partition: i32,
) -> Result<(i64, i64, u64, u8), LogError> {
    let first = children
        .first()
        .ok_or_else(|| invalid("empty index branch"))?;
    if children.len() > PAGE_ENTRIES || first.level >= MAX_LEVEL {
        return Err(invalid("index branch exceeds limit"));
    }
    let mut next = first.first_offset;
    let mut count = 0_u64;
    let mut objects = HashSet::new();
    for (index, child) in children.iter().enumerate() {
        child.validate(prefix, topic, partition)?;
        if child.first_offset != next
            || child.level != first.level
            || !objects.insert(&child.object)
            || (index + 1 < children.len() && !child.full())
        {
            return Err(invalid("index children have inconsistent ranges or shape"));
        }
        next = child.next_offset;
        count = count
            .checked_add(child.segment_count)
            .ok_or(LogError::OffsetOverflow)?;
    }
    Ok((first.first_offset, next, count, first.level + 1))
}

pub(super) struct Selection {
    offset: i64,
    remaining: usize,
    allow_oversized: bool,
    pub segments: Vec<Segment>,
    pub oversized_first_batch: bool,
}

impl Selection {
    pub fn new(offset: i64, maximum_bytes: usize, allow_oversized: bool) -> Self {
        Self {
            offset,
            remaining: maximum_bytes,
            allow_oversized,
            segments: Vec::new(),
            oversized_first_batch: false,
        }
    }

    // False means the response is complete; do not read later index pages.
    pub fn push(&mut self, segment: Segment) -> Result<bool, LogError> {
        if segment.last_offset()? < self.offset {
            return Ok(true);
        }
        let length = usize::try_from(segment.byte_length).map_err(|_| LogError::OffsetOverflow)?;
        if length > self.remaining {
            if self.segments.is_empty() && self.allow_oversized {
                self.segments.push(segment);
                self.oversized_first_batch = true;
            }
            return Ok(false);
        }
        self.remaining -= length;
        self.segments.push(segment);
        Ok(self.remaining != 0)
    }
}
