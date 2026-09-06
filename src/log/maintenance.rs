//! Explicit partition maintenance. A completed inventory precedes the root
//! fence; only its unreachable keys can be collected after a complete trace.

use super::*;
use std::collections::BTreeMap;

#[cfg(test)]
mod tests;

/// Limits for a single partition maintenance invocation.
#[derive(Clone, Debug)]
pub struct MaintenanceOptions {
    /// Expire complete oldest batches received at least this many milliseconds ago.
    pub max_age_ms: Option<u64>,
    /// Retain at most this many bytes of encoded record objects.
    pub max_bytes: Option<u64>,
    /// Publish retention and collect objects. False performs a read-only preview.
    pub apply: bool,
    /// Maximum listed entries and maximum objects in each complete live graph.
    pub max_objects: usize,
}

impl Default for MaintenanceOptions {
    fn default() -> Self {
        Self {
            max_age_ms: None,
            max_bytes: None,
            apply: false,
            max_objects: 100_000,
        }
    }
}

impl MaintenanceOptions {
    /// Check local limits before any object-store request.
    pub fn validate(&self) -> Result<(), LogError> {
        if !(1..=1_000_000).contains(&self.max_objects) {
            return Err(LogError::InvalidMaintenance {
                detail: "max-objects must be between 1 and 1000000".into(),
            });
        }
        Ok(())
    }
}

/// Observed retention plan and collection progress. Byte counts refer to
/// visible object sizes, not noncurrent versions or billed bucket storage.
#[derive(Clone, Debug, Serialize)]
pub struct MaintenanceReport {
    pub topic: String,
    pub partition: i32,
    pub applied: bool,
    /// A failed root request may have committed, but never authorizes deletion.
    pub publication_uncertain: bool,
    pub previous_start_offset: i64,
    pub start_offset: i64,
    pub next_offset: i64,
    pub max_age_ms: Option<u64>,
    pub max_bytes: Option<u64>,
    pub format_adoption: bool,
    pub retained_batches: usize,
    pub retained_record_bytes: u64,
    pub expired_batches: usize,
    pub expired_record_bytes: u64,
    pub removable_objects: usize,
    pub removable_bytes: u64,
    /// Successful deletes, including candidates already absent.
    pub collected_objects: usize,
    /// Inventory sizes of successfully collected candidates.
    pub collected_bytes: u64,
}

impl std::fmt::Display for MaintenanceReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mode = if self.publication_uncertain {
            "Publication uncertain; no deletion"
        } else if self.applied {
            "Applied"
        } else {
            "Preview"
        };
        writeln!(
            f,
            "{mode}: topic {} partition {}",
            self.topic, self.partition
        )?;
        writeln!(
            f,
            "Readable offsets: [{}, {}) -> [{}, {})",
            self.previous_start_offset, self.next_offset, self.start_offset, self.next_offset
        )?;
        writeln!(
            f,
            "Limits: age {} ms; record bytes {}",
            self.max_age_ms.map_or("none".into(), |n| n.to_string()),
            self.max_bytes.map_or("none".into(), |n| n.to_string())
        )?;
        writeln!(
            f,
            "Retained: {} batches, {} record bytes",
            self.retained_batches, self.retained_record_bytes
        )?;
        writeln!(
            f,
            "Expired: {} batches, {} record bytes",
            self.expired_batches, self.expired_record_bytes
        )?;
        writeln!(
            f,
            "Collectible: {} objects, {} visible bytes",
            self.removable_objects, self.removable_bytes
        )?;
        writeln!(
            f,
            "Collected or already absent: {} objects ({} inventory bytes)",
            self.collected_objects, self.collected_bytes
        )?;
        if self.format_adoption {
            writeln!(
                f,
                "Format adoption: schema 4; stored age metadata is preserved"
            )?;
        }
        if !self.applied && !self.publication_uncertain {
            writeln!(f, "No objects changed. Use --apply to run maintenance.")?;
        }
        Ok(())
    }
}

pub(super) struct LiveGraph {
    pub segments: Vec<Segment>,
    pub objects: HashSet<String>,
}

impl LiveGraph {
    pub fn insert(&mut self, object: &str, maximum: usize) -> Result<(), LogError> {
        if self.objects.len() >= maximum {
            return Err(LogError::MaintenanceBudget { maximum });
        }
        if !self.objects.insert(object.to_owned()) {
            return Err(index::invalid("duplicate object in live graph"));
        }
        Ok(())
    }
}

impl LogEngine {
    /// Preview or apply whole-batch retention and collect unreachable objects.
    /// No topic is created, and preview performs no writes or deletions.
    pub async fn maintain(
        &self,
        topic: &str,
        partition: i32,
        options: &MaintenanceOptions,
    ) -> Result<MaintenanceReport, LogError> {
        options.validate()?;
        validate_topic(topic)?;
        validate_partition(partition, MAX_TOPIC_PARTITIONS)?;
        let count =
            self.topic_partition_count(topic)
                .await?
                .ok_or_else(|| LogError::UnknownTopic {
                    topic: topic.to_owned(),
                })?;
        validate_partition(partition, count)?;
        self.maintain_at(topic, partition, options, unix_millis()?)
            .await
    }

    async fn maintain_at(
        &self,
        topic: &str,
        partition: i32,
        options: &MaintenanceOptions,
        now_ms: u64,
    ) -> Result<MaintenanceReport, LogError> {
        let mut last_publication = None;
        let result = self
            .maintain_attempts(topic, partition, options, now_ms, &mut last_publication)
            .await;
        match (result, last_publication) {
            (Err(error @ LogError::MaintenanceIncomplete { .. }), _) => Err(error),
            (Err(error), Some(report)) => Err(incomplete(report, error)),
            (result, _) => result,
        }
    }

    async fn maintain_attempts(
        &self,
        topic: &str,
        partition: i32,
        options: &MaintenanceOptions,
        now_ms: u64,
        last_publication: &mut Option<MaintenanceReport>,
    ) -> Result<MaintenanceReport, LogError> {
        for _ in 0..MAX_CAS_ATTEMPTS {
            // This set is closed before loading/fencing the root. Fresh tentative
            // objects from later attempts can never enter this deletion pass.
            let inventory = self
                .inventory(topic, partition, options.max_objects)
                .await?;
            let loaded = self
                .load_manifest(topic, partition)
                .await?
                .unwrap_or_else(LoadedManifest::empty);
            let graph = match self
                .trace_manifest(topic, partition, &loaded.manifest, options.max_objects)
                .await
            {
                Ok(graph) => graph,
                Err(error) => {
                    if self
                        .stale_missing(topic, partition, &loaded.version, &error)
                        .await?
                    {
                        continue;
                    }
                    return Err(error);
                }
            };
            let previous_start_offset = loaded.manifest.start_offset();
            let next_offset = loaded.manifest.next_offset();
            let (revision, adoption, format_adoption) = match &loaded.manifest {
                LogManifest::Legacy(m) => (m.revision, now_ms, true),
                LogManifest::Indexed(root) => (
                    root.revision,
                    root.adopted_at_ms.unwrap_or(now_ms),
                    root.schema != INDEX_SCHEMA,
                ),
            };
            let total_bytes: u64 = graph.segments.iter().map(|s| s.byte_length).sum();
            let mut retained_bytes = total_bytes;
            let mut expired = 0;
            for segment in &graph.segments {
                let age_expired = options.max_age_ms.is_some_and(|age| {
                    segment
                        .received_at_ms
                        .unwrap_or(adoption)
                        .checked_add(age)
                        .is_some_and(|expires| expires <= now_ms)
                });
                let size_expired = options
                    .max_bytes
                    .is_some_and(|limit| retained_bytes > limit);
                if !age_expired && !size_expired {
                    break;
                }
                expired += 1;
                retained_bytes -= segment.byte_length;
            }
            let start_offset = graph
                .segments
                .get(expired)
                .map_or(next_offset, |s| s.base_offset);
            let retained = &graph.segments[expired..];
            // Retention rebuilds the small-page index; no record bytes change.
            // With no trimming, an indexed root reuses its live pages.
            let rebuild = expired != 0 || matches!(&loaded.manifest, LogManifest::Legacy(_));
            let producers = match &loaded.manifest {
                LogManifest::Indexed(root) => root.producers.clone(),
                _ => None,
            };
            let producer_namespace =
                format!("{}/topics/{topic}/{partition}/producer-state/", self.prefix);
            let planned_live: HashSet<String> = if rebuild {
                retained
                    .iter()
                    .map(|s| s.object.clone())
                    .chain(
                        graph
                            .objects
                            .iter()
                            .filter(|key| key.starts_with(&producer_namespace))
                            .cloned(),
                    )
                    .collect()
            } else {
                graph.objects.clone()
            };
            let removable: Vec<_> = inventory
                .into_iter()
                .filter(|(key, _)| !planned_live.contains(key))
                .collect();
            let mut report = MaintenanceReport {
                topic: topic.into(),
                partition,
                applied: false,
                publication_uncertain: false,
                previous_start_offset,
                start_offset,
                next_offset,
                max_age_ms: options.max_age_ms,
                max_bytes: options.max_bytes,
                format_adoption,
                retained_batches: retained.len(),
                retained_record_bytes: retained_bytes,
                expired_batches: expired,
                expired_record_bytes: total_bytes - retained_bytes,
                removable_objects: removable.len(),
                removable_bytes: removable.iter().try_fold(0_u64, |total, (_, size)| {
                    total.checked_add(*size).ok_or(LogError::OffsetOverflow)
                })?,
                collected_objects: 0,
                collected_bytes: 0,
            };
            if !options.apply {
                return Ok(report);
            }
            let mut root = if rebuild {
                self.rebuild_index(
                    topic,
                    partition,
                    retained,
                    start_offset,
                    next_offset,
                    options.max_objects,
                )
                .await?
            } else {
                match loaded.manifest {
                    LogManifest::Indexed(root) => root,
                    _ => unreachable!(),
                }
            };
            root.producers = producers;
            root.schema = INDEX_SCHEMA;
            root.adopted_at_ms = Some(adoption);
            // Monotonic revision prevents identical-body/ETag ABA, even for an
            // empty GC-only pass. It no longer equals retained segment count.
            root.revision = revision.checked_add(1).ok_or(LogError::RevisionOverflow)?;
            root.validate(&self.prefix, topic, partition)?;
            let bytes = Bytes::from(serde_json::to_vec(&root)?);
            if bytes.len() > MAX_MANIFEST_BYTES {
                return Err(index::invalid("maintenance root exceeds byte limit"));
            }
            let mode = loaded
                .version
                .clone()
                .map_or(PutMode::Create, PutMode::Update);
            let result = self
                .store
                .put_opts(
                    &self.manifest_path(topic, partition),
                    bytes.into(),
                    mode.into(),
                )
                .await;
            let version = match result {
                Ok(result) => Some(UpdateVersion {
                    e_tag: result.e_tag,
                    version: result.version,
                }),
                Err(StoreError::Precondition { .. } | StoreError::AlreadyExists { .. }) => continue,
                Err(error) => {
                    report.publication_uncertain = true;
                    return Err(incomplete(report, error.into()));
                }
            };
            report.applied = true;
            *last_publication = Some(report.clone());
            let live = match self
                .trace_manifest(
                    topic,
                    partition,
                    &LogManifest::Indexed(root),
                    options.max_objects,
                )
                .await
            {
                Ok(live) => live,
                Err(error) => match self.stale_missing(topic, partition, &version, &error).await {
                    Ok(true) => continue,
                    Ok(false) => return Err(incomplete(report, error)),
                    Err(error) => return Err(incomplete(report, error)),
                },
            };
            // Recheck membership using the complete committed graph. Never use
            // the fetch traversal, which deliberately skips out-of-range pages.
            for (object, size) in removable {
                if live.objects.contains(&object) {
                    continue;
                }
                match self.store.delete(&Path::from(object)).await {
                    Ok(()) | Err(StoreError::NotFound { .. }) => {
                        report.collected_objects += 1;
                        report.collected_bytes += size;
                    }
                    Err(error) => return Err(incomplete(report, error.into())),
                }
            }
            return Ok(report);
        }
        Err(LogError::ContentionExhausted {
            attempts: MAX_CAS_ATTEMPTS,
        })
    }

    async fn inventory(
        &self,
        topic: &str,
        partition: i32,
        maximum: usize,
    ) -> Result<BTreeMap<String, u64>, LogError> {
        let namespace = format!("{}/topics/{topic}/{partition}/", self.prefix);
        let mut objects = BTreeMap::new();
        let mut scanned = 0;
        for (directory, suffix) in [
            ("segments", ".batch"),
            ("index", ".json"),
            ("producer-state", ".json"),
        ] {
            let prefix = format!("{namespace}{directory}/");
            let path = Path::from(prefix.clone());
            let mut listed = self.store.list(Some(&path));
            while let Some(object) = listed.next().await {
                let object = object?;
                scanned += 1;
                if scanned > maximum {
                    return Err(LogError::MaintenanceBudget { maximum });
                }
                let key = object.location.to_string();
                let recognized = key
                    .strip_prefix(&prefix)
                    .and_then(|name| name.strip_suffix(suffix))
                    .and_then(|name| Uuid::parse_str(name).ok())
                    .is_some();
                if recognized {
                    objects.insert(key, object.size);
                }
            }
        }
        Ok(objects)
    }

    async fn trace_manifest(
        &self,
        topic: &str,
        partition: i32,
        manifest: &LogManifest,
        maximum: usize,
    ) -> Result<LiveGraph, LogError> {
        let mut graph = LiveGraph {
            segments: Vec::new(),
            objects: HashSet::new(),
        };
        match manifest {
            LogManifest::Legacy(legacy) => graph.segments = legacy.segments.clone(),
            LogManifest::Indexed(root) => {
                self.trace_index(topic, partition, root, &mut graph, maximum)
                    .await?;
                self.trace_producers(topic, partition, root, &mut graph, maximum)
                    .await?
            }
        }
        for segment in &graph.segments {
            if graph.objects.len() >= maximum {
                return Err(LogError::MaintenanceBudget { maximum });
            }
            if !graph.objects.insert(segment.object.clone()) {
                return Err(index::invalid("duplicate live record object"));
            }
            let meta = self.store.head(&Path::from(segment.object.clone())).await?;
            if meta.size != segment.byte_length {
                return Err(index::invalid("live record object length mismatch"));
            }
        }
        Ok(graph)
    }
}

fn incomplete(report: MaintenanceReport, source: LogError) -> LogError {
    LogError::MaintenanceIncomplete {
        report: Box::new(report),
        source: Box::new(source),
    }
}
