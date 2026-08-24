//! Deliberately bounded Kafka protocol handling.

use std::net::IpAddr;

use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::{
    error::ResponseError,
    messages::{
        ApiKey, BrokerId, RequestKind, ResponseHeader, ResponseKind, TopicName,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        fetch_request::FetchRequest,
        fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
        list_offsets_request::ListOffsetsRequest,
        list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
        },
        metadata_request::MetadataRequest,
        metadata_response::{
            MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
            MetadataResponseTopic,
        },
        produce_request::ProduceRequest,
        produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
    },
    protocol::{Decodable, Encodable, StrBytes, decode_request_header_from_buffer},
};
use thiserror::Error;

use crate::{
    codec::{CodecError, decode_record_batches},
    log::{LogEngine, LogError},
    wire::validate_request_frame,
};

const BROKER_ID: i32 = 1;
const FETCH_RESPONSE_BASE_RESERVE: usize = 4 * 1024;
const FETCH_RESPONSE_ITEM_RESERVE: usize = 512;
/// Broker-side cap independent of the client-provided Fetch max_bytes.
pub const MAX_FETCH_PAYLOAD_BYTES: usize = 1024 * 1024;

/// Kafka API/version surface advertised by Walstream.
pub const SUPPORTED_APIS: &[(ApiKey, i16, i16)] = &[
    (ApiKey::Produce, 7, 7),
    (ApiKey::Fetch, 4, 4),
    (ApiKey::ListOffsets, 3, 3),
    (ApiKey::Metadata, 4, 4),
    (ApiKey::ApiVersions, 0, 3),
];

/// Stable identity returned in broker metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerIdentity {
    pub host: String,
    pub port: u16,
    pub cluster_id: String,
}

impl BrokerIdentity {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if !valid_advertised_host(&self.host) || self.port == 0 || self.cluster_id.trim().is_empty()
        {
            return Err(ProtocolError::InvalidBrokerIdentity);
        }
        Ok(())
    }
}

fn valid_advertised_host(host: &str) -> bool {
    if host.parse::<IpAddr>().is_ok() {
        return true;
    }
    !host.is_empty()
        && host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                && label
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
        })
}

/// Decode and execute one Kafka request frame (without the length prefix).
///
/// `None` is returned only for a valid Produce request with `acks=0`.
pub async fn handle_request(
    mut frame: Bytes,
    engine: &LogEngine,
    identity: &BrokerIdentity,
) -> Result<Option<Bytes>, ProtocolError> {
    identity.validate()?;
    if frame.len() < 8 {
        return Err(ProtocolError::FrameTooSmall {
            actual: frame.len(),
        });
    }

    let raw_key = i16::from_be_bytes([frame[0], frame[1]]);
    let version = i16::from_be_bytes([frame[2], frame[3]]);
    let api_key = ApiKey::try_from(raw_key).map_err(|_| ProtocolError::UnknownApi(raw_key))?;
    let supported = is_supported(api_key, version);
    let unsupported_api_versions = api_key == ApiKey::ApiVersions && (0..=4).contains(&version);
    if !supported && !unsupported_api_versions {
        return Err(if SUPPORTED_APIS.iter().any(|entry| entry.0 == api_key) {
            ProtocolError::UnsupportedVersion {
                api_key: raw_key,
                version,
            }
        } else {
            ProtocolError::UnsupportedApi(raw_key)
        });
    }
    validate_request_frame(&frame, api_key, version).map_err(|source| {
        ProtocolError::MalformedRequest {
            api_key: raw_key,
            version,
            detail: source.to_string(),
        }
    })?;
    let header = decode_request_header_from_buffer(&mut frame).map_err(|source| {
        ProtocolError::MalformedRequest {
            api_key: raw_key,
            version,
            detail: source.to_string(),
        }
    })?;

    if !supported {
        if unsupported_api_versions {
            let _ = kafka_protocol::messages::api_versions_request::ApiVersionsRequest::decode(
                &mut frame, version,
            )
            .map_err(|source| ProtocolError::MalformedRequest {
                api_key: raw_key,
                version,
                detail: source.to_string(),
            })?;
            if frame.has_remaining() {
                return Err(ProtocolError::TrailingRequestBytes(frame.remaining()));
            }
            let response = ApiVersionsResponse::default()
                .with_error_code(ResponseError::UnsupportedVersion.code());
            return encode_response(
                header.correlation_id,
                api_key,
                version,
                ResponseKind::ApiVersions(response),
            )
            .map(Some);
        }

        unreachable!("all other unsupported APIs return before wire decoding");
    }

    let request = RequestKind::decode(api_key, &mut frame, version).map_err(|source| {
        ProtocolError::MalformedRequest {
            api_key: raw_key,
            version,
            detail: source.to_string(),
        }
    })?;
    if frame.has_remaining() {
        return Err(ProtocolError::TrailingRequestBytes(frame.remaining()));
    }

    let (response, send_response) = match request {
        RequestKind::ApiVersions(_) => (ResponseKind::ApiVersions(api_versions_response()), true),
        RequestKind::Metadata(request) => (
            ResponseKind::Metadata(metadata_response(request, version, engine, identity).await?),
            true,
        ),
        RequestKind::Produce(request) => {
            let send_response = request.acks != 0;
            (
                ResponseKind::Produce(produce_response(request, engine).await),
                send_response,
            )
        }
        RequestKind::Fetch(request) => (
            ResponseKind::Fetch(fetch_response(request, engine).await),
            true,
        ),
        RequestKind::ListOffsets(request) => (
            ResponseKind::ListOffsets(list_offsets_response(request, engine).await),
            true,
        ),
        _ => return Err(ProtocolError::UnsupportedApi(raw_key)),
    };

    if !send_response {
        return Ok(None);
    }
    encode_response(header.correlation_id, api_key, version, response).map(Some)
}

fn is_supported(api_key: ApiKey, version: i16) -> bool {
    SUPPORTED_APIS
        .iter()
        .any(|(key, min, max)| *key == api_key && (*min..=*max).contains(&version))
}

fn api_versions_response() -> ApiVersionsResponse {
    let api_keys = SUPPORTED_APIS
        .iter()
        .map(|(key, min, max)| {
            ApiVersion::default()
                .with_api_key(*key as i16)
                .with_min_version(*min)
                .with_max_version(*max)
        })
        .collect();
    ApiVersionsResponse::default().with_api_keys(api_keys)
}

async fn metadata_response(
    request: MetadataRequest,
    version: i16,
    engine: &LogEngine,
    identity: &BrokerIdentity,
) -> Result<MetadataResponse, ProtocolError> {
    let explicit_topics = request.topics.is_some();
    let requested = match request.topics {
        Some(topics) => {
            let mut names = Vec::with_capacity(topics.len());
            for topic in topics {
                let name = topic.name.ok_or_else(|| ProtocolError::MalformedRequest {
                    api_key: ApiKey::Metadata as i16,
                    version,
                    detail: "metadata topic name is null".into(),
                })?;
                names.push(name.0.as_str().to_owned());
            }
            names
        }
        None => engine
            .topics()
            .await
            .map_err(|error| ProtocolError::Storage(error.to_string()))?,
    };

    let mut topics = Vec::with_capacity(requested.len());
    for topic in requested {
        let should_create = version < 4 || request.allow_auto_topic_creation;
        let result = if !explicit_topics {
            Ok(())
        } else if should_create {
            engine.ensure_topic(&topic, 0).await
        } else {
            match engine.topic_exists(&topic, 0).await {
                Ok(true) => Ok(()),
                Ok(false) => Err(LogError::UnknownTopic {
                    topic: topic.clone(),
                }),
                Err(error) => Err(error),
            }
        };

        let response = match result {
            Ok(()) => MetadataResponseTopic::default()
                .with_name(Some(topic_name(topic)))
                .with_partitions(vec![
                    MetadataResponsePartition::default()
                        .with_partition_index(0)
                        .with_leader_id(BrokerId(BROKER_ID))
                        .with_replica_nodes(vec![BrokerId(BROKER_ID)])
                        .with_isr_nodes(vec![BrokerId(BROKER_ID)]),
                ]),
            Err(LogError::InvalidTopic { .. }) => MetadataResponseTopic::default()
                .with_name(Some(topic_name(topic)))
                .with_error_code(ResponseError::InvalidTopicException.code()),
            Err(LogError::UnknownTopic { .. }) => MetadataResponseTopic::default()
                .with_name(Some(topic_name(topic)))
                .with_error_code(ResponseError::UnknownTopicOrPartition.code()),
            Err(_) => MetadataResponseTopic::default()
                .with_name(Some(topic_name(topic)))
                .with_error_code(ResponseError::KafkaStorageError.code()),
        };
        topics.push(response);
    }

    Ok(MetadataResponse::default()
        .with_brokers(vec![
            MetadataResponseBroker::default()
                .with_node_id(BrokerId(BROKER_ID))
                .with_host(StrBytes::from_string(identity.host.clone()))
                .with_port(i32::from(identity.port)),
        ])
        .with_cluster_id(Some(StrBytes::from_string(identity.cluster_id.clone())))
        .with_controller_id(BrokerId(BROKER_ID))
        .with_topics(topics))
}

async fn produce_response(request: ProduceRequest, engine: &LogEngine) -> ProduceResponse {
    let invalid_acks = !matches!(request.acks, -1..=1);
    let transactional = request.transactional_id.is_some();
    let mut topics = Vec::with_capacity(request.topic_data.len());

    for topic in request.topic_data {
        let name = topic.name.0.as_str().to_owned();
        let mut partitions = Vec::with_capacity(topic.partition_data.len());
        for partition in topic.partition_data {
            let (error_code, base_offset) = if invalid_acks {
                (ResponseError::InvalidRequiredAcks.code(), -1)
            } else if transactional {
                (ResponseError::UnsupportedForMessageFormat.code(), -1)
            } else {
                match decode_records(partition.records) {
                    Ok(records) => match engine.append(&name, partition.index, records).await {
                        Ok(result) => (0, result.base_offset),
                        Err(error) => (log_error_code(&error), -1),
                    },
                    Err(error_code) => (error_code, -1),
                }
            };
            partitions.push(
                PartitionProduceResponse::default()
                    .with_index(partition.index)
                    .with_error_code(error_code)
                    .with_base_offset(base_offset),
            );
        }
        topics.push(
            TopicProduceResponse::default()
                .with_name(topic_name(name))
                .with_partition_responses(partitions),
        );
    }

    ProduceResponse::default().with_responses(topics)
}

fn decode_records(records: Option<Bytes>) -> Result<Vec<kafka_protocol::records::Record>, i16> {
    let records = records.ok_or(ResponseError::InvalidRequest.code())?;
    if records.is_empty() {
        return Err(ResponseError::InvalidRequest.code());
    }
    match decode_record_batches(records) {
        Ok((_, records)) if records.is_empty() => Err(ResponseError::InvalidRequest.code()),
        Ok((_, records)) => Ok(records),
        Err(CodecError::UnsupportedCompression) => {
            Err(ResponseError::UnsupportedCompressionType.code())
        }
        Err(CodecError::TooManyRecords | CodecError::TooManyHeaders) => {
            Err(ResponseError::MessageTooLarge.code())
        }
        Err(CodecError::DuplicateHeaders) => Err(ResponseError::UnsupportedForMessageFormat.code()),
        Err(CodecError::Malformed | CodecError::ArithmeticOverflow) => {
            Err(ResponseError::CorruptMessage.code())
        }
    }
}

async fn fetch_response(request: FetchRequest, engine: &LogEngine) -> FetchResponse {
    let item_count = request
        .topics
        .iter()
        .fold(request.topics.len(), |count, topic| {
            count.saturating_add(topic.partitions.len())
        });
    // The Kafka limit is primarily a record-data budget. Reserving more than
    // the maximum encoded topic/partition envelope keeps ordinary responses
    // below max_bytes without reading record objects speculatively.
    let overhead_reserve = FETCH_RESPONSE_BASE_RESERVE
        .saturating_add(FETCH_RESPONSE_ITEM_RESERVE.saturating_mul(item_count));
    let requested_max = usize::try_from(request.max_bytes)
        .unwrap_or_default()
        .min(MAX_FETCH_PAYLOAD_BYTES);
    let mut remaining = requested_max.saturating_sub(overhead_reserve);
    let invalid_request_max = request.max_bytes <= 0;
    let follower_fetch = request.replica_id != BrokerId(-1);
    let mut first_nonempty_partition = true;
    let mut topics = Vec::with_capacity(request.topics.len());

    for topic in request.topics {
        let name = topic.topic.0.as_str().to_owned();
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for partition in topic.partitions {
            let requested_partition_max = usize::try_from(partition.partition_max_bytes)
                .unwrap_or_default()
                .min(MAX_FETCH_PAYLOAD_BYTES);
            let partition_limit = requested_partition_max.min(remaining);
            let mut response = PartitionData::default().with_partition_index(partition.partition);

            if invalid_request_max || partition.partition_max_bytes <= 0 {
                response.error_code = ResponseError::InvalidFetchSize.code();
            } else if follower_fetch {
                response.error_code = ResponseError::InvalidRequest.code();
            } else {
                match engine
                    .fetch_bounded(
                        &name,
                        partition.partition,
                        partition.fetch_offset,
                        partition_limit,
                        first_nonempty_partition,
                    )
                    .await
                {
                    Ok(fetched) => {
                        response.high_watermark = fetched.high_watermark;
                        response.last_stable_offset = fetched.high_watermark;
                        if !fetched.records.is_empty() {
                            first_nonempty_partition = false;
                            remaining = remaining.saturating_sub(fetched.records.len());
                        }
                        response.records = Some(fetched.records);
                    }
                    Err(error) => response.error_code = log_error_code(&error),
                }
            }
            partitions.push(response);
        }
        topics.push(
            FetchableTopicResponse::default()
                .with_topic(topic_name(name))
                .with_partitions(partitions),
        );
    }

    FetchResponse::default().with_responses(topics)
}

async fn list_offsets_response(
    request: ListOffsetsRequest,
    engine: &LogEngine,
) -> ListOffsetsResponse {
    let follower_fetch = request.replica_id != BrokerId(-1);
    let mut topics = Vec::with_capacity(request.topics.len());
    for topic in request.topics {
        let name = topic.name.0.as_str().to_owned();
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for partition in topic.partitions {
            let mut response = ListOffsetsPartitionResponse::default()
                .with_partition_index(partition.partition_index);
            if follower_fetch {
                response.error_code = ResponseError::InvalidRequest.code();
            } else {
                match engine.offsets(&name, partition.partition_index).await {
                    Ok(offsets) => match partition.timestamp {
                        -2 => response.offset = offsets.earliest,
                        -1 => response.offset = offsets.latest,
                        _ => response.error_code = ResponseError::InvalidRequest.code(),
                    },
                    Err(error) => response.error_code = log_error_code(&error),
                }
            }
            partitions.push(response);
        }
        topics.push(
            ListOffsetsTopicResponse::default()
                .with_name(topic_name(name))
                .with_partitions(partitions),
        );
    }
    ListOffsetsResponse::default().with_topics(topics)
}

fn log_error_code(error: &LogError) -> i16 {
    match error {
        LogError::InvalidTopic { .. } => ResponseError::InvalidTopicException.code(),
        LogError::UnknownTopic { .. } => ResponseError::UnknownTopicOrPartition.code(),
        LogError::UnsupportedPartition { .. } => ResponseError::UnknownTopicOrPartition.code(),
        LogError::EmptyBatch => ResponseError::InvalidRequest.code(),
        LogError::UnsupportedRecordSemantics => ResponseError::UnsupportedForMessageFormat.code(),
        LogError::InvalidTimestampRange => ResponseError::InvalidTimestamp.code(),
        LogError::InvalidOffset { .. } | LogError::OffsetOutOfRange { .. } => {
            ResponseError::OffsetOutOfRange.code()
        }
        LogError::BatchTooLarge { .. } | LogError::TooManyRecords { .. } => {
            ResponseError::MessageTooLarge.code()
        }
        LogError::InvalidManifest { .. }
        | LogError::MissingSegment { .. }
        | LogError::CorruptSegment { .. }
        | LogError::Codec { .. } => ResponseError::CorruptMessage.code(),
        LogError::OffsetOverflow
        | LogError::RevisionOverflow
        | LogError::SegmentLimit { .. }
        | LogError::ManifestTooLarge { .. }
        | LogError::Serialization(_)
        | LogError::ObjectStore(_)
        | LogError::ContentionExhausted { .. } => ResponseError::KafkaStorageError.code(),
    }
}

fn topic_name(name: String) -> TopicName {
    TopicName(StrBytes::from_string(name))
}

fn encode_response(
    correlation_id: i32,
    api_key: ApiKey,
    version: i16,
    response: ResponseKind,
) -> Result<Bytes, ProtocolError> {
    let mut encoded = BytesMut::new();
    ResponseHeader::default()
        .with_correlation_id(correlation_id)
        .encode(&mut encoded, response.header_version(version))
        .map_err(|source| ProtocolError::Encode(source.to_string()))?;
    response
        .encode(&mut encoded, version)
        .map_err(|source| ProtocolError::Encode(source.to_string()))?;

    // Keep the key in the signature so call sites cannot accidentally encode
    // a response for a different request family.
    debug_assert!(matches!(
        (api_key, &response),
        (ApiKey::Produce, ResponseKind::Produce(_))
            | (ApiKey::Fetch, ResponseKind::Fetch(_))
            | (ApiKey::ListOffsets, ResponseKind::ListOffsets(_))
            | (ApiKey::Metadata, ResponseKind::Metadata(_))
            | (ApiKey::ApiVersions, ResponseKind::ApiVersions(_))
    ));
    Ok(encoded.freeze())
}

/// Connection-safe protocol failure.
#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("Kafka frame is too small: {actual} bytes")]
    FrameTooSmall { actual: usize },
    #[error("unknown Kafka API key {0}")]
    UnknownApi(i16),
    #[error("Kafka API key {0} is not implemented")]
    UnsupportedApi(i16),
    #[error("Kafka API key {api_key} version {version} is not supported")]
    UnsupportedVersion { api_key: i16, version: i16 },
    #[error("malformed Kafka API key {api_key} version {version}: {detail}")]
    MalformedRequest {
        api_key: i16,
        version: i16,
        detail: String,
    },
    #[error("Kafka request has {0} trailing bytes")]
    TrailingRequestBytes(usize),
    #[error("cannot encode Kafka response: {0}")]
    Encode(String),
    #[error("object-store metadata lookup failed: {0}")]
    Storage(String),
    #[error("advertised broker identity must contain a host, non-zero port, and cluster id")]
    InvalidBrokerIdentity,
}

#[cfg(test)]
mod tests {
    use std::{
        fmt,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use kafka_protocol::{
        indexmap::IndexMap,
        messages::{
            RequestHeader, TransactionalId,
            api_versions_request::ApiVersionsRequest,
            fetch_request::{FetchPartition, FetchTopic},
            list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
            metadata_request::MetadataRequestTopic,
            produce_request::{PartitionProduceData, TopicProduceData},
        },
        records::{
            NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record, RecordBatchDecoder,
            TimestampType,
        },
    };
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
        memory::InMemory, path::Path,
    };

    use super::*;
    use crate::{
        codec::{MAX_BATCH_RECORDS, MAX_HEADERS_PER_REQUEST},
        log::encode_records,
    };

    #[derive(Debug)]
    struct InstrumentedStore {
        inner: InMemory,
        segment_gets: AtomicUsize,
        list_calls: AtomicUsize,
    }

    impl InstrumentedStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                segment_gets: AtomicUsize::new(0),
                list_calls: AtomicUsize::new(0),
            }
        }
    }

    impl fmt::Display for InstrumentedStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("instrumented-protocol-test-store")
        }
    }

    #[async_trait]
    impl ObjectStore for InstrumentedStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> StoreResult<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> StoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
            if location.to_string().ends_with(".batch") {
                self.segment_gets.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, StoreResult<Path>>,
        ) -> BoxStream<'static, StoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn record() -> Record {
        Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: NO_SEQUENCE,
            timestamp: 1_000,
            key: None,
            value: Some(Bytes::from_static(b"value")),
            headers: IndexMap::new(),
        }
    }

    fn produce_request(partition: i32) -> ProduceRequest {
        ProduceRequest::default()
            .with_acks(-1)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(topic_name("events".into()))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(partition)
                            .with_records(Some(encode_records(&[record()]).unwrap())),
                    ]),
            ])
    }

    fn request_bytes(
        api_key: ApiKey,
        version: i16,
        correlation_id: i32,
        request: RequestKind,
    ) -> Bytes {
        let mut bytes = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(api_key as i16)
            .with_request_api_version(version)
            .with_correlation_id(correlation_id)
            .encode(&mut bytes, api_key.request_header_version(version))
            .unwrap();
        request.encode(&mut bytes, version).unwrap();
        bytes.freeze()
    }

    fn bare_request_bytes(api_key: ApiKey, version: i16, correlation_id: i32) -> Bytes {
        let mut bytes = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(api_key as i16)
            .with_request_api_version(version)
            .with_correlation_id(correlation_id)
            .encode(&mut bytes, api_key.request_header_version(version))
            .unwrap();
        bytes.freeze()
    }

    async fn wire_round_trip(
        engine: &LogEngine,
        api_key: ApiKey,
        version: i16,
        request: RequestKind,
    ) -> (usize, ResponseKind) {
        let identity = BrokerIdentity {
            host: "localhost".into(),
            port: 9092,
            cluster_id: "matrix".into(),
        };
        let response = handle_request(
            request_bytes(api_key, version, 71, request),
            engine,
            &identity,
        )
        .await
        .unwrap()
        .unwrap();
        let length = response.len();
        let mut response = response;
        let header =
            ResponseHeader::decode(&mut response, api_key.response_header_version(version))
                .unwrap();
        assert_eq!(header.correlation_id, 71);
        let decoded = ResponseKind::decode(api_key, &mut response, version).unwrap();
        assert!(!response.has_remaining());
        (length, decoded)
    }

    #[tokio::test]
    async fn produce_maps_unsupported_semantics_to_explicit_errors() {
        let engine = LogEngine::in_memory("walstream/clusters/protocol-errors").unwrap();

        let response = produce_response(produce_request(7), &engine).await;
        assert_eq!(
            response.responses[0].partition_responses[0].error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );

        let mut transactional = produce_request(0);
        transactional.transactional_id = Some(TransactionalId(StrBytes::from_static_str("tx")));
        let response = produce_response(transactional, &engine).await;
        assert_eq!(
            response.responses[0].partition_responses[0].error_code,
            ResponseError::UnsupportedForMessageFormat.code()
        );

        let mut invalid_acks = produce_request(0);
        invalid_acks.acks = 2;
        let response = produce_response(invalid_acks, &engine).await;
        assert_eq!(
            response.responses[0].partition_responses[0].error_code,
            ResponseError::InvalidRequiredAcks.code()
        );
        assert!(matches!(
            engine.offsets("events", 0).await,
            Err(LogError::UnknownTopic { .. })
        ));
    }

    #[test]
    fn produce_rejects_compression_and_declared_count_before_record_decode() {
        let encoded = encode_records(&[record()]).unwrap();

        let mut compressed = encoded.to_vec();
        compressed[21..23].copy_from_slice(&1_i16.to_be_bytes());
        let checksum = crc32c::crc32c(&compressed[21..]);
        compressed[17..21].copy_from_slice(&checksum.to_be_bytes());
        assert_eq!(
            decode_records(Some(Bytes::from(compressed))),
            Err(ResponseError::UnsupportedCompressionType.code())
        );

        let mut oversized_count = encoded.to_vec();
        oversized_count[57..61].copy_from_slice(&((MAX_BATCH_RECORDS as i32) + 1).to_be_bytes());
        let checksum = crc32c::crc32c(&oversized_count[21..]);
        oversized_count[17..21].copy_from_slice(&checksum.to_be_bytes());
        assert_eq!(
            decode_records(Some(Bytes::from(oversized_count))),
            Err(ResponseError::MessageTooLarge.code())
        );
    }

    fn push_unsigned_var(mut value: u64, output: &mut Vec<u8>) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            output.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn push_var_i32(value: i32, output: &mut Vec<u8>) {
        let encoded = ((value as u32) << 1) ^ ((value >> 31) as u32);
        push_unsigned_var(u64::from(encoded), output);
    }

    fn push_var_i64(value: i64, output: &mut Vec<u8>) {
        let encoded = ((value as u64) << 1) ^ ((value >> 63) as u64);
        push_unsigned_var(encoded, output);
    }

    fn batch_with_record_body(body: &[u8]) -> Vec<u8> {
        batch_with_record_bodies(&[body.to_vec()])
    }

    fn batch_with_record_bodies(bodies: &[Vec<u8>]) -> Vec<u8> {
        let canonical = encode_records(&[record()]).unwrap();
        let mut batch = canonical[..61].to_vec();
        batch[57..61].copy_from_slice(&(bodies.len() as i32).to_be_bytes());
        for body in bodies {
            push_var_i32(body.len() as i32, &mut batch);
            batch.extend_from_slice(body);
        }
        let length = (batch.len() - 12) as i32;
        batch[8..12].copy_from_slice(&length.to_be_bytes());
        let checksum = crc32c::crc32c(&batch[21..]);
        batch[17..21].copy_from_slice(&checksum.to_be_bytes());
        batch
    }

    #[test]
    fn raw_validator_rejects_header_allocation_and_delta_overflow_hazards() {
        let mut reserved_attributes = vec![1];
        push_var_i64(0, &mut reserved_attributes);
        push_var_i32(0, &mut reserved_attributes);
        push_var_i32(-1, &mut reserved_attributes);
        push_var_i32(-1, &mut reserved_attributes);
        push_var_i32(0, &mut reserved_attributes);
        assert_eq!(
            decode_records(Some(Bytes::from(batch_with_record_body(
                &reserved_attributes,
            )))),
            Err(ResponseError::CorruptMessage.code())
        );

        let mut enormous_headers = vec![0]; // attributes
        push_var_i64(0, &mut enormous_headers);
        push_var_i32(0, &mut enormous_headers);
        push_var_i32(-1, &mut enormous_headers);
        push_var_i32(-1, &mut enormous_headers);
        push_var_i32(i32::MAX, &mut enormous_headers);
        assert_eq!(
            decode_records(Some(Bytes::from(batch_with_record_body(&enormous_headers)))),
            Err(ResponseError::MessageTooLarge.code())
        );

        let mut offset_overflow = vec![0];
        push_var_i64(0, &mut offset_overflow);
        push_var_i32(1, &mut offset_overflow);
        push_var_i32(-1, &mut offset_overflow);
        push_var_i32(-1, &mut offset_overflow);
        push_var_i32(0, &mut offset_overflow);
        let mut batch = batch_with_record_body(&offset_overflow);
        batch[0..8].copy_from_slice(&i64::MAX.to_be_bytes());
        assert_eq!(
            decode_records(Some(Bytes::from(batch))),
            Err(ResponseError::CorruptMessage.code())
        );

        let mut timestamp_overflow = vec![0];
        push_var_i64(1, &mut timestamp_overflow);
        push_var_i32(0, &mut timestamp_overflow);
        push_var_i32(-1, &mut timestamp_overflow);
        push_var_i32(-1, &mut timestamp_overflow);
        push_var_i32(0, &mut timestamp_overflow);
        let mut batch = batch_with_record_body(&timestamp_overflow);
        batch[27..35].copy_from_slice(&i64::MAX.to_be_bytes());
        batch[35..43].copy_from_slice(&i64::MAX.to_be_bytes());
        let checksum = crc32c::crc32c(&batch[21..]);
        batch[17..21].copy_from_slice(&checksum.to_be_bytes());
        assert_eq!(
            decode_records(Some(Bytes::from(batch))),
            Err(ResponseError::CorruptMessage.code())
        );
    }

    #[test]
    fn raw_validator_bounds_headers_across_the_entire_request() {
        let record_count = MAX_HEADERS_PER_REQUEST / 1_024 + 1;
        let bodies = (0..record_count)
            .map(|offset| {
                let mut body = vec![0]; // attributes
                push_var_i64(0, &mut body);
                push_var_i32(offset as i32, &mut body);
                push_var_i32(-1, &mut body);
                push_var_i32(-1, &mut body);
                push_var_i32(1_024, &mut body);
                for _ in 0..1_024 {
                    push_var_i32(0, &mut body); // empty header key
                    push_var_i32(-1, &mut body); // null header value
                }
                body
            })
            .collect::<Vec<_>>();
        assert_eq!(
            decode_records(Some(Bytes::from(batch_with_record_bodies(&bodies)))),
            Err(ResponseError::MessageTooLarge.code())
        );
    }

    #[tokio::test]
    async fn produce_rejects_duplicate_headers_instead_of_fetching_deduplicated_data() {
        let mut body = vec![0]; // attributes
        push_var_i64(0, &mut body);
        push_var_i32(0, &mut body);
        push_var_i32(-1, &mut body); // null key
        push_var_i32(1, &mut body);
        body.push(b'v');
        push_var_i32(2, &mut body);
        for &value in b"ab" {
            push_var_i32(3, &mut body);
            body.extend_from_slice(b"dup");
            push_var_i32(1, &mut body);
            body.push(value);
        }

        let engine = LogEngine::in_memory("walstream/clusters/duplicate-headers").unwrap();
        let request = ProduceRequest::default()
            .with_acks(-1)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(topic_name("events".into()))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(0)
                            .with_records(Some(Bytes::from(batch_with_record_body(&body)))),
                    ]),
            ]);
        let response = produce_response(request, &engine).await;
        assert_eq!(
            response.responses[0].partition_responses[0].error_code,
            ResponseError::UnsupportedForMessageFormat.code()
        );

        let fetch = FetchRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_max_bytes(10_000)
            .with_topics(vec![fetch_topic("events")]);
        assert_eq!(
            fetch_response(fetch, &engine).await.responses[0].partitions[0].error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
    }

    fn fetch_topic(name: &str) -> FetchTopic {
        FetchTopic::default()
            .with_topic(topic_name(name.to_owned()))
            .with_partitions(vec![
                FetchPartition::default()
                    .with_partition(0)
                    .with_fetch_offset(0)
                    .with_partition_max_bytes(1),
            ])
    }

    #[tokio::test]
    async fn fetch_applies_first_batch_exception_only_once() {
        let engine = LogEngine::in_memory("walstream/clusters/fetch-budget").unwrap();
        engine.append("alpha", 0, vec![record()]).await.unwrap();
        engine.append("beta", 0, vec![record()]).await.unwrap();
        let request = FetchRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_max_bytes(1)
            .with_topics(vec![fetch_topic("alpha"), fetch_topic("beta")]);

        let response = fetch_response(request, &engine).await;
        assert_eq!(response.responses[0].partitions[0].error_code, 0);
        assert!(
            !response.responses[0].partitions[0]
                .records
                .as_ref()
                .unwrap()
                .is_empty()
        );
        assert_eq!(response.responses[1].partitions[0].error_code, 0);
        assert!(
            response.responses[1].partitions[0]
                .records
                .as_ref()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn broker_fetch_cap_bounds_i32_max_to_one_segment_get() {
        let store = Arc::new(InstrumentedStore::new());
        let engine = LogEngine::new(store.clone(), "walstream/clusters/hard-fetch-cap").unwrap();
        let mut large = record();
        large.value = Some(Bytes::from(vec![7_u8; 700 * 1024]));
        engine
            .append("events", 0, vec![large.clone()])
            .await
            .unwrap();
        engine.append("events", 0, vec![large]).await.unwrap();

        store.segment_gets.store(0, Ordering::SeqCst);
        let request = FetchRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_max_bytes(i32::MAX)
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic(topic_name("events".into()))
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(0)
                            .with_fetch_offset(0)
                            .with_partition_max_bytes(i32::MAX),
                    ]),
            ]);
        let response = fetch_response(request, &engine).await;
        assert_eq!(response.responses[0].partitions[0].error_code, 0);
        let bytes = response.responses[0].partitions[0]
            .records
            .as_ref()
            .unwrap();
        assert!(bytes.len() <= MAX_FETCH_PAYLOAD_BYTES);
        assert_eq!(store.segment_gets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn explicit_metadata_does_not_scan_unrelated_objects() {
        let store = Arc::new(InstrumentedStore::new());
        let engine = LogEngine::new(store.clone(), "walstream/clusters/metadata-lists").unwrap();
        engine.append("unrelated", 0, vec![record()]).await.unwrap();
        let identity = BrokerIdentity {
            host: "localhost".into(),
            port: 9092,
            cluster_id: "metadata-lists".into(),
        };

        store.list_calls.store(0, Ordering::SeqCst);
        let request = MetadataRequest::default()
            .with_topics(Some(vec![
                MetadataRequestTopic::default().with_name(Some(topic_name("requested".into()))),
            ]))
            .with_allow_auto_topic_creation(true);
        metadata_response(request, 4, &engine, &identity)
            .await
            .unwrap();
        assert_eq!(store.list_calls.load(Ordering::SeqCst), 0);

        metadata_response(
            MetadataRequest::default().with_topics(None),
            4,
            &engine,
            &identity,
        )
        .await
        .unwrap();
        assert_eq!(store.list_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn produce_canonicalizes_multiple_input_batches_to_one_segment_batch() {
        let engine = LogEngine::in_memory("walstream/clusters/one-batch").unwrap();
        let first = record();
        let mut second = record();
        second.offset = 1;
        second.sequence = 0;
        second.partition_leader_epoch = 9;
        let encoded = encode_records(&[first, second]).unwrap();
        let mut headers = encoded.clone();
        assert_eq!(
            RecordBatchDecoder::decode_batch_info(&mut headers)
                .unwrap()
                .len(),
            2
        );

        let request = ProduceRequest::default()
            .with_acks(-1)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(topic_name("events".into()))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(0)
                            .with_records(Some(encoded)),
                    ]),
            ]);
        let response = produce_response(request, &engine).await;
        assert_eq!(response.responses[0].partition_responses[0].error_code, 0);

        let fetched = engine
            .fetch_bounded("events", 0, 0, MAX_FETCH_PAYLOAD_BYTES, true)
            .await
            .unwrap();
        let infos = RecordBatchDecoder::decode_batch_info(&mut fetched.records.clone()).unwrap();
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].record_count, 2);
    }

    #[tokio::test]
    async fn produce_rejects_timestamp_ranges_that_cannot_be_canonicalized() {
        let engine = LogEngine::in_memory("walstream/clusters/timestamp-range").unwrap();
        let mut first = record();
        first.timestamp = i64::MIN;
        let mut second = record();
        second.offset = 1;
        second.sequence = 0;
        second.partition_leader_epoch = 9;
        second.timestamp = i64::MAX;
        let encoded = encode_records(&[first, second]).unwrap();
        let request = ProduceRequest::default()
            .with_acks(-1)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(topic_name("events".into()))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(0)
                            .with_records(Some(encoded)),
                    ]),
            ]);

        let response = produce_response(request, &engine).await;
        assert_eq!(
            response.responses[0].partition_responses[0].error_code,
            ResponseError::InvalidTimestamp.code()
        );
        assert!(matches!(
            engine.offsets("events", 0).await,
            Err(LogError::UnknownTopic { .. })
        ));
    }

    #[tokio::test]
    async fn follower_reads_and_missing_or_future_offsets_are_explicit_errors() {
        let engine = LogEngine::in_memory("walstream/clusters/read-protocol-errors").unwrap();
        engine.ensure_topic("events", 0).await.unwrap();

        let follower = FetchRequest::default()
            .with_replica_id(BrokerId(9))
            .with_max_bytes(10_000)
            .with_topics(vec![fetch_topic("events")]);
        assert_eq!(
            fetch_response(follower, &engine).await.responses[0].partitions[0].error_code,
            ResponseError::InvalidRequest.code()
        );

        let list = ListOffsetsRequest::default()
            .with_replica_id(BrokerId(9))
            .with_topics(vec![
                ListOffsetsTopic::default()
                    .with_name(topic_name("events".into()))
                    .with_partitions(vec![
                        ListOffsetsPartition::default()
                            .with_partition_index(0)
                            .with_timestamp(-1),
                    ]),
            ]);
        assert_eq!(
            list_offsets_response(list, &engine).await.topics[0].partitions[0].error_code,
            ResponseError::InvalidRequest.code()
        );

        let missing = FetchRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_max_bytes(10_000)
            .with_topics(vec![fetch_topic("missing")]);
        assert_eq!(
            fetch_response(missing, &engine).await.responses[0].partitions[0].error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );

        let mut future = fetch_topic("events");
        future.partitions[0].fetch_offset = 1;
        let future = FetchRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_max_bytes(10_000)
            .with_topics(vec![future]);
        assert_eq!(
            fetch_response(future, &engine).await.responses[0].partitions[0].error_code,
            ResponseError::OffsetOutOfRange.code()
        );
    }

    #[tokio::test]
    async fn every_advertised_version_round_trips_and_adjacent_versions_fail() {
        let engine = LogEngine::in_memory("walstream/clusters/version-matrix").unwrap();

        for version in 0..=3 {
            let (_, response) = wire_round_trip(
                &engine,
                ApiKey::ApiVersions,
                version,
                RequestKind::ApiVersions(ApiVersionsRequest::default()),
            )
            .await;
            assert!(matches!(response, ResponseKind::ApiVersions(_)));
        }

        let metadata = MetadataRequest::default()
            .with_topics(Some(vec![
                MetadataRequestTopic::default().with_name(Some(topic_name("matrix".into()))),
            ]))
            .with_allow_auto_topic_creation(true);
        let (_, response) = wire_round_trip(
            &engine,
            ApiKey::Metadata,
            4,
            RequestKind::Metadata(metadata),
        )
        .await;
        assert!(matches!(response, ResponseKind::Metadata(_)));

        let (_, response) = wire_round_trip(
            &engine,
            ApiKey::Produce,
            7,
            RequestKind::Produce(produce_request(0)),
        )
        .await;
        assert!(matches!(response, ResponseKind::Produce(_)));

        let fetch = FetchRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_max_bytes(10_000)
            .with_topics(vec![fetch_topic("matrix")]);
        let (response_length, response) =
            wire_round_trip(&engine, ApiKey::Fetch, 4, RequestKind::Fetch(fetch)).await;
        assert!(response_length <= 10_000);
        assert!(matches!(response, ResponseKind::Fetch(_)));

        let list = ListOffsetsRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_topics(vec![
                ListOffsetsTopic::default()
                    .with_name(topic_name("matrix".into()))
                    .with_partitions(vec![
                        ListOffsetsPartition::default()
                            .with_partition_index(0)
                            .with_timestamp(-1),
                    ]),
            ]);
        let (_, response) = wire_round_trip(
            &engine,
            ApiKey::ListOffsets,
            3,
            RequestKind::ListOffsets(list),
        )
        .await;
        assert!(matches!(response, ResponseKind::ListOffsets(_)));

        let identity = BrokerIdentity {
            host: "localhost".into(),
            port: 9092,
            cluster_id: "matrix".into(),
        };
        for (api_key, version) in [
            (ApiKey::Produce, 6),
            (ApiKey::Produce, 8),
            (ApiKey::Fetch, 3),
            (ApiKey::Fetch, 5),
            (ApiKey::ListOffsets, 2),
            (ApiKey::ListOffsets, 4),
            (ApiKey::Metadata, 3),
            (ApiKey::Metadata, 5),
        ] {
            assert!(matches!(
                handle_request(bare_request_bytes(api_key, version, 90), &engine, &identity).await,
                Err(ProtocolError::UnsupportedVersion { .. })
            ));
        }

        let (_, response) = wire_round_trip(
            &engine,
            ApiKey::ApiVersions,
            4,
            RequestKind::ApiVersions(ApiVersionsRequest::default()),
        )
        .await;
        let ResponseKind::ApiVersions(response) = response else {
            panic!("wrong ApiVersions response type");
        };
        assert_eq!(
            response.error_code,
            ResponseError::UnsupportedVersion.code()
        );
    }

    #[tokio::test]
    async fn missing_topics_and_future_offsets_are_wire_errors() {
        let engine = LogEngine::in_memory("walstream/clusters/wire-read-errors").unwrap();
        engine.ensure_topic("events", 0).await.unwrap();

        let fetch = FetchRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_max_bytes(10_000)
            .with_topics(vec![fetch_topic("missing")]);
        let (_, response) =
            wire_round_trip(&engine, ApiKey::Fetch, 4, RequestKind::Fetch(fetch)).await;
        let ResponseKind::Fetch(response) = response else {
            panic!("wrong Fetch response type");
        };
        assert_eq!(
            response.responses[0].partitions[0].error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );

        let mut future = fetch_topic("events");
        future.partitions[0].fetch_offset = 1;
        let fetch = FetchRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_max_bytes(10_000)
            .with_topics(vec![future]);
        let (_, response) =
            wire_round_trip(&engine, ApiKey::Fetch, 4, RequestKind::Fetch(fetch)).await;
        let ResponseKind::Fetch(response) = response else {
            panic!("wrong Fetch response type");
        };
        assert_eq!(
            response.responses[0].partitions[0].error_code,
            ResponseError::OffsetOutOfRange.code()
        );

        let list = ListOffsetsRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_topics(vec![
                ListOffsetsTopic::default()
                    .with_name(topic_name("missing".into()))
                    .with_partitions(vec![
                        ListOffsetsPartition::default()
                            .with_partition_index(0)
                            .with_timestamp(-1),
                    ]),
            ]);
        let (_, response) = wire_round_trip(
            &engine,
            ApiKey::ListOffsets,
            3,
            RequestKind::ListOffsets(list),
        )
        .await;
        let ResponseKind::ListOffsets(response) = response else {
            panic!("wrong ListOffsets response type");
        };
        assert_eq!(
            response.topics[0].partitions[0].error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
    }
}
