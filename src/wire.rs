//! Allocation-safe structural validation for the advertised Kafka request subset.
//!
//! Generated protocol decoders allocate collections directly from wire counts.
//! This bounded, allocation-free pass proves those counts and lengths fit the
//! actual frame before generated decoding is allowed to run.

use bytes::Bytes;
use kafka_protocol::messages::ApiKey;
use thiserror::Error;

const MAX_REQUEST_COLLECTION_ITEMS: usize = 10_000;

pub(crate) fn validate_request_frame(
    frame: &Bytes,
    api_key: ApiKey,
    version: i16,
) -> Result<(), WireError> {
    let mut cursor = Cursor::new(frame);
    if cursor.i16()? != api_key as i16 || cursor.i16()? != version {
        return Err(WireError::Malformed("request header key/version mismatch"));
    }
    cursor.take(4)?; // correlation id
    cursor.classic_string(true)?; // nullable client id
    match api_key.request_header_version(version) {
        1 => {}
        2 => cursor.tagged_fields()?,
        _ => return Err(WireError::Malformed("unsupported request header version")),
    }

    match api_key {
        ApiKey::ApiVersions => validate_api_versions(&mut cursor, version)?,
        ApiKey::Metadata => validate_metadata(&mut cursor)?,
        ApiKey::Produce => validate_produce(&mut cursor)?,
        ApiKey::Fetch => validate_fetch(&mut cursor)?,
        ApiKey::ListOffsets => validate_list_offsets(&mut cursor)?,
        ApiKey::FindCoordinator => validate_find_coordinator(&mut cursor)?,
        ApiKey::JoinGroup => validate_join_group(&mut cursor)?,
        ApiKey::SyncGroup => validate_sync_group(&mut cursor)?,
        ApiKey::Heartbeat => validate_heartbeat(&mut cursor)?,
        ApiKey::LeaveGroup => validate_leave_group(&mut cursor)?,
        ApiKey::OffsetCommit => validate_offset_commit(&mut cursor)?,
        ApiKey::OffsetFetch => validate_offset_fetch(&mut cursor)?,
        _ => return Err(WireError::Malformed("unsupported request API")),
    }
    if !cursor.input.is_empty() {
        return Err(WireError::Malformed("trailing request bytes"));
    }
    Ok(())
}

fn validate_api_versions(cursor: &mut Cursor<'_>, version: i16) -> Result<(), WireError> {
    if version >= 3 {
        cursor.compact_string()?;
        cursor.compact_string()?;
        cursor.tagged_fields()?;
    }
    Ok(())
}

fn validate_metadata(cursor: &mut Cursor<'_>) -> Result<(), WireError> {
    if let Some(topics) = cursor.classic_array(true, 2)? {
        for _ in 0..topics {
            cursor.classic_string(false)?;
        }
    }
    cursor.take(1)?; // allow_auto_topic_creation
    Ok(())
}

fn validate_produce(cursor: &mut Cursor<'_>) -> Result<(), WireError> {
    cursor.classic_string(true)?; // transactional id
    cursor.take(6)?; // acks and timeout
    let topics = cursor
        .classic_array(false, 6)?
        .ok_or(WireError::Malformed("null topic array"))?;
    for _ in 0..topics {
        cursor.classic_string(false)?;
        let partitions = cursor
            .classic_array(false, 8)?
            .ok_or(WireError::Malformed("null partition array"))?;
        for _ in 0..partitions {
            cursor.take(4)?; // partition index
            cursor.classic_bytes(true)?;
        }
    }
    Ok(())
}

fn validate_fetch(cursor: &mut Cursor<'_>) -> Result<(), WireError> {
    cursor.take(17)?; // replica id, wait/min/max bytes, isolation level
    let topics = cursor
        .classic_array(false, 6)?
        .ok_or(WireError::Malformed("null topic array"))?;
    for _ in 0..topics {
        cursor.classic_string(false)?;
        let partitions = cursor
            .classic_array(false, 16)?
            .ok_or(WireError::Malformed("null partition array"))?;
        for _ in 0..partitions {
            cursor.take(16)?; // partition, fetch offset, partition max bytes
        }
    }
    Ok(())
}

fn validate_list_offsets(cursor: &mut Cursor<'_>) -> Result<(), WireError> {
    cursor.take(5)?; // replica id and isolation level
    let topics = cursor
        .classic_array(false, 6)?
        .ok_or(WireError::Malformed("null topic array"))?;
    for _ in 0..topics {
        cursor.classic_string(false)?;
        let partitions = cursor
            .classic_array(false, 12)?
            .ok_or(WireError::Malformed("null partition array"))?;
        for _ in 0..partitions {
            cursor.take(12)?; // partition index and timestamp
        }
    }
    Ok(())
}

fn validate_find_coordinator(cursor: &mut Cursor<'_>) -> Result<(), WireError> {
    cursor.classic_string(false)?; // coordinator key
    cursor.take(1)?; // key type
    Ok(())
}

fn validate_join_group(cursor: &mut Cursor<'_>) -> Result<(), WireError> {
    cursor.classic_string(false)?; // group id
    cursor.take(8)?; // session and rebalance timeouts
    cursor.classic_string(false)?; // member id
    cursor.classic_string(false)?; // protocol type
    let protocols = cursor
        .classic_array(false, 6)?
        .ok_or(WireError::Malformed("null protocol array"))?;
    for _ in 0..protocols {
        cursor.classic_string(false)?;
        cursor.classic_bytes(false)?;
    }
    Ok(())
}

fn validate_sync_group(cursor: &mut Cursor<'_>) -> Result<(), WireError> {
    cursor.classic_string(false)?; // group id
    cursor.take(4)?; // generation id
    cursor.classic_string(false)?; // member id
    let assignments = cursor
        .classic_array(false, 6)?
        .ok_or(WireError::Malformed("null assignment array"))?;
    for _ in 0..assignments {
        cursor.classic_string(false)?;
        cursor.classic_bytes(false)?;
    }
    Ok(())
}

fn validate_heartbeat(cursor: &mut Cursor<'_>) -> Result<(), WireError> {
    cursor.classic_string(false)?; // group id
    cursor.take(4)?; // generation id
    cursor.classic_string(false)?; // member id
    Ok(())
}

fn validate_leave_group(cursor: &mut Cursor<'_>) -> Result<(), WireError> {
    cursor.classic_string(false)?; // group id
    cursor.classic_string(false)?; // member id
    Ok(())
}

fn validate_offset_commit(cursor: &mut Cursor<'_>) -> Result<(), WireError> {
    cursor.classic_string(false)?; // group id
    cursor.take(4)?; // generation id
    cursor.classic_string(false)?; // member id
    cursor.take(8)?; // retention time
    let topics = cursor
        .classic_array(false, 6)?
        .ok_or(WireError::Malformed("null offset-commit topic array"))?;
    for _ in 0..topics {
        cursor.classic_string(false)?;
        let partitions = cursor
            .classic_array(false, 14)?
            .ok_or(WireError::Malformed("null offset-commit partition array"))?;
        for _ in 0..partitions {
            cursor.take(12)?; // partition index and committed offset
            cursor.classic_string(true)?; // metadata
        }
    }
    Ok(())
}

fn validate_offset_fetch(cursor: &mut Cursor<'_>) -> Result<(), WireError> {
    cursor.classic_string(false)?; // group id
    if let Some(topics) = cursor.classic_array(true, 6)? {
        for _ in 0..topics {
            cursor.classic_string(false)?;
            let partitions = cursor
                .classic_array(false, 4)?
                .ok_or(WireError::Malformed("null offset-fetch partition array"))?;
            cursor.take(partitions * 4)?;
        }
    }
    Ok(())
}

struct Cursor<'a> {
    input: &'a [u8],
    collection_items_left: usize,
}

impl<'a> Cursor<'a> {
    fn new(input: &'a Bytes) -> Self {
        Self {
            input,
            collection_items_left: MAX_REQUEST_COLLECTION_ITEMS,
        }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], WireError> {
        if self.input.len() < length {
            return Err(WireError::Malformed("request field exceeds frame"));
        }
        let (value, remaining) = self.input.split_at(length);
        self.input = remaining;
        Ok(value)
    }

    fn i16(&mut self) -> Result<i16, WireError> {
        Ok(i16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| WireError::Malformed("invalid int16"))?,
        ))
    }

    fn i32(&mut self) -> Result<i32, WireError> {
        Ok(i32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| WireError::Malformed("invalid int32"))?,
        ))
    }

    fn classic_string(&mut self, nullable: bool) -> Result<(), WireError> {
        match self.i16()? {
            -1 if nullable => Ok(()),
            length if length >= 0 => {
                self.take(length as usize)?;
                Ok(())
            }
            _ => Err(WireError::Malformed("invalid string length")),
        }
    }

    fn compact_string(&mut self) -> Result<(), WireError> {
        let length = self.unsigned_varint()?;
        if length == 0 {
            return Err(WireError::Malformed("null compact string"));
        }
        self.take((length - 1) as usize)?;
        Ok(())
    }

    fn classic_bytes(&mut self, nullable: bool) -> Result<(), WireError> {
        match self.i32()? {
            -1 if nullable => Ok(()),
            length if length >= 0 => {
                self.take(length as usize)?;
                Ok(())
            }
            _ => Err(WireError::Malformed("invalid bytes length")),
        }
    }

    fn classic_array(
        &mut self,
        nullable: bool,
        minimum_item_bytes: usize,
    ) -> Result<Option<usize>, WireError> {
        match self.i32()? {
            -1 if nullable => Ok(None),
            count if count >= 0 => {
                let count = count as usize;
                self.reserve_collection(count, minimum_item_bytes)?;
                Ok(Some(count))
            }
            _ => Err(WireError::Malformed("invalid array count")),
        }
    }

    fn tagged_fields(&mut self) -> Result<(), WireError> {
        let count =
            usize::try_from(self.unsigned_varint()?).map_err(|_| WireError::CollectionLimit)?;
        self.reserve_collection(count, 2)?;
        let mut previous = None;
        for _ in 0..count {
            let tag = self.unsigned_varint()?;
            if previous.is_some_and(|previous| tag <= previous) {
                return Err(WireError::Malformed(
                    "tagged fields are duplicated or unordered",
                ));
            }
            previous = Some(tag);
            let size = usize::try_from(self.unsigned_varint()?)
                .map_err(|_| WireError::Malformed("tagged field is too large"))?;
            self.take(size)?;
        }
        Ok(())
    }

    fn reserve_collection(
        &mut self,
        count: usize,
        minimum_item_bytes: usize,
    ) -> Result<(), WireError> {
        if count > self.collection_items_left
            || count
                .checked_mul(minimum_item_bytes)
                .is_none_or(|minimum| minimum > self.input.len())
        {
            return Err(WireError::CollectionLimit);
        }
        self.collection_items_left -= count;
        Ok(())
    }

    fn unsigned_varint(&mut self) -> Result<u32, WireError> {
        let mut value = 0_u32;
        for index in 0..5 {
            let byte = self.take(1)?[0];
            let payload = u32::from(byte & 0x7f);
            if index == 4 && payload > 0x0f {
                return Err(WireError::Malformed("varint overflow"));
            }
            value |= payload << (index * 7);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(WireError::Malformed("unterminated varint"))
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum WireError {
    #[error("malformed Kafka request layout: {0}")]
    Malformed(&'static str),
    #[error("Kafka request collection count exceeds the bounded frame layout")]
    CollectionLimit,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classic_header(api_key: ApiKey, version: i16) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&(api_key as i16).to_be_bytes());
        frame.extend_from_slice(&version.to_be_bytes());
        frame.extend_from_slice(&1_i32.to_be_bytes());
        frame.extend_from_slice(&(-1_i16).to_be_bytes());
        frame
    }

    fn string(frame: &mut Vec<u8>, value: &str) {
        frame.extend_from_slice(&(value.len() as i16).to_be_bytes());
        frame.extend_from_slice(value.as_bytes());
    }

    fn assert_collection_limit(frame: Vec<u8>, api_key: ApiKey, version: i16) {
        assert_eq!(
            validate_request_frame(&Bytes::from(frame), api_key, version),
            Err(WireError::CollectionLimit)
        );
    }

    #[test]
    fn rejects_tiny_frames_with_huge_top_level_and_nested_counts() {
        let mut metadata = Vec::new();
        metadata.extend_from_slice(&(ApiKey::Metadata as i16).to_be_bytes());
        metadata.extend_from_slice(&4_i16.to_be_bytes());
        metadata.extend_from_slice(&1_i32.to_be_bytes());
        metadata.extend_from_slice(&(-1_i16).to_be_bytes());
        metadata.extend_from_slice(&i32::MAX.to_be_bytes());
        metadata.push(1);
        assert_eq!(
            validate_request_frame(&Bytes::from(metadata), ApiKey::Metadata, 4),
            Err(WireError::CollectionLimit)
        );

        let mut produce = Vec::new();
        produce.extend_from_slice(&(ApiKey::Produce as i16).to_be_bytes());
        produce.extend_from_slice(&7_i16.to_be_bytes());
        produce.extend_from_slice(&1_i32.to_be_bytes());
        produce.extend_from_slice(&(-1_i16).to_be_bytes());
        produce.extend_from_slice(&(-1_i16).to_be_bytes());
        produce.extend_from_slice(&(-1_i16).to_be_bytes());
        produce.extend_from_slice(&0_i32.to_be_bytes());
        produce.extend_from_slice(&1_i32.to_be_bytes());
        produce.extend_from_slice(&0_i16.to_be_bytes());
        produce.extend_from_slice(&i32::MAX.to_be_bytes());
        assert_eq!(
            validate_request_frame(&Bytes::from(produce), ApiKey::Produce, 7),
            Err(WireError::CollectionLimit)
        );
    }

    #[test]
    fn rejects_group_api_top_level_count_bombs() {
        let mut join = classic_header(ApiKey::JoinGroup, 2);
        string(&mut join, "workers");
        join.extend_from_slice(&30_000_i32.to_be_bytes());
        join.extend_from_slice(&30_000_i32.to_be_bytes());
        string(&mut join, "");
        string(&mut join, "consumer");
        join.extend_from_slice(&i32::MAX.to_be_bytes());
        assert_collection_limit(join, ApiKey::JoinGroup, 2);

        let mut sync = classic_header(ApiKey::SyncGroup, 1);
        string(&mut sync, "workers");
        sync.extend_from_slice(&1_i32.to_be_bytes());
        string(&mut sync, "member");
        sync.extend_from_slice(&i32::MAX.to_be_bytes());
        assert_collection_limit(sync, ApiKey::SyncGroup, 1);

        let mut commit = classic_header(ApiKey::OffsetCommit, 2);
        string(&mut commit, "workers");
        commit.extend_from_slice(&1_i32.to_be_bytes());
        string(&mut commit, "member");
        commit.extend_from_slice(&(-1_i64).to_be_bytes());
        commit.extend_from_slice(&i32::MAX.to_be_bytes());
        assert_collection_limit(commit, ApiKey::OffsetCommit, 2);

        let mut fetch = classic_header(ApiKey::OffsetFetch, 3);
        string(&mut fetch, "workers");
        fetch.extend_from_slice(&i32::MAX.to_be_bytes());
        assert_collection_limit(fetch, ApiKey::OffsetFetch, 3);
    }

    #[test]
    fn rejects_group_api_nested_count_bombs() {
        let mut commit = classic_header(ApiKey::OffsetCommit, 2);
        string(&mut commit, "workers");
        commit.extend_from_slice(&1_i32.to_be_bytes());
        string(&mut commit, "member");
        commit.extend_from_slice(&(-1_i64).to_be_bytes());
        commit.extend_from_slice(&1_i32.to_be_bytes());
        string(&mut commit, "events");
        commit.extend_from_slice(&i32::MAX.to_be_bytes());
        assert_collection_limit(commit, ApiKey::OffsetCommit, 2);

        let mut fetch = classic_header(ApiKey::OffsetFetch, 3);
        string(&mut fetch, "workers");
        fetch.extend_from_slice(&1_i32.to_be_bytes());
        string(&mut fetch, "events");
        fetch.extend_from_slice(&i32::MAX.to_be_bytes());
        assert_collection_limit(fetch, ApiKey::OffsetFetch, 3);
    }
}
