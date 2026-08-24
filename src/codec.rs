//! Allocation-safe validation boundary for Kafka v2 record batches.

use bytes::{Buf, Bytes};
use kafka_protocol::records::{BatchDecodeInfo, Compression, Record, RecordBatchDecoder};
use thiserror::Error;

pub(crate) const MAX_BATCH_RECORDS: usize = 100_000;
pub(crate) const MAX_HEADERS_PER_RECORD: usize = 1_024;
pub(crate) const MAX_HEADERS_PER_REQUEST: usize = 32_768;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BatchInspection {
    pub batch_count: usize,
    pub record_count: usize,
    pub record_header_counts: Vec<usize>,
}

/// Validate headers and every raw record before the upstream decoder allocates.
pub(crate) fn inspect_record_batches(encoded: &Bytes) -> Result<BatchInspection, CodecError> {
    if encoded.is_empty() {
        return Err(CodecError::Malformed);
    }
    let mut headers = encoded.clone();
    let infos =
        RecordBatchDecoder::decode_batch_info(&mut headers).map_err(|_| CodecError::Malformed)?;
    if headers.has_remaining() || infos.is_empty() {
        return Err(CodecError::Malformed);
    }
    if infos
        .iter()
        .any(|info| info.compression != Compression::None)
    {
        return Err(CodecError::UnsupportedCompression);
    }
    let record_count = infos.iter().try_fold(0_usize, |total, info| {
        usize::try_from(info.record_count)
            .ok()
            .and_then(|count| total.checked_add(count))
    });
    let Some(record_count) = record_count else {
        return Err(CodecError::TooManyRecords);
    };
    if record_count > MAX_BATCH_RECORDS {
        return Err(CodecError::TooManyRecords);
    }
    let record_header_counts = validate_raw_records(encoded, &infos)?;
    Ok(BatchInspection {
        batch_count: infos.len(),
        record_count,
        record_header_counts,
    })
}

/// Validate, then decode records through the upstream implementation.
pub(crate) fn decode_record_batches(
    mut encoded: Bytes,
) -> Result<(BatchInspection, Vec<Record>), CodecError> {
    let inspection = inspect_record_batches(&encoded)?;
    let batches =
        RecordBatchDecoder::decode_all(&mut encoded).map_err(|_| CodecError::Malformed)?;
    if encoded.has_remaining() {
        return Err(CodecError::Malformed);
    }
    let records = batches
        .into_iter()
        .flat_map(|batch| batch.records)
        .collect::<Vec<_>>();
    if records.len() != inspection.record_count {
        return Err(CodecError::Malformed);
    }
    if records
        .iter()
        .zip(&inspection.record_header_counts)
        .any(|(record, raw_count)| record.headers.len() != *raw_count)
    {
        return Err(CodecError::DuplicateHeaders);
    }
    Ok((inspection, records))
}

fn validate_raw_records(
    encoded: &[u8],
    infos: &[BatchDecodeInfo],
) -> Result<Vec<usize>, CodecError> {
    let mut batches = encoded;
    let mut aggregate_headers = 0_usize;
    let mut record_header_counts = Vec::new();
    for info in infos {
        let prefix = take_raw(&mut batches, 12)?;
        let batch_length = i32::from_be_bytes(
            prefix[8..12]
                .try_into()
                .map_err(|_| CodecError::Malformed)?,
        );
        let batch_length = usize::try_from(batch_length).map_err(|_| CodecError::Malformed)?;
        let batch = take_raw(&mut batches, batch_length)?;
        let mut records = batch.get(49..).ok_or(CodecError::Malformed)?;

        for _ in 0..info.record_count {
            let record_length = read_var_i32(&mut records)?;
            let record_length =
                usize::try_from(record_length).map_err(|_| CodecError::Malformed)?;
            let mut record = take_raw(&mut records, record_length)?;
            take_raw(&mut record, 1)?; // attributes

            let timestamp_delta = read_var_i64(&mut record)?;
            info.min_timestamp
                .checked_add(timestamp_delta)
                .ok_or(CodecError::ArithmeticOverflow)?;
            let offset_delta = read_var_i32(&mut record)?;
            if offset_delta < 0 {
                return Err(CodecError::Malformed);
            }
            info.min_offset
                .checked_add(i64::from(offset_delta))
                .ok_or(CodecError::ArithmeticOverflow)?;

            take_nullable_raw(&mut record)?; // key
            take_nullable_raw(&mut record)?; // value

            let header_count = read_var_i32(&mut record)?;
            let header_count = usize::try_from(header_count).map_err(|_| CodecError::Malformed)?;
            aggregate_headers = aggregate_headers
                .checked_add(header_count)
                .ok_or(CodecError::TooManyHeaders)?;
            if header_count > MAX_HEADERS_PER_RECORD
                || aggregate_headers > MAX_HEADERS_PER_REQUEST
                || header_count
                    .checked_mul(2)
                    .is_none_or(|minimum| minimum > record.len())
            {
                return Err(CodecError::TooManyHeaders);
            }
            record_header_counts.push(header_count);
            for _ in 0..header_count {
                let key_length = read_var_i32(&mut record)?;
                let key_length = usize::try_from(key_length).map_err(|_| CodecError::Malformed)?;
                take_raw(&mut record, key_length)?;
                take_nullable_raw(&mut record)?;
            }
            if !record.is_empty() {
                return Err(CodecError::Malformed);
            }
        }
        if !records.is_empty() {
            return Err(CodecError::Malformed);
        }
    }
    if batches.is_empty() {
        Ok(record_header_counts)
    } else {
        Err(CodecError::Malformed)
    }
}

fn take_nullable_raw(input: &mut &[u8]) -> Result<(), CodecError> {
    match read_var_i32(input)? {
        -1 => Ok(()),
        length if length >= 0 => {
            take_raw(
                input,
                usize::try_from(length).map_err(|_| CodecError::Malformed)?,
            )?;
            Ok(())
        }
        _ => Err(CodecError::Malformed),
    }
}

fn take_raw<'a>(input: &mut &'a [u8], length: usize) -> Result<&'a [u8], CodecError> {
    if input.len() < length {
        return Err(CodecError::Malformed);
    }
    let (taken, remaining) = input.split_at(length);
    *input = remaining;
    Ok(taken)
}

fn read_var_i32(input: &mut &[u8]) -> Result<i32, CodecError> {
    let value = read_unsigned_var(input, 5)?;
    let value = u32::try_from(value).map_err(|_| CodecError::Malformed)?;
    Ok(((value >> 1) as i32) ^ -((value & 1) as i32))
}

fn read_var_i64(input: &mut &[u8]) -> Result<i64, CodecError> {
    let value = read_unsigned_var(input, 10)?;
    Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
}

fn read_unsigned_var(input: &mut &[u8], maximum_bytes: usize) -> Result<u64, CodecError> {
    let mut value = 0_u64;
    for index in 0..maximum_bytes {
        let byte = *take_raw(input, 1)?.first().ok_or(CodecError::Malformed)?;
        let payload = u64::from(byte & 0x7f);
        let shift = index * 7;
        if (shift == 28 && maximum_bytes == 5 && payload > 0x0f)
            || (shift == 63 && maximum_bytes == 10 && payload > 0x01)
        {
            return Err(CodecError::Malformed);
        }
        value |= payload << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(CodecError::Malformed)
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum CodecError {
    #[error("malformed Kafka v2 record data")]
    Malformed,
    #[error("compressed Kafka record batches are unsupported")]
    UnsupportedCompression,
    #[error("Kafka record count exceeds the broker limit")]
    TooManyRecords,
    #[error("Kafka header count exceeds the broker limit")]
    TooManyHeaders,
    #[error("duplicate Kafka record header keys cannot be preserved")]
    DuplicateHeaders,
    #[error("Kafka record delta arithmetic overflow")]
    ArithmeticOverflow,
}
