//! Shared auto-format fallback dispatch used by the per-signal transform
//! entry points.
//!
//! All `*_auto*` variants follow the same pattern: when the payload looks
//! like JSON we try JSON, then JSONL, then protobuf; otherwise we try
//! protobuf, then JSON. Error wrapping is identical across signals, so we
//! centralize it here. The closures take an extra `&mut O` parameter so the
//! observed variants can thread `&mut Option<&mut dyn TransformObserver>`
//! through without simultaneous mutable borrows.

use crate::{
    decode::{looks_like_json, DecodeError},
    Error, Result,
};

pub(super) fn auto_dispatch<T, O>(
    bytes: &[u8],
    observer: &mut O,
    json: impl FnOnce(&[u8], &mut O) -> Result<T>,
    jsonl: impl FnOnce(&[u8], &mut O) -> Result<T>,
    protobuf: impl FnOnce(&[u8], &mut O) -> Result<T>,
) -> Result<T> {
    if looks_like_json(bytes) {
        match json(bytes, observer) {
            Ok(value) => Ok(value),
            Err(json_err) => match jsonl(bytes, observer) {
                Ok(value) => Ok(value),
                Err(_) => protobuf(bytes, observer).map_err(|proto_err| {
                    Error::Decode(DecodeError::Unsupported(format!(
                        "json decode failed: {json_err}; protobuf fallback failed: {proto_err}"
                    )))
                }),
            },
        }
    } else {
        match protobuf(bytes, observer) {
            Ok(value) => Ok(value),
            Err(proto_err) => json(bytes, observer).map_err(|json_err| {
                Error::Decode(DecodeError::Unsupported(format!(
                    "protobuf decode failed: {proto_err}; json fallback failed: {json_err}"
                )))
            }),
        }
    }
}
