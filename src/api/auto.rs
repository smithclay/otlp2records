//! Shared dispatch helpers used by the per-signal transform entry points.
//!
//! Two patterns repeat identically across logs, traces, and metrics and are
//! centralized here:
//!
//! - [`auto_dispatch`]: all `*_auto*` variants try JSON, then JSONL, then
//!   protobuf when the payload looks like JSON, otherwise protobuf then JSON.
//!   The closures take the `&mut Observation` as a parameter rather than
//!   capturing it, so each attempt can borrow it in turn.
//! - [`decode_json_observed`]: decode a JSON or JSONL request and report the
//!   decode phase to the observation. Both the normalized and OTAP star JSON paths
//!   of every signal share this exact block.

use std::time::Instant;

use crate::{
    batch::{Observation, TransformPhase},
    decode::{looks_like_json, DecodeError, InputFormat},
    Error, Result,
};

/// Decode a JSON (`InputFormat::Json`) or JSONL (`InputFormat::Jsonl`) request
/// and report the corresponding decode phase to `obs`. Returns the decoded
/// request; the caller chooses the normalized or OTAP star transform to apply.
/// Any other `format` is a caller bug and yields an `Unsupported` error tagged
/// with `signal_label` (e.g. `"logs"`).
pub(super) fn decode_json_observed<R>(
    bytes: &[u8],
    format: InputFormat,
    obs: &mut Observation<'_>,
    decode_json: impl FnOnce(&[u8]) -> std::result::Result<R, DecodeError>,
    decode_jsonl: impl FnOnce(&[u8]) -> std::result::Result<R, DecodeError>,
    signal_label: &str,
) -> Result<R> {
    let start = Instant::now();
    let (request, phase) = match format {
        InputFormat::Json => (decode_json(bytes)?, TransformPhase::JsonDecode),
        InputFormat::Jsonl => (decode_jsonl(bytes)?, TransformPhase::JsonlDecode),
        _ => {
            return Err(Error::Decode(DecodeError::Unsupported(format!(
                "expected JSON or JSONL {signal_label} input"
            ))));
        }
    };
    obs.record(phase, start.elapsed());
    Ok(request)
}

pub(super) fn auto_dispatch<T, O>(
    bytes: &[u8],
    obs: &mut O,
    json: impl FnOnce(&[u8], &mut O) -> Result<T>,
    jsonl: impl FnOnce(&[u8], &mut O) -> Result<T>,
    protobuf: impl FnOnce(&[u8], &mut O) -> Result<T>,
) -> Result<T> {
    if looks_like_json(bytes) {
        match json(bytes, obs) {
            Ok(value) => Ok(value),
            Err(json_err) => match jsonl(bytes, obs) {
                Ok(value) => Ok(value),
                Err(_) => protobuf(bytes, obs).map_err(|proto_err| {
                    Error::Decode(DecodeError::Unsupported(format!(
                        "json decode failed: {json_err}; protobuf fallback failed: {proto_err}"
                    )))
                }),
            },
        }
    } else {
        match protobuf(bytes, obs) {
            Ok(value) => Ok(value),
            Err(proto_err) => json(bytes, obs).map_err(|json_err| {
                Error::Decode(DecodeError::Unsupported(format!(
                    "protobuf decode failed: {proto_err}; json fallback failed: {json_err}"
                )))
            }),
        }
    }
}
