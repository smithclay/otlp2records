//! JSON convenience helpers for public API transforms.

use arrow_array::RecordBatch;

use crate::{Error, Result};

pub(crate) fn optional_batch_to_json_values(
    batch: Option<&RecordBatch>,
) -> Result<Vec<serde_json::Value>> {
    match batch {
        Some(batch) => batch_to_json_values(batch),
        None => Ok(Vec::new()),
    }
}

pub(crate) fn batch_to_json_values(batch: &RecordBatch) -> Result<Vec<serde_json::Value>> {
    let bytes = crate::output::to_json(batch)?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|err| Error::InvalidInput(format!("JSON output was not UTF-8: {err}")))?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line)
                .map_err(|err| Error::InvalidInput(format!("invalid JSON output row: {err}")))
        })
        .collect()
}
