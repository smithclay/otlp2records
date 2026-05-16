//! OTLP decode support for public transforms.

mod common;
mod logs;
mod metrics;
mod normalize;
mod traces;

pub use common::{looks_like_json, DecodeError};
pub use metrics::SkippedMetrics;
use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
    trace::v1::ExportTraceServiceRequest,
};

/// Input format for OTLP decoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputFormat {
    /// Protocol Buffers binary format.
    Protobuf,
    /// JSON format.
    Json,
    /// Newline-delimited JSON (JSONL/NDJSON) format.
    Jsonl,
    /// Auto-detect JSON vs protobuf, with fallback decoding.
    Auto,
}

impl InputFormat {
    /// Infer input format from Content-Type header.
    pub fn from_content_type(content_type: Option<&str>) -> Self {
        let content_type = content_type.map(|v| v.trim().to_ascii_lowercase());

        match content_type.as_deref() {
            Some("application/x-ndjson") | Some("application/jsonl") => InputFormat::Jsonl,
            Some("application/json") | Some("application/otlp+json") => InputFormat::Json,
            Some("application/x-protobuf")
            | Some("application/protobuf")
            | Some("application/otlp") => InputFormat::Protobuf,
            _ => InputFormat::Auto,
        }
    }

    /// Returns the canonical Content-Type string for this format.
    pub fn content_type(&self) -> &'static str {
        match self {
            InputFormat::Protobuf => "application/x-protobuf",
            InputFormat::Json => "application/json",
            InputFormat::Jsonl => "application/x-ndjson",
            InputFormat::Auto => "application/x-protobuf",
        }
    }
}

pub(crate) fn decode_logs_json_request(
    bytes: &[u8],
) -> Result<ExportLogsServiceRequest, DecodeError> {
    logs::decode_json_request(bytes)
}

pub(crate) fn decode_logs_jsonl_request(
    bytes: &[u8],
) -> Result<ExportLogsServiceRequest, DecodeError> {
    let mut request = ExportLogsServiceRequest::default();
    for_jsonl_requests(bytes, |line| {
        let mut next = logs::decode_json_request(line)?;
        request.resource_logs.append(&mut next.resource_logs);
        Ok(())
    })?;
    Ok(request)
}

pub(crate) fn decode_traces_json_request(
    bytes: &[u8],
) -> Result<ExportTraceServiceRequest, DecodeError> {
    traces::decode_json_request(bytes)
}

pub(crate) fn decode_traces_jsonl_request(
    bytes: &[u8],
) -> Result<ExportTraceServiceRequest, DecodeError> {
    let mut request = ExportTraceServiceRequest::default();
    for_jsonl_requests(bytes, |line| {
        let mut next = traces::decode_json_request(line)?;
        request.resource_spans.append(&mut next.resource_spans);
        Ok(())
    })?;
    Ok(request)
}

pub(crate) fn decode_metrics_json_request(
    bytes: &[u8],
) -> Result<ExportMetricsServiceRequest, DecodeError> {
    metrics::decode_json_request(bytes)
}

pub(crate) fn decode_metrics_jsonl_request(
    bytes: &[u8],
) -> Result<ExportMetricsServiceRequest, DecodeError> {
    let mut request = ExportMetricsServiceRequest::default();
    for_jsonl_requests(bytes, |line| {
        let mut next = metrics::decode_json_request(line)?;
        request.resource_metrics.append(&mut next.resource_metrics);
        Ok(())
    })?;
    Ok(request)
}

fn for_jsonl_requests<F>(bytes: &[u8], mut decode_line: F) -> Result<(), DecodeError>
where
    F: FnMut(&[u8]) -> Result<(), DecodeError>,
{
    let text = std::str::from_utf8(bytes).map_err(|e| DecodeError::Parse(e.to_string()))?;
    let mut saw_line = false;

    for (line_num, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        saw_line = true;
        decode_line(trimmed.as_bytes())
            .map_err(|e| DecodeError::Parse(format!("line {}: {}", line_num + 1, e)))?;
    }

    if !saw_line {
        return Err(DecodeError::Parse(
            "jsonl payload contained no records".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_format_from_content_type_jsonl() {
        assert_eq!(
            InputFormat::from_content_type(Some("application/x-ndjson")),
            InputFormat::Jsonl
        );
        assert_eq!(
            InputFormat::from_content_type(Some("application/jsonl")),
            InputFormat::Jsonl
        );
    }

    #[test]
    fn input_format_content_type() {
        assert_eq!(
            InputFormat::Protobuf.content_type(),
            "application/x-protobuf"
        );
        assert_eq!(InputFormat::Json.content_type(), "application/json");
        assert_eq!(InputFormat::Jsonl.content_type(), "application/x-ndjson");
    }

    #[test]
    fn logs_jsonl_combines_requests() {
        let jsonl = r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"a"}}]}]}]}
{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"b"}}]}]}]}"#;
        let request = decode_logs_jsonl_request(jsonl.as_bytes()).unwrap();
        assert_eq!(request.resource_logs.len(), 2);
    }
}
