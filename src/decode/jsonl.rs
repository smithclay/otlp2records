//! JSONL request aggregation for OTLP JSON request structs.

use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
    trace::v1::ExportTraceServiceRequest,
};

use super::json::{
    decode_logs_json_request, decode_metrics_json_request, decode_traces_json_request, DecodeError,
};

pub(crate) fn decode_logs_jsonl_request(
    bytes: &[u8],
) -> Result<ExportLogsServiceRequest, DecodeError> {
    let mut request = ExportLogsServiceRequest::default();
    for_jsonl_requests(bytes, |line| {
        let mut next = decode_logs_json_request(line)?;
        request.resource_logs.append(&mut next.resource_logs);
        Ok(())
    })?;
    Ok(request)
}

pub(crate) fn decode_traces_jsonl_request(
    bytes: &[u8],
) -> Result<ExportTraceServiceRequest, DecodeError> {
    let mut request = ExportTraceServiceRequest::default();
    for_jsonl_requests(bytes, |line| {
        let mut next = decode_traces_json_request(line)?;
        request.resource_spans.append(&mut next.resource_spans);
        Ok(())
    })?;
    Ok(request)
}

pub(crate) fn decode_metrics_jsonl_request(
    bytes: &[u8],
) -> Result<ExportMetricsServiceRequest, DecodeError> {
    let mut request = ExportMetricsServiceRequest::default();
    for_jsonl_requests(bytes, |line| {
        let mut next = decode_metrics_json_request(line)?;
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
    fn logs_jsonl_combines_requests() {
        let jsonl = r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"a"}}]}]}]}
{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"b"}}]}]}]}"#;
        let request = decode_logs_jsonl_request(jsonl.as_bytes()).unwrap();
        assert_eq!(request.resource_logs.len(), 2);
    }
}
