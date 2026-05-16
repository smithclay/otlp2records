//! Input format classification for OTLP decoding.

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
}
