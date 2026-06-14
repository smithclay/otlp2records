//! JSON serialization helpers for semantic telemetry views.

use std::{borrow::Cow, fmt, io};

use arrow_array::builder::StringBuilder;
use const_hex::encode_to_str;

use crate::{
    views::pdata::{
        AnyValueView, AttributeView, EventView, ExemplarView, LinkView, Value, ValueType,
    },
    Error, Result,
};

pub(super) fn append_body<'a, V: AnyValueView<'a>>(
    builder: &mut StringBuilder,
    value: Option<V>,
) -> Result<()> {
    let Some(value) = value else {
        builder.append_null();
        return Ok(());
    };

    match value.value_type() {
        ValueType::Empty => builder.append_null(),
        ValueType::String => {
            let value = view_text(value.as_string().unwrap_or_default());
            if value.is_empty() {
                builder.append_null();
            } else {
                builder.append_value(value.as_ref());
            }
        }
        ValueType::Bool => {
            write_str(
                builder,
                if value.as_bool().unwrap_or(false) {
                    "true"
                } else {
                    "false"
                },
            )?;
            builder.append_value("");
        }
        ValueType::Int64 => {
            write_str(builder, &value.as_int64().unwrap_or_default().to_string())?;
            builder.append_value("");
        }
        ValueType::Double => match value.as_double() {
            Some(value) if value.is_finite() => {
                serde_json::to_writer(FmtWriter(builder), &value)?;
                builder.append_value("");
            }
            _ => builder.append_null(),
        },
        ValueType::Bytes => {
            let value = String::from_utf8_lossy(value.as_bytes().unwrap_or_default());
            if value.is_empty() {
                builder.append_null();
            } else {
                builder.append_value(value.as_ref());
            }
        }
        ValueType::Array | ValueType::KeyValueList => {
            write_any(builder, &value)?;
            builder.append_value("");
        }
    }
    Ok(())
}

pub(super) fn append_attributes<A: AttributeView>(
    builder: &mut StringBuilder,
    attributes: impl Iterator<Item = A>,
    scratch: &mut String,
) -> Result<()> {
    let mut attributes: Vec<_> = attributes.filter(|attr| attr.value().is_some()).collect();
    if attributes.is_empty() {
        builder.append_null();
        return Ok(());
    }

    scratch.clear();
    write_attributes(scratch, &mut attributes)?;
    builder.append_value(scratch.as_str());
    Ok(())
}

pub(super) fn append_events<E: EventView>(
    builder: &mut StringBuilder,
    events: impl Iterator<Item = E>,
    scratch: &mut String,
) -> Result<()> {
    let events: Vec<_> = events.collect();
    if events.is_empty() {
        builder.append_null();
        return Ok(());
    }

    scratch.clear();
    write_str(scratch, "[")?;
    for (index, event) in events.iter().enumerate() {
        if index > 0 {
            write_str(scratch, ",")?;
        }
        write_str(scratch, "{\"attributes\":")?;
        write_attribute_iter(scratch, event.attributes())?;
        write_str(scratch, ",\"name\":")?;
        write_json_string(scratch, &view_text(event.name().unwrap_or_default()))?;
        write_str(scratch, ",\"time_unix_nano\":")?;
        write_u64_as_i64(scratch, event.time_unix_nano().unwrap_or_default(), "event")?;
        write_str(scratch, "}")?;
    }
    write_str(scratch, "]")?;
    builder.append_value(scratch.as_str());
    Ok(())
}

pub(super) fn append_links<L: LinkView>(
    builder: &mut StringBuilder,
    links: impl Iterator<Item = L>,
    scratch: &mut String,
) -> Result<()> {
    let links: Vec<_> = links.collect();
    if links.is_empty() {
        builder.append_null();
        return Ok(());
    }

    scratch.clear();
    write_str(scratch, "[")?;
    for (index, link) in links.iter().enumerate() {
        if index > 0 {
            write_str(scratch, ",")?;
        }
        write_str(scratch, "{\"attributes\":")?;
        write_attribute_iter(scratch, link.attributes())?;
        write_str(scratch, ",\"span_id\":")?;
        write_id(scratch, link.span_id().map(|value| value.as_slice()))?;
        write_str(scratch, ",\"trace_id\":")?;
        write_id(scratch, link.trace_id().map(|value| value.as_slice()))?;
        write_str(scratch, ",\"trace_state\":")?;
        write_json_string(scratch, &view_text(link.trace_state().unwrap_or_default()))?;
        write_str(scratch, "}")?;
    }
    write_str(scratch, "]")?;
    builder.append_value(scratch.as_str());
    Ok(())
}

pub(super) fn append_exemplars<E: ExemplarView>(
    builder: &mut StringBuilder,
    exemplars: impl Iterator<Item = E>,
    scratch: &mut String,
) -> Result<()> {
    let exemplars: Vec<_> = exemplars.collect();
    if exemplars.is_empty() {
        builder.append_null();
        return Ok(());
    }

    scratch.clear();
    write_str(scratch, "[")?;
    for (index, exemplar) in exemplars.iter().enumerate() {
        if index > 0 {
            write_str(scratch, ",")?;
        }
        write_str(scratch, "{\"filtered_attributes\":")?;
        write_attribute_iter(scratch, exemplar.filtered_attributes())?;
        write_str(scratch, ",\"span_id\":")?;
        write_id(scratch, exemplar.span_id().map(|value| value.as_slice()))?;
        write_str(scratch, ",\"time_unix_nano\":")?;
        write_u64_as_i64(scratch, exemplar.time_unix_nano(), "exemplar")?;
        write_str(scratch, ",\"trace_id\":")?;
        write_id(scratch, exemplar.trace_id().map(|value| value.as_slice()))?;
        if let Some(value) = exemplar.value().filter(value_is_finite) {
            write_str(scratch, ",\"value\":")?;
            match value {
                Value::Integer(value) => {
                    serde_json::to_writer(FmtWriter(scratch), &(value as f64))?;
                }
                Value::Double(value) => {
                    serde_json::to_writer(FmtWriter(scratch), &value)?;
                }
            }
        }
        write_str(scratch, "}")?;
    }
    write_str(scratch, "]")?;
    builder.append_value(scratch.as_str());
    Ok(())
}

pub(super) fn attributes_json<A: AttributeView>(
    attributes: impl Iterator<Item = A>,
) -> Option<String> {
    let mut attributes: Vec<_> = attributes.filter(|attr| attr.value().is_some()).collect();
    if attributes.is_empty() {
        return None;
    }

    let mut output = String::new();
    write_attributes(&mut output, &mut attributes).expect("writing JSON to String cannot fail");
    Some(output)
}

pub(super) fn attribute_field<A: AttributeView>(
    attributes: impl Iterator<Item = A>,
    key: &[u8],
) -> Option<String> {
    for attribute in attributes {
        if attribute.key() == key {
            if let Some(value) = attribute.value() {
                return any_to_string(&value).filter(|value| !value.is_empty());
            }
        }
    }
    None
}

fn write_attributes<W: fmt::Write, A: AttributeView>(
    output: &mut W,
    attributes: &mut [A],
) -> Result<()> {
    attributes.sort_unstable_by(|left, right| left.key().cmp(right.key()));
    write_str(output, "{")?;
    let mut index = 0;
    let mut written = false;
    while index < attributes.len() {
        let key = attributes[index].key();
        let mut next = index + 1;
        while next < attributes.len() && attributes[next].key() == key {
            next += 1;
        }
        let attribute = &attributes[next - 1];
        if let Some(value) = attribute.value() {
            if !omits_object_field(&value) {
                if written {
                    write_str(output, ",")?;
                }
                write_json_string(output, &view_text(key))?;
                write_str(output, ":")?;
                write_any(output, &value)?;
                written = true;
            }
        }
        index = next;
    }
    write_str(output, "}")?;
    Ok(())
}

fn write_attribute_iter<W: fmt::Write, A: AttributeView>(
    output: &mut W,
    attributes: impl Iterator<Item = A>,
) -> Result<()> {
    let mut attributes: Vec<_> = attributes.filter(|attr| attr.value().is_some()).collect();
    write_attributes(output, &mut attributes)
}

fn write_any<'a, W: fmt::Write, V: AnyValueView<'a>>(output: &mut W, value: &V) -> Result<()> {
    match value.value_type() {
        ValueType::Empty => write_str(output, "null"),
        ValueType::String => {
            write_json_string(output, &view_text(value.as_string().unwrap_or_default()))
        }
        ValueType::Bool => write_str(
            output,
            if value.as_bool().unwrap_or(false) {
                "true"
            } else {
                "false"
            },
        ),
        ValueType::Int64 => write_str(output, &value.as_int64().unwrap_or_default().to_string()),
        ValueType::Double => match value.as_double() {
            Some(value) if value.is_finite() => {
                serde_json::to_writer(FmtWriter(output), &value)?;
                Ok(())
            }
            _ => write_str(output, "null"),
        },
        ValueType::Bytes => {
            let value = String::from_utf8_lossy(value.as_bytes().unwrap_or_default());
            write_json_string(output, &value)
        }
        ValueType::Array => {
            write_str(output, "[")?;
            if let Some(values) = value.as_array() {
                for (index, value) in values.enumerate() {
                    if index > 0 {
                        write_str(output, ",")?;
                    }
                    write_any(output, &value)?;
                }
            }
            write_str(output, "]")
        }
        ValueType::KeyValueList => {
            let mut attributes: Vec<_> = value
                .as_kvlist()
                .into_iter()
                .flatten()
                .filter(|attr| attr.value().is_some())
                .collect();
            write_attributes(output, &mut attributes)
        }
    }
}

fn any_to_string<'a, V: AnyValueView<'a>>(value: &V) -> Option<String> {
    match value.value_type() {
        ValueType::Empty => None,
        ValueType::String => Some(view_text(value.as_string()?).into_owned()),
        ValueType::Bool => Some(value.as_bool()?.to_string()),
        ValueType::Int64 => Some(value.as_int64()?.to_string()),
        ValueType::Double => value
            .as_double()
            .filter(|value| value.is_finite())
            .map(|value| value.to_string()),
        ValueType::Bytes => Some(String::from_utf8_lossy(value.as_bytes()?).into_owned()),
        ValueType::Array | ValueType::KeyValueList => {
            let mut output = String::new();
            write_any(&mut output, value).expect("writing JSON to String cannot fail");
            Some(output)
        }
    }
}

fn omits_object_field<'a, V: AnyValueView<'a>>(value: &V) -> bool {
    value.value_type() == ValueType::Double
        && value.as_double().is_some_and(|value| !value.is_finite())
}

fn view_text(value: &[u8]) -> Cow<'_, str> {
    String::from_utf8_lossy(value)
}

fn write_json_string<W: fmt::Write>(output: &mut W, value: &str) -> Result<()> {
    serde_json::to_writer(FmtWriter(output), value)?;
    Ok(())
}

fn write_str<W: fmt::Write>(output: &mut W, value: &str) -> Result<()> {
    output
        .write_str(value)
        .map_err(|_| Error::InvalidInput("failed to write semantic view JSON".to_string()))
}

fn write_u64_as_i64<W: fmt::Write>(output: &mut W, value: u64, field: &str) -> Result<()> {
    let value = i64::try_from(value)
        .map_err(|_| Error::InvalidInput(format!("{field}.time_unix_nano exceeds i64::MAX")))?;
    write_str(output, &value.to_string())
}

fn write_id<W: fmt::Write>(output: &mut W, value: Option<&[u8]>) -> Result<()> {
    write_str(output, "\"")?;
    if let Some(value) = value {
        let mut buffer = [0_u8; 32];
        let encoded = encode_to_str(value, &mut buffer[..value.len() * 2])
            .expect("fixed telemetry identifier buffer has exact capacity");
        write_str(output, encoded)?;
    }
    write_str(output, "\"")
}

fn value_is_finite(value: &Value) -> bool {
    match value {
        Value::Integer(_) => true,
        Value::Double(value) => value.is_finite(),
    }
}

struct FmtWriter<'a, W: fmt::Write>(&'a mut W);

impl<W: fmt::Write> io::Write for FmtWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let value = std::str::from_utf8(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        self.0.write_str(value).map_err(io::Error::other)?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
