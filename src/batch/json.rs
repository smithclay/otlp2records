//! JSON string serialization helpers for OTLP batch columns.

use std::{fmt, io};

use arrow_array::builder::StringBuilder;
use const_hex::{encode as hex_encode, encode_to_str};
use opentelemetry_proto::tonic::{
    common::v1::{any_value, AnyValue, KeyValue},
    metrics::v1::{exemplar, Exemplar},
    trace::v1::span,
};
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Serialize, Serializer,
};

use crate::{Error, Result};

use super::util::u64_to_i64;

pub(super) fn append_u64_json_array(
    builder: &mut StringBuilder,
    values: &[u64],
    scratch: &mut String,
) -> Result<()> {
    scratch.clear();
    scratch.reserve(values.len().saturating_mul(3).saturating_add(2));
    scratch.push('[');
    let mut integer = itoa::Buffer::new();
    for (idx, value) in values.iter().enumerate() {
        if idx > 0 {
            scratch.push(',');
        }
        scratch.push_str(integer.format(*value));
    }
    scratch.push(']');
    builder.append_value(scratch.as_str());
    Ok(())
}

pub(super) fn append_f64_json_array(
    builder: &mut StringBuilder,
    values: &[f64],
    scratch: &mut String,
) -> Result<()> {
    scratch.clear();
    if write_f64_json_array(scratch, values).is_err() {
        scratch.clear();
        scratch.push_str("[]");
    }
    builder.append_value(scratch.as_str());
    Ok(())
}

pub(super) fn append_exemplars_json(
    builder: &mut StringBuilder,
    exemplars: &[Exemplar],
    scratch: &mut String,
) -> Result<()> {
    if exemplars.is_empty() {
        builder.append_null();
        return Ok(());
    }

    scratch.clear();
    write_str_builder(scratch, "[")?;
    for (idx, exemplar) in exemplars.iter().enumerate() {
        if idx > 0 {
            write_str_builder(scratch, ",")?;
        }
        write_str_builder(scratch, "{\"filtered_attributes\":")?;
        write_attrs_object(scratch, &exemplar.filtered_attributes)?;
        write_str_builder(scratch, ",\"span_id\":")?;
        write_hex_json(scratch, &exemplar.span_id)?;
        write_str_builder(scratch, ",\"time_unix_nano\":")?;
        write_i64_builder(
            scratch,
            u64_to_i64(exemplar.time_unix_nano, "exemplar.time_unix_nano")?,
        )?;
        write_str_builder(scratch, ",\"trace_id\":")?;
        write_hex_json(scratch, &exemplar.trace_id)?;
        if exemplar_value_is_finite(&exemplar.value) {
            write_str_builder(scratch, ",\"value\":")?;
            write_exemplar_value_json(scratch, &exemplar.value)?;
        }
        write_str_builder(scratch, "}")?;
    }
    write_str_builder(scratch, "]")?;
    builder.append_value(scratch.as_str());
    Ok(())
}

pub(super) fn append_span_events_json(
    builder: &mut StringBuilder,
    events: &[span::Event],
    scratch: &mut String,
) -> Result<()> {
    if events.is_empty() {
        builder.append_null();
        return Ok(());
    }

    scratch.clear();
    write_str_builder(scratch, "[")?;
    for (idx, event) in events.iter().enumerate() {
        if idx > 0 {
            write_str_builder(scratch, ",")?;
        }
        write_str_builder(scratch, "{\"attributes\":")?;
        write_attrs_object(scratch, &event.attributes)?;
        write_str_builder(scratch, ",\"name\":")?;
        write_json_string(scratch, &event.name)?;
        write_str_builder(scratch, ",\"time_unix_nano\":")?;
        write_i64_builder(
            scratch,
            u64_to_i64(event.time_unix_nano, "event.time_unix_nano")?,
        )?;
        write_str_builder(scratch, "}")?;
    }
    write_str_builder(scratch, "]")?;
    builder.append_value(scratch.as_str());
    Ok(())
}

pub(super) fn append_span_links_json(
    builder: &mut StringBuilder,
    links: &[span::Link],
    scratch: &mut String,
) -> Result<()> {
    if links.is_empty() {
        builder.append_null();
        return Ok(());
    }

    scratch.clear();
    write_str_builder(scratch, "[")?;
    for (idx, link) in links.iter().enumerate() {
        if idx > 0 {
            write_str_builder(scratch, ",")?;
        }
        write_str_builder(scratch, "{\"attributes\":")?;
        write_attrs_object(scratch, &link.attributes)?;
        write_str_builder(scratch, ",\"span_id\":")?;
        write_hex_json(scratch, &link.span_id)?;
        write_str_builder(scratch, ",\"trace_id\":")?;
        write_hex_json(scratch, &link.trace_id)?;
        write_str_builder(scratch, ",\"trace_state\":")?;
        write_json_string(scratch, &link.trace_state)?;
        write_str_builder(scratch, "}")?;
    }
    write_str_builder(scratch, "]")?;
    builder.append_value(scratch.as_str());
    Ok(())
}

#[inline]
pub(super) fn append_log_body(builder: &mut StringBuilder, value: Option<&AnyValue>) -> Result<()> {
    let Some(value) = value.and_then(|value| value.value.as_ref()) else {
        builder.append_null();
        return Ok(());
    };

    match value {
        any_value::Value::StringValue(value) if value.is_empty() => builder.append_null(),
        any_value::Value::StringValue(value) => builder.append_value(value),
        any_value::Value::BoolValue(value) => {
            write_str_builder(builder, if *value { "true" } else { "false" })?;
            builder.append_value("");
        }
        any_value::Value::IntValue(value) => {
            write_i64_builder(builder, *value)?;
            builder.append_value("");
        }
        any_value::Value::DoubleValue(value) if value.is_finite() => {
            serde_json::to_writer(FmtWriter(builder), value)?;
            builder.append_value("");
        }
        any_value::Value::DoubleValue(_) => builder.append_null(),
        any_value::Value::BytesValue(value) if value.is_empty() => builder.append_null(),
        any_value::Value::BytesValue(value) => {
            let value = String::from_utf8_lossy(value);
            builder.append_value(&value);
        }
        any_value::Value::ArrayValue(_) | any_value::Value::KvlistValue(_) => {
            write_any_json(builder, value)?;
            builder.append_value("");
        }
    }
    Ok(())
}

#[inline]
pub(super) fn append_attrs_json(
    builder: &mut StringBuilder,
    attrs: &[KeyValue],
    scratch: &mut String,
) -> Result<()> {
    if !attrs.iter().any(|kv| kv.value.is_some()) {
        builder.append_null();
        return Ok(());
    }

    scratch.clear();
    write_attrs_object(scratch, attrs)?;
    builder.append_value(scratch.as_str());
    Ok(())
}

#[inline]
pub(super) fn attrs_json(attrs: &[KeyValue]) -> Option<String> {
    if !attrs.iter().any(|kv| kv.value.is_some()) {
        None
    } else {
        Some(serde_json::to_string(&AttrsJson(attrs)).expect("attributes serialize to JSON"))
    }
}

#[inline]
pub(super) fn attr_field(attrs: &[KeyValue], key: &str) -> Option<String> {
    attrs
        .iter()
        .find(|kv| kv.key == key)
        .and_then(|kv| kv.value.as_ref())
        .and_then(any_to_arrow_string)
        .filter(|value| !value.is_empty())
}

#[inline]
fn any_to_arrow_string(value: &AnyValue) -> Option<String> {
    match value.value.as_ref()? {
        any_value::Value::StringValue(value) => Some(value.clone()),
        any_value::Value::BoolValue(value) => Some(value.to_string()),
        any_value::Value::IntValue(value) => Some(value.to_string()),
        any_value::Value::DoubleValue(value) if value.is_finite() => Some(value.to_string()),
        any_value::Value::DoubleValue(_) => None,
        any_value::Value::BytesValue(value) => Some(String::from_utf8_lossy(value).into_owned()),
        any_value::Value::ArrayValue(_) | any_value::Value::KvlistValue(_) => {
            Some(any_json_string(value.value.as_ref().unwrap()))
        }
    }
}

#[inline]
fn any_json_string(value: &any_value::Value) -> String {
    serde_json::to_string(&AnyJson(value)).expect("OTLP AnyValue serializes to JSON")
}

#[inline]
fn write_any_json<W: fmt::Write + ?Sized>(builder: &mut W, value: &any_value::Value) -> Result<()> {
    match value {
        any_value::Value::StringValue(value) => write_json_string(builder, value),
        any_value::Value::BoolValue(value) => {
            write_str_builder(builder, if *value { "true" } else { "false" })
        }
        any_value::Value::IntValue(value) => write_i64_builder(builder, *value),
        any_value::Value::DoubleValue(value) if value.is_finite() => {
            serde_json::to_writer(FmtWriter(builder), value)?;
            Ok(())
        }
        any_value::Value::DoubleValue(_) => write_str_builder(builder, "null"),
        any_value::Value::BytesValue(value) => {
            let value = String::from_utf8_lossy(value);
            write_json_string(builder, &value)
        }
        any_value::Value::ArrayValue(value) => {
            write_str_builder(builder, "[")?;
            for (idx, item) in value.values.iter().enumerate() {
                if idx > 0 {
                    write_str_builder(builder, ",")?;
                }
                if let Some(value) = item.value.as_ref() {
                    write_any_json(builder, value)?;
                } else {
                    write_str_builder(builder, "null")?;
                }
            }
            write_str_builder(builder, "]")
        }
        any_value::Value::KvlistValue(value) => write_attrs_object(builder, &value.values),
    }
}

#[inline]
fn write_attrs_object<W: fmt::Write + ?Sized>(builder: &mut W, attrs: &[KeyValue]) -> Result<()> {
    if attrs_are_sorted_unique(attrs) {
        write_str_builder(builder, "{")?;
        let mut written = false;
        for kv in attrs {
            if let Some(value) = kv.value.as_ref().and_then(|value| value.value.as_ref()) {
                if any_omits_object_field(value) {
                    continue;
                }
                if written {
                    write_str_builder(builder, ",")?;
                }
                write_json_string(builder, &kv.key)?;
                write_str_builder(builder, ":")?;
                write_any_json(builder, value)?;
                written = true;
            }
        }
        write_str_builder(builder, "}")?;
        return Ok(());
    }

    let mut values: Vec<_> = attrs
        .iter()
        .filter_map(|kv| {
            kv.value
                .as_ref()
                .and_then(|value| value.value.as_ref())
                .map(|value| (kv.key.as_str(), value))
        })
        .collect();
    values.sort_unstable_by(|left, right| left.0.cmp(right.0));

    write_str_builder(builder, "{")?;
    let mut idx = 0;
    let mut written = false;
    while idx < values.len() {
        let key = values[idx].0;
        let mut next = idx + 1;
        while next < values.len() && values[next].0 == key {
            next += 1;
        }
        let value = values[next - 1].1;
        if !any_omits_object_field(value) {
            if written {
                write_str_builder(builder, ",")?;
            }
            write_json_string(builder, key)?;
            write_str_builder(builder, ":")?;
            write_any_json(builder, value)?;
            written = true;
        }
        idx = next;
    }
    write_str_builder(builder, "}")?;
    Ok(())
}

#[inline]
fn write_json_string<W: fmt::Write + ?Sized>(builder: &mut W, value: &str) -> Result<()> {
    if !value
        .as_bytes()
        .iter()
        .any(|byte| *byte < 0x20 || *byte == b'"' || *byte == b'\\')
    {
        write_str_builder(builder, "\"")?;
        write_str_builder(builder, value)?;
        write_str_builder(builder, "\"")?;
        return Ok(());
    }

    serde_json::to_writer(FmtWriter(builder), value)?;
    Ok(())
}

#[inline]
fn write_i64_builder<W: fmt::Write + ?Sized>(builder: &mut W, value: i64) -> Result<()> {
    fmt::Write::write_fmt(builder, format_args!("{value}")).map_err(|_| {
        Error::InvalidInput("failed to write integer to Arrow string builder".to_string())
    })
}

fn write_f64_json_array<W: fmt::Write + ?Sized>(builder: &mut W, values: &[f64]) -> Result<()> {
    write_str_builder(builder, "[")?;
    for (idx, value) in values.iter().enumerate() {
        if idx > 0 {
            write_str_builder(builder, ",")?;
        }
        serde_json::to_writer(FmtWriter(&mut *builder), value)?;
    }
    write_str_builder(builder, "]")
}

#[inline]
fn write_exemplar_value_json<W: fmt::Write + ?Sized>(
    builder: &mut W,
    value: &Option<exemplar::Value>,
) -> Result<()> {
    match value {
        Some(exemplar::Value::AsInt(value)) => {
            serde_json::to_writer(FmtWriter(builder), &(*value as f64))?;
            Ok(())
        }
        Some(exemplar::Value::AsDouble(value)) if value.is_finite() => {
            serde_json::to_writer(FmtWriter(builder), value)?;
            Ok(())
        }
        _ => write_str_builder(builder, "null"),
    }
}

#[inline]
fn write_hex_json<W: fmt::Write + ?Sized>(builder: &mut W, bytes: &[u8]) -> Result<()> {
    write_str_builder(builder, "\"")?;
    if bytes.len() <= 32 {
        let mut buf = [0_u8; 64];
        let hex = encode_to_str(bytes, &mut buf[..bytes.len() * 2])
            .expect("hex buffer length is exactly input length * 2");
        write_str_builder(builder, hex)?;
    } else {
        write_str_builder(builder, &hex_encode(bytes))?;
    }
    write_str_builder(builder, "\"")
}

#[inline]
fn write_str_builder<W: fmt::Write + ?Sized>(builder: &mut W, value: &str) -> Result<()> {
    fmt::Write::write_str(builder, value).map_err(|_| {
        Error::InvalidInput("failed to write string to Arrow string builder".to_string())
    })
}

struct FmtWriter<'a, W: fmt::Write + ?Sized>(&'a mut W);

impl<W: fmt::Write + ?Sized> io::Write for FmtWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let text = std::str::from_utf8(buf).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("JSON serializer wrote invalid UTF-8: {err}"),
            )
        })?;
        fmt::Write::write_str(self.0, text).map_err(io::Error::other)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct AttrsJson<'a>(&'a [KeyValue]);

impl Serialize for AttrsJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        if attrs_are_sorted_unique(self.0) {
            let mut map = serializer.serialize_map(None)?;
            for kv in self.0 {
                if let Some(value) = kv.value.as_ref().and_then(|value| value.value.as_ref()) {
                    if !any_omits_object_field(value) {
                        map.serialize_entry(kv.key.as_str(), &AnyJson(value))?;
                    }
                }
            }
            return map.end();
        }

        let mut values: Vec<_> = self
            .0
            .iter()
            .filter_map(|kv| {
                kv.value
                    .as_ref()
                    .and_then(|value| value.value.as_ref())
                    .map(|value| (kv.key.as_str(), value))
            })
            .collect();
        values.sort_unstable_by(|left, right| left.0.cmp(right.0));

        let mut map = serializer.serialize_map(None)?;
        let mut idx = 0;
        while idx < values.len() {
            let key = values[idx].0;
            let mut next = idx + 1;
            while next < values.len() && values[next].0 == key {
                next += 1;
            }
            let value = values[next - 1].1;
            if !any_omits_object_field(value) {
                map.serialize_entry(key, &AnyJson(value))?;
            }
            idx = next;
        }
        map.end()
    }
}

#[inline]
fn attrs_are_sorted_unique(attrs: &[KeyValue]) -> bool {
    let mut last_key = None;
    for kv in attrs {
        if kv.value.is_none() {
            continue;
        }
        if let Some(last_key) = last_key {
            if last_key >= kv.key.as_str() {
                return false;
            }
        }
        last_key = Some(kv.key.as_str());
    }
    true
}

struct AnyJson<'a>(&'a any_value::Value);

impl Serialize for AnyJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self.0 {
            any_value::Value::StringValue(value) => serializer.serialize_str(value),
            any_value::Value::BoolValue(value) => serializer.serialize_bool(*value),
            any_value::Value::IntValue(value) => serializer.serialize_i64(*value),
            any_value::Value::DoubleValue(value) if value.is_finite() => {
                serializer.serialize_f64(*value)
            }
            any_value::Value::DoubleValue(_) => serializer.serialize_unit(),
            any_value::Value::BytesValue(value) => {
                let value = String::from_utf8_lossy(value);
                serializer.serialize_str(&value)
            }
            any_value::Value::ArrayValue(value) => {
                let mut seq = serializer.serialize_seq(Some(value.values.len()))?;
                for item in &value.values {
                    match item.value.as_ref() {
                        Some(value) => seq.serialize_element(&AnyJson(value))?,
                        None => seq.serialize_element(&None::<()>)?,
                    }
                }
                seq.end()
            }
            any_value::Value::KvlistValue(value) => AttrsJson(&value.values).serialize(serializer),
        }
    }
}

#[inline]
fn any_omits_object_field(value: &any_value::Value) -> bool {
    matches!(value, any_value::Value::DoubleValue(value) if !value.is_finite())
}

#[inline]
fn exemplar_value_is_finite(value: &Option<exemplar::Value>) -> bool {
    match value {
        Some(exemplar::Value::AsInt(_)) => true,
        Some(exemplar::Value::AsDouble(value)) => value.is_finite(),
        None => false,
    }
}
