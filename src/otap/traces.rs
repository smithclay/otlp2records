use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{Array, RecordBatch, StructArray, UInt32Array};

use crate::{
    batch::transform_traces_view,
    views::pdata::{
        EventView, InstrumentationScopeView, LinkView, ResourceSpansView, ResourceView,
        ScopeSpansView, SpanId, SpanView, StatusView, Str, TraceId, TracesView,
    },
    Error, Result,
};

use super::logs::{
    bool_at, build_groups, bytes_at, column_string, column_u16, column_u32, decode_attr_parent_ids,
    decode_quasi_delta, decode_root_ids, duration_nanos_at, f64_at, i32_at, i64_at, id, index_u16,
    is_plain, nested_string, nested_u32_value, replace, same_attribute_value, string_at, timestamp,
    u16_at, u32_at, u8_at, validate_attr_value_rows, validate_attrs, Attr16Iter, Attr32Iter,
    AttributeTable, AttributeTable32, OtapAttribute, ResourceGroup, ScopeGroup,
};
use super::validation::{validate, PayloadSchema};

#[allow(clippy::too_many_arguments)]
pub(super) fn normalize(
    spans: RecordBatch,
    resource_attrs: Option<RecordBatch>,
    scope_attrs: Option<RecordBatch>,
    span_attrs: Option<RecordBatch>,
    events: Option<RecordBatch>,
    event_attrs: Option<RecordBatch>,
    links: Option<RecordBatch>,
    link_attrs: Option<RecordBatch>,
    input_bytes: usize,
) -> Result<RecordBatch> {
    validate(PayloadSchema::Spans, "spans", &spans)?;
    for (payload, name, batch) in [
        (
            PayloadSchema::Attrs16,
            "resource attributes",
            resource_attrs.as_ref(),
        ),
        (
            PayloadSchema::Attrs16,
            "scope attributes",
            scope_attrs.as_ref(),
        ),
        (
            PayloadSchema::Attrs16,
            "span attributes",
            span_attrs.as_ref(),
        ),
        (PayloadSchema::SpanEvents, "span events", events.as_ref()),
        (
            PayloadSchema::Attrs32,
            "span event attributes",
            event_attrs.as_ref(),
        ),
        (PayloadSchema::SpanLinks, "span links", links.as_ref()),
        (
            PayloadSchema::Attrs32,
            "span link attributes",
            link_attrs.as_ref(),
        ),
    ] {
        if let Some(batch) = batch {
            validate(payload, name, batch)?;
            if matches!(payload, PayloadSchema::Attrs32) {
                validate_attr_value_rows(name, batch)?;
            }
        }
    }
    for (name, batch) in [
        ("resource attributes", resource_attrs.as_ref()),
        ("scope attributes", scope_attrs.as_ref()),
        ("span attributes", span_attrs.as_ref()),
    ] {
        if let Some(batch) = batch {
            validate_attrs(name, batch)?;
        }
    }

    let spans = decode_root_ids(spans)?;
    let resource_attrs = resource_attrs.map(decode_attr_parent_ids).transpose()?;
    let scope_attrs = scope_attrs.map(decode_attr_parent_ids).transpose()?;
    let span_attrs = span_attrs.map(decode_attr_parent_ids).transpose()?;
    let events = events
        .map(|batch| decode_child(batch, &["name"]))
        .transpose()?;
    let links = links
        .map(|batch| decode_child(batch, &["trace_id"]))
        .transpose()?;
    let event_attrs = event_attrs.map(decode_attr_parent_ids_u32).transpose()?;
    let link_attrs = link_attrs.map(decode_attr_parent_ids_u32).transpose()?;

    let view = OtapTracesView::new(
        spans,
        resource_attrs,
        scope_attrs,
        span_attrs,
        events,
        event_attrs,
        links,
        link_attrs,
    )?;
    transform_traces_view(&view, input_bytes)
}

struct OtapTracesView {
    spans: RecordBatch,
    resource_attrs: Option<AttributeTable>,
    scope_attrs: Option<AttributeTable>,
    span_attrs: Option<AttributeTable>,
    events: Option<RecordBatch>,
    event_attrs: Option<AttributeTable32>,
    links: Option<RecordBatch>,
    link_attrs: Option<AttributeTable32>,
    resources: Vec<ResourceGroup>,
    events_by_parent: BTreeMap<u16, Vec<usize>>,
    links_by_parent: BTreeMap<u16, Vec<usize>>,
}

impl OtapTracesView {
    #[allow(clippy::too_many_arguments)]
    fn new(
        spans: RecordBatch,
        resource_attrs: Option<RecordBatch>,
        scope_attrs: Option<RecordBatch>,
        span_attrs: Option<RecordBatch>,
        events: Option<RecordBatch>,
        event_attrs: Option<RecordBatch>,
        links: Option<RecordBatch>,
        link_attrs: Option<RecordBatch>,
    ) -> Result<Self> {
        let resources = build_groups(&spans)?;
        let events_by_parent = events
            .as_ref()
            .map(|batch| index_u16(batch, "parent_id"))
            .transpose()?
            .unwrap_or_default();
        let links_by_parent = links
            .as_ref()
            .map(|batch| index_u16(batch, "parent_id"))
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            spans,
            resource_attrs: resource_attrs.map(AttributeTable::new).transpose()?,
            scope_attrs: scope_attrs.map(AttributeTable::new).transpose()?,
            span_attrs: span_attrs.map(AttributeTable::new).transpose()?,
            events,
            event_attrs: event_attrs.map(AttributeTable32::new).transpose()?,
            links,
            link_attrs: link_attrs.map(AttributeTable32::new).transpose()?,
            resources,
            events_by_parent,
            links_by_parent,
        })
    }
}

impl TracesView for OtapTracesView {
    type ResourceSpans<'a>
        = OtapResourceSpans<'a>
    where
        Self: 'a;
    type ResourcesIter<'a>
        = ResourceIter<'a>
    where
        Self: 'a;

    fn resources(&self) -> Self::ResourcesIter<'_> {
        ResourceIter {
            view: self,
            groups: self.resources.iter(),
        }
    }
}

struct ResourceIter<'a> {
    view: &'a OtapTracesView,
    groups: std::slice::Iter<'a, ResourceGroup>,
}

impl<'a> Iterator for ResourceIter<'a> {
    type Item = OtapResourceSpans<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        Some(OtapResourceSpans {
            view: self.view,
            group: self.groups.next()?,
        })
    }
}

struct OtapResourceSpans<'a> {
    view: &'a OtapTracesView,
    group: &'a ResourceGroup,
}

impl ResourceSpansView for OtapResourceSpans<'_> {
    type Resource<'a>
        = OtapResource<'a>
    where
        Self: 'a;
    type ScopeSpans<'a>
        = OtapScopeSpans<'a>
    where
        Self: 'a;
    type ScopesIter<'a>
        = ScopeIter<'a>
    where
        Self: 'a;

    fn resource(&self) -> Option<Self::Resource<'_>> {
        Some(OtapResource {
            view: self.view,
            id: self.group.id,
            row: self.group.representative,
        })
    }
    fn scopes(&self) -> Self::ScopesIter<'_> {
        ScopeIter {
            view: self.view,
            groups: self.group.scopes.iter(),
        }
    }
    fn schema_url(&self) -> Option<Str<'_>> {
        nested_string(
            &self.view.spans,
            "resource",
            "schema_url",
            self.group.representative,
        )
    }
}

struct ScopeIter<'a> {
    view: &'a OtapTracesView,
    groups: std::slice::Iter<'a, ScopeGroup>,
}

impl<'a> Iterator for ScopeIter<'a> {
    type Item = OtapScopeSpans<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        Some(OtapScopeSpans {
            view: self.view,
            group: self.groups.next()?,
        })
    }
}

struct OtapScopeSpans<'a> {
    view: &'a OtapTracesView,
    group: &'a ScopeGroup,
}

impl ScopeSpansView for OtapScopeSpans<'_> {
    type Scope<'a>
        = OtapScope<'a>
    where
        Self: 'a;
    type Span<'a>
        = OtapSpan<'a>
    where
        Self: 'a;
    type SpanIter<'a>
        = SpanIter<'a>
    where
        Self: 'a;

    fn scope(&self) -> Option<Self::Scope<'_>> {
        Some(OtapScope {
            view: self.view,
            id: self.group.id,
            row: self.group.representative,
        })
    }
    fn spans(&self) -> Self::SpanIter<'_> {
        SpanIter {
            view: self.view,
            rows: self.group.rows.iter(),
        }
    }
    fn schema_url(&self) -> Option<Str<'_>> {
        nested_string(
            &self.view.spans,
            "scope",
            "schema_url",
            self.group.representative,
        )
    }
}

struct OtapResource<'a> {
    view: &'a OtapTracesView,
    id: Option<u16>,
    row: usize,
}

impl ResourceView for OtapResource<'_> {
    type Attribute<'a>
        = OtapAttribute<'a>
    where
        Self: 'a;
    type AttributesIter<'a>
        = Attr16Iter<'a>
    where
        Self: 'a;
    fn attributes(&self) -> Self::AttributesIter<'_> {
        Attr16Iter::new(self.view.resource_attrs.as_ref(), self.id)
    }
    fn dropped_attributes_count(&self) -> u32 {
        nested_u32_value(
            &self.view.spans,
            "resource",
            "dropped_attributes_count",
            self.row,
        )
        .unwrap_or_default()
    }
}

struct OtapScope<'a> {
    view: &'a OtapTracesView,
    id: Option<u16>,
    row: usize,
}

impl InstrumentationScopeView for OtapScope<'_> {
    type Attribute<'a>
        = OtapAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = Attr16Iter<'a>
    where
        Self: 'a;
    fn name(&self) -> Option<Str<'_>> {
        nested_string(&self.view.spans, "scope", "name", self.row)
    }
    fn version(&self) -> Option<Str<'_>> {
        nested_string(&self.view.spans, "scope", "version", self.row)
    }
    fn attributes(&self) -> Self::AttributeIter<'_> {
        Attr16Iter::new(self.view.scope_attrs.as_ref(), self.id)
    }
    fn dropped_attributes_count(&self) -> u32 {
        nested_u32_value(
            &self.view.spans,
            "scope",
            "dropped_attributes_count",
            self.row,
        )
        .unwrap_or_default()
    }
}

struct SpanIter<'a> {
    view: &'a OtapTracesView,
    rows: std::slice::Iter<'a, usize>,
}

impl<'a> Iterator for SpanIter<'a> {
    type Item = OtapSpan<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        Some(OtapSpan {
            view: self.view,
            row: *self.rows.next()?,
        })
    }
}

struct OtapSpan<'a> {
    view: &'a OtapTracesView,
    row: usize,
}

impl SpanView for OtapSpan<'_> {
    type Attribute<'a>
        = OtapAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = Attr16Iter<'a>
    where
        Self: 'a;
    type Event<'a>
        = OtapEvent<'a>
    where
        Self: 'a;
    type EventsIter<'a>
        = EventIter<'a>
    where
        Self: 'a;
    type Link<'a>
        = OtapLink<'a>
    where
        Self: 'a;
    type LinksIter<'a>
        = LinkIter<'a>
    where
        Self: 'a;
    type Status<'a>
        = OtapStatus<'a>
    where
        Self: 'a;

    fn trace_id(&self) -> Option<&TraceId> {
        id(&self.view.spans, "trace_id", self.row)
    }
    fn span_id(&self) -> Option<&SpanId> {
        id(&self.view.spans, "span_id", self.row)
    }
    fn trace_state(&self) -> Option<Str<'_>> {
        column_string(&self.view.spans, "trace_state", self.row)
    }
    fn parent_span_id(&self) -> Option<&SpanId> {
        id(&self.view.spans, "parent_span_id", self.row)
    }
    fn flags(&self) -> Option<u32> {
        column_u32(&self.view.spans, "flags", self.row)
    }
    fn name(&self) -> Option<Str<'_>> {
        column_string(&self.view.spans, "name", self.row)
    }
    fn kind(&self) -> i32 {
        self.view
            .spans
            .column_by_name("kind")
            .and_then(|array| i32_at(array, self.row))
            .unwrap_or_default()
    }
    fn start_time_unix_nano(&self) -> Option<u64> {
        timestamp(&self.view.spans, "start_time_unix_nano", self.row)
    }
    fn end_time_unix_nano(&self) -> Option<u64> {
        let start = self.start_time_unix_nano()?;
        let duration = self
            .view
            .spans
            .column_by_name("duration_time_unix_nano")
            .and_then(|array| duration_nanos_at(array, self.row))
            .and_then(|value| u64::try_from(value).ok())
            .unwrap_or_default();
        Some(start.saturating_add(duration))
    }
    fn attributes(&self) -> Self::AttributeIter<'_> {
        Attr16Iter::new(
            self.view.span_attrs.as_ref(),
            column_u16(&self.view.spans, "id", self.row),
        )
    }
    fn dropped_attributes_count(&self) -> u32 {
        column_u32(&self.view.spans, "dropped_attributes_count", self.row).unwrap_or_default()
    }
    fn events(&self) -> Self::EventsIter<'_> {
        EventIter::new(
            self.view,
            column_u16(&self.view.spans, "id", self.row)
                .and_then(|id| self.view.events_by_parent.get(&id)),
        )
    }
    fn dropped_events_count(&self) -> u32 {
        column_u32(&self.view.spans, "dropped_events_count", self.row).unwrap_or_default()
    }
    fn links(&self) -> Self::LinksIter<'_> {
        LinkIter::new(
            self.view,
            column_u16(&self.view.spans, "id", self.row)
                .and_then(|id| self.view.links_by_parent.get(&id)),
        )
    }
    fn dropped_links_count(&self) -> u32 {
        column_u32(&self.view.spans, "dropped_links_count", self.row).unwrap_or_default()
    }
    fn status(&self) -> Option<Self::Status<'_>> {
        let status = self
            .view
            .spans
            .column_by_name("status")?
            .as_any()
            .downcast_ref::<StructArray>()?;
        (!status.is_null(self.row)).then_some(OtapStatus {
            status,
            row: self.row,
        })
    }
}

struct OtapStatus<'a> {
    status: &'a StructArray,
    row: usize,
}

impl StatusView for OtapStatus<'_> {
    fn message(&self) -> Option<Str<'_>> {
        self.status
            .column_by_name("status_message")
            .and_then(|array| string_at(array, self.row))
    }
    fn status_code(&self) -> i32 {
        self.status
            .column_by_name("code")
            .and_then(|array| i32_at(array, self.row))
            .unwrap_or_default()
    }
}

struct EventIter<'a> {
    view: &'a OtapTracesView,
    rows: std::slice::Iter<'a, usize>,
}

impl<'a> EventIter<'a> {
    fn new(view: &'a OtapTracesView, rows: Option<&'a Vec<usize>>) -> Self {
        Self {
            view,
            rows: rows.map(Vec::as_slice).unwrap_or_default().iter(),
        }
    }
}

impl<'a> Iterator for EventIter<'a> {
    type Item = OtapEvent<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        Some(OtapEvent {
            view: self.view,
            row: *self.rows.next()?,
        })
    }
}

struct OtapEvent<'a> {
    view: &'a OtapTracesView,
    row: usize,
}

impl EventView for OtapEvent<'_> {
    type Attribute<'a>
        = OtapAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = Attr32Iter<'a>
    where
        Self: 'a;
    fn time_unix_nano(&self) -> Option<u64> {
        self.view
            .events
            .as_ref()
            .and_then(|batch| timestamp(batch, "time_unix_nano", self.row))
    }
    fn name(&self) -> Option<Str<'_>> {
        self.view
            .events
            .as_ref()
            .and_then(|batch| column_string(batch, "name", self.row))
    }
    fn attributes(&self) -> Self::AttributeIter<'_> {
        let id = self
            .view
            .events
            .as_ref()
            .and_then(|batch| column_u32(batch, "id", self.row));
        Attr32Iter::new(self.view.event_attrs.as_ref(), id)
    }
    fn dropped_attributes_count(&self) -> u32 {
        self.view
            .events
            .as_ref()
            .and_then(|batch| column_u32(batch, "dropped_attributes_count", self.row))
            .unwrap_or_default()
    }
}

struct LinkIter<'a> {
    view: &'a OtapTracesView,
    rows: std::slice::Iter<'a, usize>,
}

impl<'a> LinkIter<'a> {
    fn new(view: &'a OtapTracesView, rows: Option<&'a Vec<usize>>) -> Self {
        Self {
            view,
            rows: rows.map(Vec::as_slice).unwrap_or_default().iter(),
        }
    }
}

impl<'a> Iterator for LinkIter<'a> {
    type Item = OtapLink<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        Some(OtapLink {
            view: self.view,
            row: *self.rows.next()?,
        })
    }
}

struct OtapLink<'a> {
    view: &'a OtapTracesView,
    row: usize,
}

impl LinkView for OtapLink<'_> {
    type Attribute<'a>
        = OtapAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = Attr32Iter<'a>
    where
        Self: 'a;
    fn trace_id(&self) -> Option<&TraceId> {
        self.view
            .links
            .as_ref()
            .and_then(|batch| id(batch, "trace_id", self.row))
    }
    fn span_id(&self) -> Option<&SpanId> {
        self.view
            .links
            .as_ref()
            .and_then(|batch| id(batch, "span_id", self.row))
    }
    fn trace_state(&self) -> Option<Str<'_>> {
        self.view
            .links
            .as_ref()
            .and_then(|batch| column_string(batch, "trace_state", self.row))
    }
    fn attributes(&self) -> Self::AttributeIter<'_> {
        let id = self
            .view
            .links
            .as_ref()
            .and_then(|batch| column_u32(batch, "id", self.row));
        Attr32Iter::new(self.view.link_attrs.as_ref(), id)
    }
    fn dropped_attributes_count(&self) -> u32 {
        self.view
            .links
            .as_ref()
            .and_then(|batch| column_u32(batch, "dropped_attributes_count", self.row))
            .unwrap_or_default()
    }
    fn flags(&self) -> Option<u32> {
        self.view
            .links
            .as_ref()
            .and_then(|batch| column_u32(batch, "flags", self.row))
    }
}

fn decode_child(mut batch: RecordBatch, equal_columns: &[&str]) -> Result<RecordBatch> {
    batch = decode_delta_u32(batch, "id")?;
    decode_quasi_u16(batch, "parent_id", equal_columns)
}

pub(super) fn decode_delta_u32(batch: RecordBatch, name: &str) -> Result<RecordBatch> {
    let Some(index) = batch.schema().index_of(name).ok() else {
        return Ok(batch);
    };
    if is_plain(batch.schema().field(index)) {
        return Ok(batch);
    }
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| Error::Otap(format!("{name} must be UInt32")))?;
    let mut accumulator = 0_u32;
    let mut values = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        if array.is_null(row) {
            values.push(None);
        } else {
            accumulator = accumulator.checked_add(array.value(row)).ok_or_else(|| {
                Error::Otap(format!("{name} delta overflows UInt32 at row {row}"))
            })?;
            values.push(Some(accumulator));
        }
    }
    replace(&batch, index, Arc::new(UInt32Array::from(values)))
}

fn decode_quasi_u16(batch: RecordBatch, name: &str, equal_columns: &[&str]) -> Result<RecordBatch> {
    decode_quasi_delta::<u16>(batch, name, |batch, left, right| {
        equal_columns
            .iter()
            .all(|column| values_equal(batch, column, left, right))
    })
}

pub(super) fn decode_attr_parent_ids_u32(batch: RecordBatch) -> Result<RecordBatch> {
    decode_quasi_delta::<u32>(batch, "parent_id", same_attribute_value)
}

pub(super) fn values_equal(batch: &RecordBatch, name: &str, left: usize, right: usize) -> bool {
    let Some(array) = batch.column_by_name(name) else {
        return true;
    };
    if array.is_null(left) || array.is_null(right) {
        return array.is_null(left) == array.is_null(right);
    }
    if let (Some(left), Some(right)) = (string_at(array, left), string_at(array, right)) {
        return left == right;
    }
    if let (Some(left), Some(right)) = (bytes_at(array, left), bytes_at(array, right)) {
        return left == right;
    }
    if let (Some(left), Some(right)) = (u32_at(array, left), u32_at(array, right)) {
        return left == right;
    }
    if let (Some(left), Some(right)) = (u16_at(array, left), u16_at(array, right)) {
        return left == right;
    }
    if let (Some(left), Some(right)) = (u8_at(array, left), u8_at(array, right)) {
        return left == right;
    }
    if let (Some(left), Some(right)) = (i32_at(array, left), i32_at(array, right)) {
        return left == right;
    }
    if let (Some(left), Some(right)) = (i64_at(array, left), i64_at(array, right)) {
        return left == right;
    }
    if let (Some(left), Some(right)) = (f64_at(array, left), f64_at(array, right)) {
        return left.to_bits() == right.to_bits();
    }
    if let (Some(left), Some(right)) = (bool_at(array, left), bool_at(array, right)) {
        return left == right;
    }
    false
}

#[cfg(test)]
mod transport_tests {
    use arrow_array::{BinaryArray, StringArray, UInt8Array};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn attribute_parent_ids_do_not_delta_decode_nested_values() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("parent_id", DataType::UInt32, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("type", DataType::UInt8, false),
                Field::new("ser", DataType::Binary, true),
            ])),
            vec![
                Arc::new(UInt32Array::from(vec![7, 2])),
                Arc::new(StringArray::from(vec!["nested", "nested"])),
                Arc::new(UInt8Array::from(vec![5, 5])),
                Arc::new(BinaryArray::from(vec![
                    Some(&[0xa0][..]),
                    Some(&[0xa0][..]),
                ])),
            ],
        )
        .unwrap();

        let decoded = decode_attr_parent_ids_u32(batch).unwrap();
        let parent_ids = decoded
            .column_by_name("parent_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(parent_ids.values(), &[7, 2]);
    }

    #[test]
    fn rejects_unknown_value_type_in_32bit_attributes() {
        // A 32-bit attribute table (data point / exemplar / span event / span
        // link) carrying an unknown AnyValue discriminant must be rejected, not
        // silently coerced to an empty value.
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("parent_id", DataType::UInt32, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("type", DataType::UInt8, false),
                Field::new("str", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(UInt32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["k"])),
                Arc::new(UInt8Array::from(vec![99])),
                Arc::new(StringArray::from(vec![Some("v")])),
            ],
        )
        .unwrap();

        let error = validate_attr_value_rows("span event attributes", &batch).unwrap_err();
        assert!(
            error.to_string().contains("unknown AnyValue type 99"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_missing_payload_in_32bit_attributes() {
        // A declared string value whose payload column is null at that row must
        // be rejected rather than silently decoded as empty.
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("parent_id", DataType::UInt32, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("type", DataType::UInt8, false),
                Field::new("str", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(UInt32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["k"])),
                Arc::new(UInt8Array::from(vec![super::super::wire::VALUE_STR])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
        )
        .unwrap();

        let error = validate_attr_value_rows("span link attributes", &batch).unwrap_err();
        assert!(
            error.to_string().contains("is missing at row 0"),
            "unexpected error: {error}"
        );
    }
}
