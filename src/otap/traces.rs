use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use arrow_array::{Array, ArrayRef, RecordBatch, StructArray, UInt16Array, UInt32Array};
use arrow_schema::{Field, Schema};

use crate::{
    batch::transform_traces_view,
    views::pdata::{
        EventView, InstrumentationScopeView, LinkView, ResourceSpansView, ResourceView,
        ScopeSpansView, SpanId, SpanView, StatusView, Str, TraceId, TracesView,
    },
    Error, Result,
};

use super::logs::{
    bool_at, bytes_at, decode_attr_parent_ids, decode_root_ids, duration_nanos_at, f64_at, i32_at,
    i64_at, is_plain, nested_string, nested_u16, nested_u32_value, parse_nested_columns,
    same_attribute_value, string_at, table_value, timestamp, u16_at, u32_at, u8_at, validate_attrs,
    value_u16, AttributeTable, OtapAttribute, OwnedValue,
};
use super::schema::{validate, PayloadSchema};

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
        }
    }
    require_columns(
        "spans",
        &spans,
        &[
            "start_time_unix_nano",
            "duration_time_unix_nano",
            "trace_id",
            "span_id",
            "name",
        ],
    )?;
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

struct ResourceGroup {
    id: Option<u16>,
    representative: usize,
    scopes: Vec<ScopeGroup>,
}

struct ScopeGroup {
    id: Option<u16>,
    representative: usize,
    rows: Vec<usize>,
}

pub(super) struct AttributeTable32 {
    batch: RecordBatch,
    by_parent: BTreeMap<u32, Vec<usize>>,
    nested: Vec<Option<OwnedValue>>,
}

impl AttributeTable32 {
    pub(super) fn new(batch: RecordBatch) -> Result<Self> {
        require_columns("32-bit attributes", &batch, &["parent_id", "key", "type"])?;
        let parent = batch
            .column_by_name("parent_id")
            .ok_or_else(|| Error::Otap("attribute parent_id is missing".into()))?;
        let mut by_parent = BTreeMap::<u32, Vec<usize>>::new();
        for row in 0..batch.num_rows() {
            let id = u32_at(parent, row)
                .ok_or_else(|| Error::Otap("attribute parent_id contains null".into()))?;
            by_parent.entry(id).or_default().push(row);
        }
        let nested = parse_nested_columns(&batch)?;
        Ok(Self {
            batch,
            by_parent,
            nested,
        })
    }

    fn rows(&self, parent: Option<u32>) -> &[usize] {
        parent
            .and_then(|id| self.by_parent.get(&id))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
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
        let events_by_parent = index_u16(events.as_ref(), "parent_id")?;
        let links_by_parent = index_u16(links.as_ref(), "parent_id")?;
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

fn build_groups(batch: &RecordBatch) -> Result<Vec<ResourceGroup>> {
    let resource_ids = nested_u16(batch, "resource", "id")?;
    let scope_ids = nested_u16(batch, "scope", "id")?;
    let mut resources = Vec::<ResourceGroup>::new();
    let mut positions = HashMap::<Option<u16>, usize>::new();
    for row in 0..batch.num_rows() {
        let resource_id = value_u16(resource_ids, row);
        let position = *positions.entry(resource_id).or_insert_with(|| {
            let position = resources.len();
            resources.push(ResourceGroup {
                id: resource_id,
                representative: row,
                scopes: Vec::new(),
            });
            position
        });
        let scope_id = value_u16(scope_ids, row);
        let resource = &mut resources[position];
        if let Some(scope) = resource
            .scopes
            .iter_mut()
            .find(|scope| scope.id == scope_id)
        {
            scope.rows.push(row);
        } else {
            resource.scopes.push(ScopeGroup {
                id: scope_id,
                representative: row,
                rows: vec![row],
            });
        }
    }
    Ok(resources)
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

pub(super) struct Attr16Iter<'a> {
    table: Option<&'a AttributeTable>,
    rows: std::slice::Iter<'a, usize>,
}

impl<'a> Attr16Iter<'a> {
    pub(super) fn new(table: Option<&'a AttributeTable>, parent: Option<u16>) -> Self {
        Self {
            table,
            rows: table
                .map(|table| table.rows(parent))
                .unwrap_or_default()
                .iter(),
        }
    }
}

impl<'a> Iterator for Attr16Iter<'a> {
    type Item = OtapAttribute<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        let row = *self.rows.next()?;
        let table = self.table?;
        Some(OtapAttribute {
            key: table
                .batch
                .column_by_name("key")
                .and_then(|array| string_at(array, row))?,
            value: table_value(table, row),
        })
    }
}

pub(super) struct Attr32Iter<'a> {
    table: Option<&'a AttributeTable32>,
    rows: std::slice::Iter<'a, usize>,
}

impl<'a> Attr32Iter<'a> {
    pub(super) fn new(table: Option<&'a AttributeTable32>, parent: Option<u32>) -> Self {
        Self {
            table,
            rows: table
                .map(|table| table.rows(parent))
                .unwrap_or_default()
                .iter(),
        }
    }
}

impl<'a> Iterator for Attr32Iter<'a> {
    type Item = OtapAttribute<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        let row = *self.rows.next()?;
        let table = self.table?;
        Some(OtapAttribute {
            key: table
                .batch
                .column_by_name("key")
                .and_then(|array| string_at(array, row))?,
            value: super::logs::row_value(
                &table.batch,
                row,
                table.nested.get(row).and_then(Option::as_ref),
            ),
        })
    }
}

fn require_columns(name: &str, batch: &RecordBatch, columns: &[&str]) -> Result<()> {
    for column in columns {
        if batch.column_by_name(column).is_none() {
            return Err(Error::Otap(format!("{name} payload is missing {column}")));
        }
    }
    Ok(())
}

fn index_u16(batch: Option<&RecordBatch>, column: &str) -> Result<BTreeMap<u16, Vec<usize>>> {
    let mut result = BTreeMap::<u16, Vec<usize>>::new();
    let Some(batch) = batch else {
        return Ok(result);
    };
    let values = batch
        .column_by_name(column)
        .ok_or_else(|| Error::Otap(format!("{column} is missing")))?;
    for row in 0..batch.num_rows() {
        let value = u16_at(values, row)
            .ok_or_else(|| Error::Otap(format!("{column} contains null at row {row}")))?;
        result.entry(value).or_default().push(row);
    }
    Ok(result)
}

fn column_string<'a>(batch: &'a RecordBatch, name: &str, row: usize) -> Option<&'a [u8]> {
    batch
        .column_by_name(name)
        .and_then(|array| string_at(array, row))
}

fn column_u16(batch: &RecordBatch, name: &str, row: usize) -> Option<u16> {
    batch
        .column_by_name(name)
        .and_then(|array| u16_at(array, row))
}

fn column_u32(batch: &RecordBatch, name: &str, row: usize) -> Option<u32> {
    batch
        .column_by_name(name)
        .and_then(|array| u32_at(array, row))
}

fn id<'a, const N: usize>(batch: &'a RecordBatch, name: &str, row: usize) -> Option<&'a [u8; N]> {
    batch
        .column_by_name(name)
        .and_then(|array| bytes_at(array, row))
        .filter(|value| value.iter().any(|byte| *byte != 0))
        .and_then(|value| value.try_into().ok())
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
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| Error::Otap(format!("{name} is missing")))?;
    if is_plain(batch.schema().field(index)) {
        return Ok(batch);
    }
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .ok_or_else(|| Error::Otap(format!("{name} must be UInt16")))?;
    let mut decoded = Vec::<u16>::with_capacity(array.len());
    for row in 0..array.len() {
        if array.is_null(row) {
            return Err(Error::Otap(format!("{name} contains null at row {row}")));
        }
        let raw = array.value(row);
        let same = row > 0
            && equal_columns
                .iter()
                .all(|column| values_equal(&batch, column, row - 1, row));
        decoded.push(if same {
            decoded[row - 1]
                .checked_add(raw)
                .ok_or_else(|| Error::Otap(format!("{name} quasi-delta overflows at row {row}")))?
        } else {
            raw
        });
    }
    replace(&batch, index, Arc::new(UInt16Array::from(decoded)))
}

pub(super) fn decode_attr_parent_ids_u32(batch: RecordBatch) -> Result<RecordBatch> {
    let index = batch
        .schema()
        .index_of("parent_id")
        .map_err(|_| Error::Otap("attribute parent_id is missing".into()))?;
    if is_plain(batch.schema().field(index)) {
        return Ok(batch);
    }
    let array = batch.column(index);
    let mut decoded = Vec::<u32>::with_capacity(array.len());
    for row in 0..array.len() {
        let raw = u32_at(array, row)
            .ok_or_else(|| Error::Otap(format!("attribute parent_id is null at row {row}")))?;
        let same = row > 0 && same_attribute_value(&batch, row - 1, row);
        decoded.push(if same {
            decoded[row - 1]
                .checked_add(raw)
                .ok_or_else(|| Error::Otap(format!("attribute parent_id overflows at row {row}")))?
        } else {
            raw
        });
    }
    replace(&batch, index, Arc::new(UInt32Array::from(decoded)))
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

pub(super) fn replace(batch: &RecordBatch, index: usize, array: ArrayRef) -> Result<RecordBatch> {
    let mut columns = batch.columns().to_vec();
    columns[index] = array.clone();
    let old_schema = batch.schema();
    let mut fields = old_schema.fields().iter().cloned().collect::<Vec<_>>();
    let old = old_schema.field(index);
    if old.data_type() != array.data_type() {
        let mut metadata = old.metadata().clone();
        let _ = metadata.insert("encoding".into(), "plain".into());
        fields[index] = Arc::new(
            Field::new(old.name(), array.data_type().clone(), old.is_nullable())
                .with_metadata(metadata),
        );
    }
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        old_schema.metadata().clone(),
    ));
    RecordBatch::try_new(schema, columns).map_err(Into::into)
}

#[cfg(test)]
mod transport_tests {
    use arrow_array::{BinaryArray, StringArray, UInt8Array};
    use arrow_schema::{DataType, Field};

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
}
