use std::{
    collections::{BTreeMap, HashMap},
    io::Cursor,
    sync::Arc,
};

use arrow_array::{
    types::{Int16Type, Int8Type, UInt16Type, UInt8Type},
    Array, ArrayRef, BinaryArray, BooleanArray, DictionaryArray, DurationNanosecondArray,
    FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    StructArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt8Array,
};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use ciborium::Value;

use super::wire::{
    ATTR_BOOL, ATTR_BYTES, ATTR_DOUBLE, ATTR_INT, ATTR_KEY, ATTR_SER, ATTR_STR, ATTR_TYPE,
    VALUE_BOOL, VALUE_BYTES, VALUE_EMPTY, VALUE_F64, VALUE_I64, VALUE_MAP, VALUE_SLICE, VALUE_STR,
};
use crate::{
    batch::transform_logs_view,
    views::pdata::{
        AnyValueView, AttributeView, InstrumentationScopeView, LogRecordView, LogsDataView,
        ResourceLogsView, ResourceView, ScopeLogsView, SpanId, Str, TraceId, ValueType,
    },
    Error, Result,
};

const ENCODING: &str = "encoding";
const PLAIN: &str = "plain";

pub(super) fn normalize(
    logs: RecordBatch,
    resource_attrs: Option<RecordBatch>,
    scope_attrs: Option<RecordBatch>,
    log_attrs: Option<RecordBatch>,
    input_bytes: usize,
) -> Result<RecordBatch> {
    validate_logs(&logs)?;
    for (name, batch) in [
        ("resource attributes", resource_attrs.as_ref()),
        ("scope attributes", scope_attrs.as_ref()),
        ("log attributes", log_attrs.as_ref()),
    ] {
        if let Some(batch) = batch {
            validate_attrs(name, batch)?;
        }
    }

    let logs = decode_root_ids(logs)?;
    let resource_attrs = resource_attrs.map(decode_attr_parent_ids).transpose()?;
    let scope_attrs = scope_attrs.map(decode_attr_parent_ids).transpose()?;
    let log_attrs = log_attrs.map(decode_attr_parent_ids).transpose()?;
    let view = OtapLogsView::new(logs, resource_attrs, scope_attrs, log_attrs)?;
    transform_logs_view(&view, input_bytes)
}

struct OtapLogsView {
    logs: RecordBatch,
    resource_attrs: Option<AttributeTable>,
    scope_attrs: Option<AttributeTable>,
    log_attrs: Option<AttributeTable>,
    resources: Vec<ResourceGroup>,
    body_nested: Vec<Option<OwnedValue>>,
}

/// Side table of attributes keyed by parent row id. OTAP stores attributes in
/// a separate record joined back to the owning rows by `parent_id`; the key
/// width is `u16` for resource/scope/record attributes and `u32` for the
/// higher-cardinality data-point and exemplar attributes.
pub(super) struct AttributeTable<K: OtapInt + Ord = u16> {
    batch: RecordBatch,
    by_parent: BTreeMap<K, Vec<usize>>,
    nested: Vec<Option<OwnedValue>>,
}

/// 32-bit-keyed attribute side table (data-point / exemplar attributes).
pub(super) type AttributeTable32 = AttributeTable<u32>;

pub(super) struct ResourceGroup {
    pub(super) id: Option<u16>,
    pub(super) representative: usize,
    pub(super) scopes: Vec<ScopeGroup>,
}

pub(super) struct ScopeGroup {
    pub(super) id: Option<u16>,
    pub(super) representative: usize,
    pub(super) rows: Vec<usize>,
}

impl OtapLogsView {
    fn new(
        logs: RecordBatch,
        resource_attrs: Option<RecordBatch>,
        scope_attrs: Option<RecordBatch>,
        log_attrs: Option<RecordBatch>,
    ) -> Result<Self> {
        let resources = build_groups(&logs)?;
        let body_nested = parse_nested_struct(&logs, "body")?;
        Ok(Self {
            logs,
            resource_attrs: resource_attrs.map(AttributeTable::new).transpose()?,
            scope_attrs: scope_attrs.map(AttributeTable::new).transpose()?,
            log_attrs: log_attrs.map(AttributeTable::new).transpose()?,
            resources,
            body_nested,
        })
    }
}

impl<K: OtapInt + Ord> AttributeTable<K> {
    pub(super) fn new(batch: RecordBatch) -> Result<Self> {
        let parent = batch
            .column_by_name("parent_id")
            .ok_or_else(|| Error::Otap("attribute parent_id is missing".into()))?;
        let mut by_parent = BTreeMap::<K, Vec<usize>>::new();
        for row in 0..batch.num_rows() {
            let id = K::at(parent, row)
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

    fn rows(&self, parent: Option<K>) -> &[usize] {
        parent
            .and_then(|id| self.by_parent.get(&id))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    fn value(&self, row: usize) -> OtapValue<'_> {
        row_value(
            &self.batch,
            row,
            self.nested.get(row).and_then(Option::as_ref),
        )
    }
}

/// Iterates the attributes attached to one parent row, yielding `OtapAttribute`
/// for the [`AttributeView`] interface. Rows with no readable `key` are skipped.
/// One implementation serves every signal; only the key width `K` varies.
pub(super) struct AttrIter<'a, K: OtapInt + Ord = u16> {
    table: Option<&'a AttributeTable<K>>,
    rows: std::slice::Iter<'a, usize>,
}

/// 16-bit-keyed attribute iterator (resource / scope / record attributes).
pub(super) type Attr16Iter<'a> = AttrIter<'a, u16>;
/// 32-bit-keyed attribute iterator (data-point / exemplar attributes).
pub(super) type Attr32Iter<'a> = AttrIter<'a, u32>;

impl<'a, K: OtapInt + Ord> AttrIter<'a, K> {
    pub(super) fn new(table: Option<&'a AttributeTable<K>>, parent: Option<K>) -> Self {
        let rows = table
            .map(|table| table.rows(parent))
            .unwrap_or_default()
            .iter();
        Self { table, rows }
    }
}

impl<'a, K: OtapInt + Ord> Iterator for AttrIter<'a, K> {
    type Item = OtapAttribute<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let row = *self.rows.next()?;
            let table = self.table?;
            let key = table
                .batch
                .column_by_name(ATTR_KEY)
                .and_then(|array| string_at(array, row));
            if let Some(key) = key {
                return Some(OtapAttribute {
                    key,
                    value: table.value(row),
                });
            }
        }
    }
}

pub(super) fn build_groups(batch: &RecordBatch) -> Result<Vec<ResourceGroup>> {
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

impl LogsDataView for OtapLogsView {
    type ResourceLogs<'a>
        = OtapResourceLogs<'a>
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
    view: &'a OtapLogsView,
    groups: std::slice::Iter<'a, ResourceGroup>,
}

impl<'a> Iterator for ResourceIter<'a> {
    type Item = OtapResourceLogs<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(OtapResourceLogs {
            view: self.view,
            group: self.groups.next()?,
        })
    }
}

struct OtapResourceLogs<'a> {
    view: &'a OtapLogsView,
    group: &'a ResourceGroup,
}

impl ResourceLogsView for OtapResourceLogs<'_> {
    type Resource<'a>
        = OtapResource<'a>
    where
        Self: 'a;
    type ScopeLogs<'a>
        = OtapScopeLogs<'a>
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
            &self.view.logs,
            "resource",
            "schema_url",
            self.group.representative,
        )
    }
}

struct ScopeIter<'a> {
    view: &'a OtapLogsView,
    groups: std::slice::Iter<'a, ScopeGroup>,
}

impl<'a> Iterator for ScopeIter<'a> {
    type Item = OtapScopeLogs<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(OtapScopeLogs {
            view: self.view,
            group: self.groups.next()?,
        })
    }
}

struct OtapScopeLogs<'a> {
    view: &'a OtapLogsView,
    group: &'a ScopeGroup,
}

impl ScopeLogsView for OtapScopeLogs<'_> {
    type Scope<'a>
        = OtapScope<'a>
    where
        Self: 'a;
    type LogRecord<'a>
        = OtapLogRecord<'a>
    where
        Self: 'a;
    type LogRecordsIter<'a>
        = LogIter<'a>
    where
        Self: 'a;

    fn scope(&self) -> Option<Self::Scope<'_>> {
        Some(OtapScope {
            view: self.view,
            id: self.group.id,
            row: self.group.representative,
        })
    }

    fn log_records(&self) -> Self::LogRecordsIter<'_> {
        LogIter {
            view: self.view,
            rows: self.group.rows.iter(),
        }
    }

    fn schema_url(&self) -> Option<Str<'_>> {
        nested_string(
            &self.view.logs,
            "scope",
            "schema_url",
            self.group.representative,
        )
    }
}

struct LogIter<'a> {
    view: &'a OtapLogsView,
    rows: std::slice::Iter<'a, usize>,
}

impl<'a> Iterator for LogIter<'a> {
    type Item = OtapLogRecord<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(OtapLogRecord {
            view: self.view,
            row: *self.rows.next()?,
        })
    }
}

struct OtapResource<'a> {
    view: &'a OtapLogsView,
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
            &self.view.logs,
            "resource",
            "dropped_attributes_count",
            self.row,
        )
        .unwrap_or(0)
    }
}

struct OtapScope<'a> {
    view: &'a OtapLogsView,
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
        nested_string(&self.view.logs, "scope", "name", self.row).filter(|value| !value.is_empty())
    }

    fn version(&self) -> Option<Str<'_>> {
        nested_string(&self.view.logs, "scope", "version", self.row)
            .filter(|value| !value.is_empty())
    }

    fn attributes(&self) -> Self::AttributeIter<'_> {
        Attr16Iter::new(self.view.scope_attrs.as_ref(), self.id)
    }

    fn dropped_attributes_count(&self) -> u32 {
        nested_u32_value(
            &self.view.logs,
            "scope",
            "dropped_attributes_count",
            self.row,
        )
        .unwrap_or(0)
    }
}

struct OtapLogRecord<'a> {
    view: &'a OtapLogsView,
    row: usize,
}

impl LogRecordView for OtapLogRecord<'_> {
    type Attribute<'a>
        = OtapAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = Attr16Iter<'a>
    where
        Self: 'a;
    type Body<'a>
        = OtapValue<'a>
    where
        Self: 'a;

    fn time_unix_nano(&self) -> Option<u64> {
        timestamp(&self.view.logs, "time_unix_nano", self.row)
    }

    fn observed_time_unix_nano(&self) -> Option<u64> {
        timestamp(&self.view.logs, "observed_time_unix_nano", self.row)
    }

    fn severity_number(&self) -> Option<i32> {
        self.view
            .logs
            .column_by_name("severity_number")
            .and_then(|array| i32_at(array, self.row))
            .filter(|value| *value != 0)
    }

    fn severity_text(&self) -> Option<Str<'_>> {
        self.view
            .logs
            .column_by_name("severity_text")
            .and_then(|array| string_at(array, self.row))
            .filter(|value| !value.is_empty())
    }

    fn body(&self) -> Option<Self::Body<'_>> {
        struct_value(
            &self.view.logs,
            "body",
            self.row,
            self.view.body_nested.get(self.row).and_then(Option::as_ref),
        )
    }

    fn attributes(&self) -> Self::AttributeIter<'_> {
        let id = self
            .view
            .logs
            .column_by_name("id")
            .and_then(|array| u16_at(array, self.row));
        Attr16Iter::new(self.view.log_attrs.as_ref(), id)
    }

    fn dropped_attributes_count(&self) -> u32 {
        self.view
            .logs
            .column_by_name("dropped_attributes_count")
            .and_then(|array| u32_at(array, self.row))
            .unwrap_or(0)
    }

    fn flags(&self) -> Option<u32> {
        self.view
            .logs
            .column_by_name("flags")
            .and_then(|array| u32_at(array, self.row))
            .filter(|value| *value != 0)
    }

    fn trace_id(&self) -> Option<&TraceId> {
        self.view
            .logs
            .column_by_name("trace_id")
            .and_then(|array| bytes_at(array, self.row))
            .filter(|value| value.iter().any(|byte| *byte != 0))
            .and_then(|value| value.try_into().ok())
    }

    fn span_id(&self) -> Option<&SpanId> {
        self.view
            .logs
            .column_by_name("span_id")
            .and_then(|array| bytes_at(array, self.row))
            .filter(|value| value.iter().any(|byte| *byte != 0))
            .and_then(|value| value.try_into().ok())
    }

    fn event_name(&self) -> Option<Str<'_>> {
        self.view
            .logs
            .column_by_name("event_name")
            .and_then(|array| string_at(array, self.row))
            .filter(|value| !value.is_empty())
    }
}

#[derive(Clone, Copy)]
pub(super) struct OtapAttribute<'a> {
    pub(super) key: &'a [u8],
    pub(super) value: OtapValue<'a>,
}

impl AttributeView for OtapAttribute<'_> {
    type Val<'a>
        = OtapValue<'a>
    where
        Self: 'a;

    fn key(&self) -> Str<'_> {
        self.key
    }

    fn value(&self) -> Option<Self::Val<'_>> {
        Some(self.value)
    }
}

#[derive(Clone, Copy)]
pub(super) enum OtapValue<'a> {
    Empty,
    String(&'a [u8]),
    Bool(bool),
    Int(i64),
    Double(f64),
    Bytes(&'a [u8]),
    Owned(&'a OwnedValue),
}

impl<'a> AnyValueView<'a> for OtapValue<'a> {
    type KeyValue = OtapAttribute<'a>;
    type ArrayIter<'b>
        = OwnedArrayIter<'a>
    where
        Self: 'b;
    type KeyValueIter<'b>
        = OwnedMapIter<'a>
    where
        Self: 'b;

    fn value_type(&self) -> ValueType {
        match self {
            Self::Empty | Self::Owned(OwnedValue::Empty) => ValueType::Empty,
            Self::String(_) | Self::Owned(OwnedValue::String(_)) => ValueType::String,
            Self::Bool(_) | Self::Owned(OwnedValue::Bool(_)) => ValueType::Bool,
            Self::Int(_) | Self::Owned(OwnedValue::Int(_)) => ValueType::Int64,
            Self::Double(_) | Self::Owned(OwnedValue::Double(_)) => ValueType::Double,
            Self::Bytes(_) | Self::Owned(OwnedValue::Bytes(_)) => ValueType::Bytes,
            Self::Owned(OwnedValue::Array(_)) => ValueType::Array,
            Self::Owned(OwnedValue::Map(_)) => ValueType::KeyValueList,
        }
    }

    fn as_string(&self) -> Option<Str<'_>> {
        match self {
            Self::String(value) => Some(value),
            Self::Owned(OwnedValue::String(value)) => Some(value),
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            Self::Owned(OwnedValue::Bool(value)) => Some(*value),
            _ => None,
        }
    }

    fn as_int64(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(*value),
            Self::Owned(OwnedValue::Int(value)) => Some(*value),
            _ => None,
        }
    }

    fn as_double(&self) -> Option<f64> {
        match self {
            Self::Double(value) => Some(*value),
            Self::Owned(OwnedValue::Double(value)) => Some(*value),
            _ => None,
        }
    }

    fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(value) => Some(value),
            Self::Owned(OwnedValue::Bytes(value)) => Some(value),
            _ => None,
        }
    }

    fn as_array(&self) -> Option<Self::ArrayIter<'_>> {
        match self {
            Self::Owned(OwnedValue::Array(values)) => Some(OwnedArrayIter(values.iter())),
            _ => None,
        }
    }

    fn as_kvlist(&self) -> Option<Self::KeyValueIter<'_>> {
        match self {
            Self::Owned(OwnedValue::Map(values)) => Some(OwnedMapIter(values.iter())),
            _ => None,
        }
    }
}

pub(super) struct OwnedArrayIter<'a>(std::slice::Iter<'a, OwnedValue>);

impl<'a> Iterator for OwnedArrayIter<'a> {
    type Item = OtapValue<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(OtapValue::Owned(self.0.next()?))
    }
}

pub(super) struct OwnedMapIter<'a>(std::slice::Iter<'a, (Vec<u8>, OwnedValue)>);

impl<'a> Iterator for OwnedMapIter<'a> {
    type Item = OtapAttribute<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.0.next()?;
        Some(OtapAttribute {
            key,
            value: OtapValue::Owned(value),
        })
    }
}

pub(super) enum OwnedValue {
    Empty,
    String(Vec<u8>),
    Bool(bool),
    Int(i64),
    Double(f64),
    Bytes(Vec<u8>),
    Array(Vec<OwnedValue>),
    Map(Vec<(Vec<u8>, OwnedValue)>),
}

fn struct_value<'a>(
    batch: &'a RecordBatch,
    name: &str,
    row: usize,
    nested: Option<&'a OwnedValue>,
) -> Option<OtapValue<'a>> {
    let value = batch.column_by_name(name)?;
    let values = value.as_any().downcast_ref::<StructArray>()?;
    if values.is_null(row) {
        return None;
    }
    Some(row_value_struct(values, row, nested))
}

pub(super) fn row_value<'a>(
    batch: &'a RecordBatch,
    row: usize,
    nested: Option<&'a OwnedValue>,
) -> OtapValue<'a> {
    let value_type = batch
        .column_by_name(ATTR_TYPE)
        .and_then(|array| u8_at(array, row));
    row_value_columns(|name| batch.column_by_name(name), value_type, row, nested)
}

fn row_value_struct<'a>(
    values: &'a StructArray,
    row: usize,
    nested: Option<&'a OwnedValue>,
) -> OtapValue<'a> {
    let value_type = values
        .column_by_name(ATTR_TYPE)
        .and_then(|array| u8_at(array, row));
    row_value_columns(|name| values.column_by_name(name), value_type, row, nested)
}

fn row_value_columns<'a>(
    column: impl Fn(&str) -> Option<&'a ArrayRef>,
    value_type: Option<u8>,
    row: usize,
    nested: Option<&'a OwnedValue>,
) -> OtapValue<'a> {
    match value_type {
        None | Some(VALUE_EMPTY) => OtapValue::Empty,
        Some(VALUE_STR) => column(ATTR_STR)
            .and_then(|array| string_at(array, row))
            .map(OtapValue::String)
            .unwrap_or(OtapValue::Empty),
        Some(VALUE_I64) => column(ATTR_INT)
            .and_then(|array| i64_at(array, row))
            .map(OtapValue::Int)
            .unwrap_or(OtapValue::Empty),
        Some(VALUE_F64) => column(ATTR_DOUBLE)
            .and_then(|array| f64_at(array, row))
            .map(OtapValue::Double)
            .unwrap_or(OtapValue::Empty),
        Some(VALUE_BOOL) => column(ATTR_BOOL)
            .and_then(|array| bool_at(array, row))
            .map(OtapValue::Bool)
            .unwrap_or(OtapValue::Empty),
        Some(VALUE_MAP | VALUE_SLICE) => nested.map(OtapValue::Owned).unwrap_or(OtapValue::Empty),
        Some(VALUE_BYTES) => column(ATTR_BYTES)
            .and_then(|array| bytes_at(array, row))
            .map(OtapValue::Bytes)
            .unwrap_or(OtapValue::Empty),
        Some(_) => OtapValue::Empty,
    }
}

fn parse_nested_struct(batch: &RecordBatch, name: &str) -> Result<Vec<Option<OwnedValue>>> {
    let Some(values) = batch
        .column_by_name(name)
        .and_then(|value| value.as_any().downcast_ref::<StructArray>())
    else {
        return Ok((0..batch.num_rows()).map(|_| None).collect());
    };
    parse_nested(
        values
            .column_by_name(ATTR_TYPE)
            .ok_or_else(|| Error::Otap(format!("{name}.type is missing")))?,
        values.column_by_name(ATTR_SER),
        batch.num_rows(),
        name,
    )
}

pub(super) fn parse_nested_columns(batch: &RecordBatch) -> Result<Vec<Option<OwnedValue>>> {
    parse_nested(
        batch
            .column_by_name(ATTR_TYPE)
            .ok_or_else(|| Error::Otap("attribute type is missing".into()))?,
        batch.column_by_name(ATTR_SER),
        batch.num_rows(),
        "attribute",
    )
}

fn parse_nested(
    types: &ArrayRef,
    serialized: Option<&ArrayRef>,
    rows: usize,
    context: &str,
) -> Result<Vec<Option<OwnedValue>>> {
    let mut result = Vec::with_capacity(rows);
    for row in 0..rows {
        if matches!(u8_at(types, row), Some(VALUE_MAP | VALUE_SLICE)) {
            let bytes = serialized
                .and_then(|array| bytes_at(array, row))
                .ok_or_else(|| Error::Otap(format!("{context} row {row} has no CBOR value")))?;
            let value: Value = ciborium::from_reader(Cursor::new(bytes)).map_err(|error| {
                Error::Otap(format!("invalid {context} CBOR at row {row}: {error}"))
            })?;
            result.push(Some(owned_value(value)?));
        } else {
            result.push(None);
        }
    }
    Ok(result)
}

fn owned_value(value: Value) -> Result<OwnedValue> {
    Ok(match value {
        Value::Null => OwnedValue::Empty,
        Value::Bool(value) => OwnedValue::Bool(value),
        Value::Integer(value) => OwnedValue::Int(value.try_into().map_err(|_| {
            Error::Otap("CBOR integer is outside the OTLP signed 64-bit range".into())
        })?),
        Value::Float(value) => OwnedValue::Double(value),
        Value::Bytes(value) => OwnedValue::Bytes(value),
        Value::Text(value) => OwnedValue::String(value.into_bytes()),
        Value::Array(values) => OwnedValue::Array(
            values
                .into_iter()
                .map(owned_value)
                .collect::<Result<Vec<_>>>()?,
        ),
        Value::Map(values) => OwnedValue::Map(
            values
                .into_iter()
                .map(|(key, value)| {
                    let Value::Text(key) = key else {
                        return Err(Error::Otap("CBOR map key is not text".into()));
                    };
                    Ok((key.into_bytes(), owned_value(value)?))
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        Value::Tag(_, value) => owned_value(*value)?,
        _ => return Err(Error::Otap("unsupported CBOR value".into())),
    })
}

pub(super) fn decode_root_ids(mut batch: RecordBatch) -> Result<RecordBatch> {
    batch = decode_column_delta(batch, "id")?;
    batch = decode_nested_delta(batch, "resource", "id")?;
    decode_nested_delta(batch, "scope", "id")
}

pub(super) fn decode_column_delta(batch: RecordBatch, name: &str) -> Result<RecordBatch> {
    let Some(index) = batch.schema().index_of(name).ok() else {
        return Ok(batch);
    };
    if is_plain(batch.schema().field(index)) {
        return Ok(batch);
    }
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .ok_or_else(|| Error::Otap(format!("{name} must be UInt16")))?;
    let decoded: ArrayRef = Arc::new(decode_delta(array, name)?);
    let mut columns = batch.columns().to_vec();
    columns[index] = decoded;
    RecordBatch::try_new(batch.schema(), columns).map_err(Into::into)
}

fn decode_nested_delta(batch: RecordBatch, outer: &str, inner: &str) -> Result<RecordBatch> {
    let Some(index) = batch.schema().index_of(outer).ok() else {
        return Ok(batch);
    };
    let values = batch
        .column(index)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::Otap(format!("{outer} must be a struct")))?;
    let Some(child_index) = values
        .fields()
        .iter()
        .position(|field| field.name() == inner)
    else {
        return Ok(batch);
    };
    if is_plain(&values.fields()[child_index]) {
        return Ok(batch);
    }
    let ids = values
        .column(child_index)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .ok_or_else(|| Error::Otap(format!("{outer}.{inner} must be UInt16")))?;
    let mut children = values.columns().to_vec();
    children[child_index] = Arc::new(decode_delta(ids, &format!("{outer}.{inner}"))?);
    let decoded = StructArray::new(values.fields().clone(), children, values.nulls().cloned());
    let mut columns = batch.columns().to_vec();
    columns[index] = Arc::new(decoded);
    RecordBatch::try_new(batch.schema(), columns).map_err(Into::into)
}

pub(super) fn decode_delta(array: &UInt16Array, name: &str) -> Result<UInt16Array> {
    let mut accumulator = 0u16;
    let mut values = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        if array.is_null(row) {
            values.push(None);
        } else {
            accumulator = accumulator.checked_add(array.value(row)).ok_or_else(|| {
                Error::Otap(format!("{name} delta overflows UInt16 at row {row}"))
            })?;
            values.push(Some(accumulator));
        }
    }
    Ok(UInt16Array::from(values))
}

/// Unsigned integer widths used by OTAP id / parent-id columns. Backs both
/// delta decoding (`checked_add`, `into_array`) and attribute-table keying
/// (`at` + the `Ord` bound at use sites).
pub(super) trait OtapInt: Copy {
    fn at(array: &ArrayRef, row: usize) -> Option<Self>;
    fn checked_add(self, rhs: Self) -> Option<Self>;
    fn into_array(values: Vec<Self>) -> ArrayRef;
}

impl OtapInt for u16 {
    fn at(array: &ArrayRef, row: usize) -> Option<Self> {
        u16_at(array, row)
    }
    fn checked_add(self, rhs: Self) -> Option<Self> {
        u16::checked_add(self, rhs)
    }
    fn into_array(values: Vec<Self>) -> ArrayRef {
        Arc::new(UInt16Array::from(values))
    }
}

impl OtapInt for u32 {
    fn at(array: &ArrayRef, row: usize) -> Option<Self> {
        u32_at(array, row)
    }
    fn checked_add(self, rhs: Self) -> Option<Self> {
        u32::checked_add(self, rhs)
    }
    fn into_array(values: Vec<Self>) -> ArrayRef {
        Arc::new(UInt32Array::from(values))
    }
}

/// Decode a quasi-delta-encoded id column in place: a value is a delta from the
/// previous row when `same(prev, cur)` holds, otherwise an absolute value.
/// Columns already marked `encoding=plain` are returned unchanged. This is the
/// shared engine behind every per-signal parent-id decoder (logs, traces,
/// metrics) — only the integer width and the equality predicate vary.
pub(super) fn decode_quasi_delta<T: OtapInt>(
    batch: RecordBatch,
    name: &str,
    same: impl Fn(&RecordBatch, usize, usize) -> bool,
) -> Result<RecordBatch> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| Error::Otap(format!("{name} is missing")))?;
    if is_plain(batch.schema().field(index)) {
        return Ok(batch);
    }
    let array = batch.column(index);
    let mut decoded = Vec::<T>::with_capacity(array.len());
    for row in 0..array.len() {
        let raw =
            T::at(array, row).ok_or_else(|| Error::Otap(format!("{name} is null at row {row}")))?;
        let value = if row > 0 && same(&batch, row - 1, row) {
            decoded[row - 1]
                .checked_add(raw)
                .ok_or_else(|| Error::Otap(format!("{name} delta overflows at row {row}")))?
        } else {
            raw
        };
        decoded.push(value);
    }
    replace(&batch, index, T::into_array(decoded))
}

pub(super) fn decode_attr_parent_ids(batch: RecordBatch) -> Result<RecordBatch> {
    decode_quasi_delta::<u16>(batch, "parent_id", same_attribute_value)
}

/// Swap a decoded column into `batch` at `index`, marking the field
/// `encoding=plain` when decoding changed its Arrow data type.
pub(super) fn replace(batch: &RecordBatch, index: usize, array: ArrayRef) -> Result<RecordBatch> {
    let mut columns = batch.columns().to_vec();
    columns[index] = array.clone();
    let old_schema = batch.schema();
    let mut fields = old_schema.fields().iter().cloned().collect::<Vec<_>>();
    let old = old_schema.field(index);
    if old.data_type() != array.data_type() {
        let mut metadata = old.metadata().clone();
        let _ = metadata.insert(ENCODING.into(), PLAIN.into());
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

pub(super) fn same_attribute_value(batch: &RecordBatch, left: usize, right: usize) -> bool {
    let Some(types) = batch.column_by_name(ATTR_TYPE) else {
        return false;
    };
    let left_type = u8_at(types, left);
    if left_type != u8_at(types, right) {
        return false;
    }
    let Some(keys) = batch.column_by_name(ATTR_KEY) else {
        return false;
    };
    if string_at(keys, left) != string_at(keys, right) {
        return false;
    }
    match left_type {
        Some(VALUE_STR) => equal_at(batch.column_by_name(ATTR_STR), left, right, string_at),
        Some(VALUE_I64) => equal_at(batch.column_by_name(ATTR_INT), left, right, i64_at),
        Some(VALUE_F64) => batch.column_by_name(ATTR_DOUBLE).is_some_and(|array| {
            let left = f64_at(array, left).map(f64::to_bits);
            left.is_some() && left == f64_at(array, right).map(f64::to_bits)
        }),
        Some(VALUE_BOOL) => equal_at(batch.column_by_name(ATTR_BOOL), left, right, bool_at),
        Some(VALUE_BYTES) => equal_at(batch.column_by_name(ATTR_BYTES), left, right, bytes_at),
        _ => false,
    }
}

fn equal_at<'a, T: PartialEq>(
    array: Option<&'a ArrayRef>,
    left: usize,
    right: usize,
    value: impl Fn(&'a ArrayRef, usize) -> Option<T>,
) -> bool {
    array.is_some_and(|array| {
        let left = value(array, left);
        left.is_some() && left == value(array, right)
    })
}

pub(super) fn is_plain(field: &Field) -> bool {
    field
        .metadata()
        .get(ENCODING)
        .is_some_and(|value| value == PLAIN)
}

fn validate_logs(batch: &RecordBatch) -> Result<()> {
    for field in batch.schema().fields() {
        match field.name().as_str() {
            "time_unix_nano" | "observed_time_unix_nano" => {
                expect(field, &DataType::Timestamp(TimeUnit::Nanosecond, None))?
            }
            "body" => validate_any_struct(field)?,
            "id" => expect(field, &DataType::UInt16)?,
            "severity_number" => expect_dict_or(field, &DataType::Int32, &[8, 16])?,
            "severity_text" | "event_name" | "schema_url" => {
                expect_dict_or(field, &DataType::Utf8, &[8, 16])?
            }
            "dropped_attributes_count" | "flags" => expect(field, &DataType::UInt32)?,
            "trace_id" => expect_dict_or(field, &DataType::FixedSizeBinary(16), &[8, 16])?,
            "span_id" => expect_dict_or(field, &DataType::FixedSizeBinary(8), &[8, 16])?,
            "resource" => validate_context_struct(field, false)?,
            "scope" => validate_context_struct(field, true)?,
            name => return Err(Error::Otap(format!("unknown Logs column {name:?}"))),
        }
    }
    if let Some(body) = batch.column_by_name("body") {
        let body = body
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| Error::Otap("body must be a struct".into()))?;
        validate_any_rows(
            |name| body.column_by_name(name),
            body.len(),
            |row| body.is_valid(row),
            "body",
        )?;
    }
    Ok(())
}

fn validate_context_struct(field: &Field, scope: bool) -> Result<()> {
    let DataType::Struct(fields) = field.data_type() else {
        return Err(Error::Otap(format!("{} must be a struct", field.name())));
    };
    for child in fields {
        match child.name().as_str() {
            "id" => expect(child, &DataType::UInt16)?,
            "dropped_attributes_count" => expect(child, &DataType::UInt32)?,
            "schema_url" => expect_dict_or(child, &DataType::Utf8, &[8, 16])?,
            "name" | "version" if scope => expect_dict_or(child, &DataType::Utf8, &[8, 16])?,
            name => {
                return Err(Error::Otap(format!(
                    "unknown {} column {name:?}",
                    field.name()
                )))
            }
        }
    }
    Ok(())
}

fn validate_any_struct(field: &Field) -> Result<()> {
    let DataType::Struct(fields) = field.data_type() else {
        return Err(Error::Otap(format!("{} must be a struct", field.name())));
    };
    validate_any_fields(fields, field.name())
}

pub(super) fn validate_attrs(name: &str, batch: &RecordBatch) -> Result<()> {
    for required in ["parent_id", ATTR_KEY, ATTR_TYPE] {
        if batch.schema().field_with_name(required).is_err() {
            return Err(Error::Otap(format!(
                "{name} missing required {required} column"
            )));
        }
    }
    validate_any_fields(batch.schema().fields(), name)?;
    expect(
        batch.schema().field_with_name("parent_id").unwrap(),
        &DataType::UInt16,
    )?;
    expect_dict_or(
        batch.schema().field_with_name(ATTR_KEY).unwrap(),
        &DataType::Utf8,
        &[8, 16],
    )?;
    for required in ["parent_id", ATTR_KEY, ATTR_TYPE] {
        if batch.column_by_name(required).unwrap().null_count() != 0 {
            return Err(Error::Otap(format!(
                "{name} required column {required} contains nulls"
            )));
        }
    }
    validate_any_rows(
        |column| batch.column_by_name(column),
        batch.num_rows(),
        |_| true,
        name,
    )
}

fn validate_any_rows<'a>(
    column: impl Fn(&str) -> Option<&'a ArrayRef>,
    rows: usize,
    is_present: impl Fn(usize) -> bool,
    context: &str,
) -> Result<()> {
    let types = column(ATTR_TYPE)
        .ok_or_else(|| Error::Otap(format!("{context} missing required type column")))?;
    for row in 0..rows {
        if !is_present(row) {
            continue;
        }
        let value_type = u8_at(types, row)
            .ok_or_else(|| Error::Otap(format!("{context} type is null at row {row}")))?;
        let value_present = match value_type {
            VALUE_EMPTY => true,
            VALUE_STR => column(ATTR_STR)
                .and_then(|array| string_at(array, row))
                .is_some(),
            VALUE_I64 => column(ATTR_INT)
                .and_then(|array| i64_at(array, row))
                .is_some(),
            VALUE_F64 => column(ATTR_DOUBLE)
                .and_then(|array| f64_at(array, row))
                .is_some(),
            VALUE_BOOL => column(ATTR_BOOL)
                .and_then(|array| bool_at(array, row))
                .is_some(),
            VALUE_MAP | VALUE_SLICE => column(ATTR_SER)
                .and_then(|array| bytes_at(array, row))
                .is_some(),
            VALUE_BYTES => column(ATTR_BYTES)
                .and_then(|array| bytes_at(array, row))
                .is_some(),
            other => {
                return Err(Error::Otap(format!(
                    "{context} has unknown AnyValue type {other} at row {row}"
                )))
            }
        };
        if !value_present {
            return Err(Error::Otap(format!(
                "{context} value for type {value_type} is missing at row {row}"
            )));
        }
    }
    Ok(())
}

fn validate_any_fields(fields: &Fields, context: &str) -> Result<()> {
    for field in fields {
        match field.name().as_str() {
            "parent_id" | ATTR_KEY => {}
            ATTR_TYPE => expect(field, &DataType::UInt8)?,
            ATTR_STR => expect_dict_or(field, &DataType::Utf8, &[8, 16])?,
            ATTR_INT => expect_dict_or(field, &DataType::Int64, &[8, 16])?,
            ATTR_DOUBLE => expect(field, &DataType::Float64)?,
            ATTR_BOOL => expect(field, &DataType::Boolean)?,
            ATTR_BYTES | ATTR_SER => expect_dict_or(field, &DataType::Binary, &[8, 16])?,
            name => {
                return Err(Error::Otap(format!(
                    "unknown {context} value column {name:?}"
                )))
            }
        }
    }
    if fields.iter().all(|field| field.name() != ATTR_TYPE) {
        return Err(Error::Otap(format!(
            "{context} missing required type column"
        )));
    }
    Ok(())
}

fn expect(field: &Field, expected: &DataType) -> Result<()> {
    if field.data_type() == expected {
        Ok(())
    } else {
        Err(Error::Otap(format!(
            "{} has type {:?}, expected {expected:?}",
            field.name(),
            field.data_type()
        )))
    }
}

fn expect_dict_or(field: &Field, expected: &DataType, key_widths: &[u8]) -> Result<()> {
    if field.data_type() == expected {
        return Ok(());
    }
    let DataType::Dictionary(key, value) = field.data_type() else {
        return expect(field, expected);
    };
    let width = match key.as_ref() {
        DataType::Int8 | DataType::UInt8 => 8,
        DataType::Int16 | DataType::UInt16 => 16,
        _ => 0,
    };
    if value.as_ref() == expected && key_widths.contains(&width) {
        Ok(())
    } else {
        Err(Error::Otap(format!(
            "{} has unsupported dictionary type {:?}",
            field.name(),
            field.data_type()
        )))
    }
}

pub(super) fn column_string<'a>(
    batch: &'a RecordBatch,
    name: &str,
    row: usize,
) -> Option<&'a [u8]> {
    batch
        .column_by_name(name)
        .and_then(|array| string_at(array, row))
}

pub(super) fn column_u16(batch: &RecordBatch, name: &str, row: usize) -> Option<u16> {
    batch
        .column_by_name(name)
        .and_then(|array| u16_at(array, row))
}

pub(super) fn column_u32(batch: &RecordBatch, name: &str, row: usize) -> Option<u32> {
    batch
        .column_by_name(name)
        .and_then(|array| u32_at(array, row))
}

pub(super) fn id<'a, const N: usize>(
    batch: &'a RecordBatch,
    name: &str,
    row: usize,
) -> Option<&'a [u8; N]> {
    batch
        .column_by_name(name)
        .and_then(|array| bytes_at(array, row))
        .filter(|value| value.iter().any(|byte| *byte != 0))
        .and_then(|value| value.try_into().ok())
}

pub(super) fn index_u16(batch: &RecordBatch, name: &str) -> Result<BTreeMap<u16, Vec<usize>>> {
    index_by_parent(batch, name, u16_at)
}

pub(super) fn index_u32(batch: &RecordBatch, name: &str) -> Result<BTreeMap<u32, Vec<usize>>> {
    index_by_parent(batch, name, u32_at)
}

fn index_by_parent<K: Ord>(
    batch: &RecordBatch,
    name: &str,
    value: impl Fn(&ArrayRef, usize) -> Option<K>,
) -> Result<BTreeMap<K, Vec<usize>>> {
    let array = batch
        .column_by_name(name)
        .ok_or_else(|| Error::Otap(format!("{name} is missing")))?;
    let mut result = BTreeMap::<K, Vec<usize>>::new();
    for row in 0..batch.num_rows() {
        let key = value(array, row)
            .ok_or_else(|| Error::Otap(format!("{name} contains null at row {row}")))?;
        result.entry(key).or_default().push(row);
    }
    Ok(result)
}

pub(super) fn nested_u16<'a>(
    batch: &'a RecordBatch,
    outer: &str,
    inner: &str,
) -> Result<Option<&'a UInt16Array>> {
    let Some(values) = batch.column_by_name(outer) else {
        return Ok(None);
    };
    let values = values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::Otap(format!("{outer} must be a struct")))?;
    Ok(values
        .column_by_name(inner)
        .and_then(|array| array.as_any().downcast_ref()))
}

pub(super) fn value_u16(array: Option<&UInt16Array>, row: usize) -> Option<u16> {
    array.and_then(|array| array.is_valid(row).then(|| array.value(row)))
}

pub(super) fn nested_string<'a>(
    batch: &'a RecordBatch,
    outer: &str,
    inner: &str,
    row: usize,
) -> Option<&'a [u8]> {
    batch
        .column_by_name(outer)?
        .as_any()
        .downcast_ref::<StructArray>()?
        .column_by_name(inner)
        .and_then(|array| string_at(array, row))
}

pub(super) fn nested_u32_value(
    batch: &RecordBatch,
    outer: &str,
    inner: &str,
    row: usize,
) -> Option<u32> {
    batch
        .column_by_name(outer)?
        .as_any()
        .downcast_ref::<StructArray>()?
        .column_by_name(inner)
        .and_then(|array| u32_at(array, row))
}

pub(super) fn timestamp(batch: &RecordBatch, name: &str, row: usize) -> Option<u64> {
    let array = batch
        .column_by_name(name)?
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()?;
    array
        .is_valid(row)
        .then(|| array.value(row))
        .and_then(|value| u64::try_from(value).ok())
        .filter(|value| *value != 0)
}

macro_rules! dictionary_value {
    ($array:expr, $row:expr, $value_ty:ty, $method:ident) => {{
        let array = $array;
        if let Some(dict) = array.as_any().downcast_ref::<DictionaryArray<UInt8Type>>() {
            dict.is_valid($row)
                .then(|| dict.values().as_any().downcast_ref::<$value_ty>())
                .flatten()
                .map(|values| values.$method(dict.keys().value($row) as usize))
        } else if let Some(dict) = array.as_any().downcast_ref::<DictionaryArray<Int8Type>>() {
            dict.is_valid($row)
                .then(|| dict.values().as_any().downcast_ref::<$value_ty>())
                .flatten()
                .map(|values| values.$method(dict.keys().value($row) as usize))
        } else if let Some(dict) = array.as_any().downcast_ref::<DictionaryArray<UInt16Type>>() {
            dict.is_valid($row)
                .then(|| dict.values().as_any().downcast_ref::<$value_ty>())
                .flatten()
                .map(|values| values.$method(dict.keys().value($row) as usize))
        } else if let Some(dict) = array.as_any().downcast_ref::<DictionaryArray<Int16Type>>() {
            dict.is_valid($row)
                .then(|| dict.values().as_any().downcast_ref::<$value_ty>())
                .flatten()
                .map(|values| values.$method(dict.keys().value($row) as usize))
        } else {
            None
        }
    }};
}

pub(super) fn string_at(array: &ArrayRef, row: usize) -> Option<&[u8]> {
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        values.is_valid(row).then(|| values.value(row).as_bytes())
    } else {
        dictionary_value!(array, row, StringArray, value).map(str::as_bytes)
    }
}

pub(super) fn bytes_at(array: &ArrayRef, row: usize) -> Option<&[u8]> {
    if let Some(values) = array.as_any().downcast_ref::<BinaryArray>() {
        values.is_valid(row).then(|| values.value(row))
    } else if let Some(values) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        values.is_valid(row).then(|| values.value(row))
    } else if let Some(value) = dictionary_value!(array, row, BinaryArray, value) {
        Some(value)
    } else {
        dictionary_value!(array, row, FixedSizeBinaryArray, value)
    }
}

pub(super) fn i64_at(array: &ArrayRef, row: usize) -> Option<i64> {
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        values.is_valid(row).then(|| values.value(row))
    } else {
        dictionary_value!(array, row, Int64Array, value)
    }
}

pub(super) fn i32_at(array: &ArrayRef, row: usize) -> Option<i32> {
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        values.is_valid(row).then(|| values.value(row))
    } else {
        dictionary_value!(array, row, Int32Array, value)
    }
}

pub(super) fn u16_at(array: &ArrayRef, row: usize) -> Option<u16> {
    array
        .as_any()
        .downcast_ref::<UInt16Array>()
        .and_then(|values| values.is_valid(row).then(|| values.value(row)))
}

pub(super) fn u32_at(array: &ArrayRef, row: usize) -> Option<u32> {
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        values.is_valid(row).then(|| values.value(row))
    } else {
        dictionary_value!(array, row, UInt32Array, value)
    }
}

pub(super) fn u8_at(array: &ArrayRef, row: usize) -> Option<u8> {
    array
        .as_any()
        .downcast_ref::<UInt8Array>()
        .and_then(|values| values.is_valid(row).then(|| values.value(row)))
}

pub(super) fn f64_at(array: &ArrayRef, row: usize) -> Option<f64> {
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .and_then(|values| values.is_valid(row).then(|| values.value(row)))
}

pub(super) fn duration_nanos_at(array: &ArrayRef, row: usize) -> Option<i64> {
    if let Some(values) = array.as_any().downcast_ref::<DurationNanosecondArray>() {
        values.is_valid(row).then(|| values.value(row))
    } else {
        dictionary_value!(array, row, DurationNanosecondArray, value)
    }
}

pub(super) fn bool_at(array: &ArrayRef, row: usize) -> Option<bool> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .and_then(|values| values.is_valid(row).then(|| values.value(row)))
}
