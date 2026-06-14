//! Shared resource, scope, and metric context extracted from OTLP requests.

use std::{
    collections::hash_map::DefaultHasher,
    collections::HashSet,
    hash::{Hash, Hasher},
};

use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};

use crate::views::pdata::{
    AnyValueView, AttributeView, InstrumentationScopeView, ResourceView, ValueType,
};

use super::{
    profile::{
        measure_phase, observe_counter, TransformCounter, TransformObserver, TransformPhase,
        TransformSignal,
    },
    view_json,
};

#[derive(Default)]
pub(super) struct ContextDuplicateTracker {
    resource_contexts: HashSet<u64>,
    scope_contexts: HashSet<u64>,
}

impl ContextDuplicateTracker {
    pub(super) fn observe_resource_view<R: ResourceView>(
        &mut self,
        resource: &R,
        signal: TransformSignal,
        observer: &mut Option<&mut dyn TransformObserver>,
    ) {
        let mut hasher = DefaultHasher::new();
        hash_view_attrs(resource.attributes(), &mut hasher);
        let counter = if self.resource_contexts.insert(hasher.finish()) {
            TransformCounter::ResourceContextDuplicateMiss
        } else {
            TransformCounter::ResourceContextDuplicateHit
        };
        observe_counter(observer, signal, counter, 1);
    }

    pub(super) fn observe_scope_view<S: InstrumentationScopeView>(
        &mut self,
        scope: &S,
        signal: TransformSignal,
        observer: &mut Option<&mut dyn TransformObserver>,
    ) {
        let mut hasher = DefaultHasher::new();
        scope.name().hash(&mut hasher);
        scope.version().hash(&mut hasher);
        hash_view_attrs(scope.attributes(), &mut hasher);
        let counter = if self.scope_contexts.insert(hasher.finish()) {
            TransformCounter::ScopeContextDuplicateMiss
        } else {
            TransformCounter::ScopeContextDuplicateHit
        };
        observe_counter(observer, signal, counter, 1);
    }
}

pub(super) struct ResourceContext {
    pub(super) service_name: Option<String>,
    pub(super) service_namespace: Option<String>,
    pub(super) service_instance_id: Option<String>,
    pub(super) attributes_json: Option<String>,
}

impl ResourceContext {
    pub(super) fn from_view_observed<R: ResourceView>(
        resource: Option<&R>,
        signal: TransformSignal,
        observer: &mut Option<&mut dyn TransformObserver>,
        duplicates: Option<&mut ContextDuplicateTracker>,
    ) -> Self {
        if let (Some(duplicates), Some(resource)) = (duplicates, resource) {
            duplicates.observe_resource_view(resource, signal, observer);
        }
        let (service_name, service_namespace, service_instance_id) = measure_phase(
            observer,
            signal,
            TransformPhase::ResourceContextBuild,
            || {
                let field = |key| {
                    resource
                        .and_then(|resource| view_json::attribute_field(resource.attributes(), key))
                };
                (
                    field(b"service.name"),
                    field(b"service.namespace"),
                    field(b"service.instance.id"),
                )
            },
        );
        let attributes_json = measure_phase(
            observer,
            signal,
            TransformPhase::ResourceAttributesJson,
            || resource.and_then(|resource| view_json::attributes_json(resource.attributes())),
        );

        Self {
            service_name,
            service_namespace,
            service_instance_id,
            attributes_json,
        }
    }
}

pub(super) struct ScopeContext {
    pub(super) name: Option<String>,
    pub(super) version: Option<String>,
    pub(super) attributes_json: Option<String>,
}

impl ScopeContext {
    pub(super) fn from_view_observed<S: InstrumentationScopeView>(
        scope: Option<&S>,
        signal: TransformSignal,
        observer: &mut Option<&mut dyn TransformObserver>,
        duplicates: Option<&mut ContextDuplicateTracker>,
    ) -> Self {
        if let (Some(duplicates), Some(scope)) = (duplicates, scope) {
            duplicates.observe_scope_view(scope, signal, observer);
        }
        let (name, version) =
            measure_phase(observer, signal, TransformPhase::ScopeContextBuild, || {
                (
                    scope
                        .and_then(InstrumentationScopeView::name)
                        .map(|value| String::from_utf8_lossy(value).into_owned()),
                    scope
                        .and_then(InstrumentationScopeView::version)
                        .map(|value| String::from_utf8_lossy(value).into_owned()),
                )
            });
        let attributes_json = measure_phase(
            observer,
            signal,
            TransformPhase::ScopeAttributesJson,
            || scope.and_then(|scope| view_json::attributes_json(scope.attributes())),
        );

        Self {
            name,
            version,
            attributes_json,
        }
    }
}

pub(super) fn hash_attrs(attrs: &[KeyValue], hasher: &mut impl Hasher) {
    attrs.len().hash(hasher);
    for attr in attrs {
        attr.key.hash(hasher);
        hash_any_value(attr.value.as_ref(), hasher);
    }
}

pub(super) fn hash_any_value(value: Option<&AnyValue>, hasher: &mut impl Hasher) {
    match value.and_then(|value| value.value.as_ref()) {
        Some(any_value::Value::StringValue(value)) => {
            1_u8.hash(hasher);
            value.hash(hasher);
        }
        Some(any_value::Value::BoolValue(value)) => {
            2_u8.hash(hasher);
            value.hash(hasher);
        }
        Some(any_value::Value::IntValue(value)) => {
            3_u8.hash(hasher);
            value.hash(hasher);
        }
        Some(any_value::Value::DoubleValue(value)) => {
            4_u8.hash(hasher);
            value.to_bits().hash(hasher);
        }
        Some(any_value::Value::BytesValue(value)) => {
            5_u8.hash(hasher);
            value.hash(hasher);
        }
        Some(any_value::Value::ArrayValue(value)) => {
            6_u8.hash(hasher);
            value.values.len().hash(hasher);
            for value in &value.values {
                hash_any_value(Some(value), hasher);
            }
        }
        Some(any_value::Value::KvlistValue(value)) => {
            7_u8.hash(hasher);
            hash_attrs(&value.values, hasher);
        }
        None => {
            0_u8.hash(hasher);
        }
    }
}

fn hash_view_attrs<A: AttributeView>(
    attributes: impl Iterator<Item = A>,
    hasher: &mut impl Hasher,
) {
    for attribute in attributes {
        attribute.key().hash(hasher);
        match attribute.value() {
            Some(value) => hash_view_any_value(&value, hasher),
            None => 0_u8.hash(hasher),
        }
    }
}

fn hash_view_any_value<'a, V: AnyValueView<'a>>(value: &V, hasher: &mut impl Hasher) {
    match value.value_type() {
        ValueType::Empty => 0_u8.hash(hasher),
        ValueType::String => {
            1_u8.hash(hasher);
            value.as_string().hash(hasher);
        }
        ValueType::Bool => {
            2_u8.hash(hasher);
            value.as_bool().hash(hasher);
        }
        ValueType::Int64 => {
            3_u8.hash(hasher);
            value.as_int64().hash(hasher);
        }
        ValueType::Double => {
            4_u8.hash(hasher);
            value.as_double().map(f64::to_bits).hash(hasher);
        }
        ValueType::Bytes => {
            5_u8.hash(hasher);
            value.as_bytes().hash(hasher);
        }
        ValueType::Array => {
            6_u8.hash(hasher);
            if let Some(values) = value.as_array() {
                for value in values {
                    hash_view_any_value(&value, hasher);
                }
            }
        }
        ValueType::KeyValueList => {
            7_u8.hash(hasher);
            if let Some(attributes) = value.as_kvlist() {
                hash_view_attrs(attributes, hasher);
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct MetricMeta<'a> {
    pub(super) name: &'a str,
    pub(super) description: &'a str,
    pub(super) unit: &'a str,
}
