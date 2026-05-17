//! Shared resource, scope, and metric context extracted from OTLP requests.

use std::{
    collections::hash_map::DefaultHasher,
    collections::HashSet,
    hash::{Hash, Hasher},
};

use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};

use super::{
    json::{attr_field, attrs_json},
    profile::{
        measure_phase, observe_counter, TransformCounter, TransformObserver, TransformPhase,
        TransformSignal,
    },
    util::non_empty_str,
};

#[derive(Default)]
pub(super) struct ContextDuplicateTracker {
    resource_contexts: HashSet<u64>,
    scope_contexts: HashSet<u64>,
}

impl ContextDuplicateTracker {
    pub(super) fn observe_resource(
        &mut self,
        attrs: &[KeyValue],
        signal: TransformSignal,
        observer: &mut Option<&mut dyn TransformObserver>,
    ) {
        let fingerprint = resource_fingerprint(attrs);
        let counter = if self.resource_contexts.insert(fingerprint) {
            TransformCounter::ResourceContextDuplicateMiss
        } else {
            TransformCounter::ResourceContextDuplicateHit
        };
        observe_counter(observer, signal, counter, 1);
    }

    pub(super) fn observe_scope(
        &mut self,
        name: Option<&str>,
        version: Option<&str>,
        attrs: &[KeyValue],
        signal: TransformSignal,
        observer: &mut Option<&mut dyn TransformObserver>,
    ) {
        let fingerprint = scope_fingerprint(name, version, attrs);
        let counter = if self.scope_contexts.insert(fingerprint) {
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
    pub(super) fn from_attrs_observed(
        attrs: &[KeyValue],
        signal: TransformSignal,
        observer: &mut Option<&mut dyn TransformObserver>,
        duplicates: Option<&mut ContextDuplicateTracker>,
    ) -> Self {
        if let Some(duplicates) = duplicates {
            duplicates.observe_resource(attrs, signal, observer);
        }
        let (service_name, service_namespace, service_instance_id) = measure_phase(
            observer,
            signal,
            TransformPhase::ResourceContextBuild,
            || {
                (
                    attr_field(attrs, "service.name"),
                    attr_field(attrs, "service.namespace"),
                    attr_field(attrs, "service.instance.id"),
                )
            },
        );
        let attributes_json = measure_phase(
            observer,
            signal,
            TransformPhase::ResourceAttributesJson,
            || attrs_json(attrs),
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
    pub(super) fn new_observed(
        name: Option<&str>,
        version: Option<&str>,
        attrs: &[KeyValue],
        signal: TransformSignal,
        observer: &mut Option<&mut dyn TransformObserver>,
        duplicates: Option<&mut ContextDuplicateTracker>,
    ) -> Self {
        if let Some(duplicates) = duplicates {
            duplicates.observe_scope(name, version, attrs, signal, observer);
        }
        let (name, version) =
            measure_phase(observer, signal, TransformPhase::ScopeContextBuild, || {
                (non_empty_str(name), non_empty_str(version))
            });
        let attributes_json = measure_phase(
            observer,
            signal,
            TransformPhase::ScopeAttributesJson,
            || attrs_json(attrs),
        );

        Self {
            name,
            version,
            attributes_json,
        }
    }
}

fn resource_fingerprint(attrs: &[KeyValue]) -> u64 {
    let mut hasher = DefaultHasher::new();
    hash_attrs(attrs, &mut hasher);
    hasher.finish()
}

fn scope_fingerprint(name: Option<&str>, version: Option<&str>, attrs: &[KeyValue]) -> u64 {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    version.hash(&mut hasher);
    hash_attrs(attrs, &mut hasher);
    hasher.finish()
}

fn hash_attrs(attrs: &[KeyValue], hasher: &mut impl Hasher) {
    attrs.len().hash(hasher);
    for attr in attrs {
        attr.key.hash(hasher);
        hash_any_value(attr.value.as_ref(), hasher);
    }
}

fn hash_any_value(value: Option<&AnyValue>, hasher: &mut impl Hasher) {
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

#[derive(Clone, Copy)]
pub(super) struct MetricMeta<'a> {
    pub(super) name: &'a str,
    pub(super) description: &'a str,
    pub(super) unit: &'a str,
}
