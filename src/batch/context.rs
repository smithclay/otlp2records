//! Shared resource, scope, and metric context extracted from OTLP requests.

use std::{
    collections::hash_map::DefaultHasher,
    collections::HashSet,
    hash::{Hash, Hasher},
};

use crate::views::pdata::{
    AnyValueView, AttributeView, InstrumentationScopeView, ResourceView, ValueType,
};

use super::{
    profile::{Observation, TransformCounter, TransformPhase},
    view_json,
};

/// The resource and scope contexts seen so far in one observed transform.
#[derive(Default)]
pub(super) struct ContextDuplicateTracker {
    resource_contexts: HashSet<u64>,
    scope_contexts: HashSet<u64>,
}

impl ContextDuplicateTracker {
    /// Whether `resource`'s context was seen before, as the counter to bump.
    pub(super) fn resource<R: ResourceView>(&mut self, resource: &R) -> TransformCounter {
        let mut hasher = DefaultHasher::new();
        hash_view_attrs(resource.attributes(), &mut hasher);
        if self.resource_contexts.insert(hasher.finish()) {
            TransformCounter::ResourceContextDuplicateMiss
        } else {
            TransformCounter::ResourceContextDuplicateHit
        }
    }

    /// Whether `scope`'s context was seen before, as the counter to bump.
    pub(super) fn scope<S: InstrumentationScopeView>(&mut self, scope: &S) -> TransformCounter {
        let mut hasher = DefaultHasher::new();
        scope.name().hash(&mut hasher);
        scope.version().hash(&mut hasher);
        hash_view_attrs(scope.attributes(), &mut hasher);
        if self.scope_contexts.insert(hasher.finish()) {
            TransformCounter::ScopeContextDuplicateMiss
        } else {
            TransformCounter::ScopeContextDuplicateHit
        }
    }
}

pub(super) struct ResourceContext {
    pub(super) service_name: Option<String>,
    pub(super) service_namespace: Option<String>,
    pub(super) service_instance_id: Option<String>,
    pub(super) attributes_json: Option<String>,
}

impl ResourceContext {
    /// `None` when `keep` rejects the resource's service name: its rows are
    /// dropped, so nothing else about it is worth computing. The attribute
    /// JSON is serialized only when `attributes` asks for it.
    pub(super) fn from_view<R: ResourceView>(
        resource: Option<&R>,
        obs: &mut Observation<'_>,
        keep: impl FnOnce(Option<&str>) -> bool,
        attributes: bool,
    ) -> Option<Self> {
        if let Some(resource) = resource {
            obs.track_resource(resource);
        }
        let (service_name, service_namespace, service_instance_id) =
            obs.phase(TransformPhase::ResourceContextBuild, || {
                let field = |key| {
                    resource
                        .and_then(|resource| view_json::attribute_field(resource.attributes(), key))
                };
                (
                    field(b"service.name"),
                    field(b"service.namespace"),
                    field(b"service.instance.id"),
                )
            });
        if !keep(service_name.as_deref()) {
            return None;
        }
        let attributes_json = if attributes {
            obs.phase(TransformPhase::ResourceAttributesJson, || {
                resource.and_then(|resource| view_json::attributes_json(resource.attributes()))
            })
        } else {
            None
        };

        Some(Self {
            service_name,
            service_namespace,
            service_instance_id,
            attributes_json,
        })
    }
}

pub(super) struct ScopeContext {
    pub(super) name: Option<String>,
    pub(super) version: Option<String>,
    pub(super) attributes_json: Option<String>,
}

impl ScopeContext {
    pub(super) fn from_view<S: InstrumentationScopeView>(
        scope: Option<&S>,
        obs: &mut Observation<'_>,
        attributes: bool,
    ) -> Self {
        if let Some(scope) = scope {
            obs.track_scope(scope);
        }
        let (name, version) = obs.phase(TransformPhase::ScopeContextBuild, || {
            (
                scope
                    .and_then(InstrumentationScopeView::name)
                    .map(|value| String::from_utf8_lossy(value).into_owned()),
                scope
                    .and_then(InstrumentationScopeView::version)
                    .map(|value| String::from_utf8_lossy(value).into_owned()),
            )
        });
        let attributes_json = if attributes {
            obs.phase(TransformPhase::ScopeAttributesJson, || {
                scope.and_then(|scope| view_json::attributes_json(scope.attributes()))
            })
        } else {
            None
        };

        Self {
            name,
            version,
            attributes_json,
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
