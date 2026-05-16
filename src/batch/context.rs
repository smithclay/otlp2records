//! Shared resource, scope, and metric context extracted from OTLP requests.

use opentelemetry_proto::tonic::common::v1::KeyValue;

use super::{
    json::{attr_field, attrs_json},
    util::non_empty_str,
};

pub(super) struct ResourceContext {
    pub(super) service_name: Option<String>,
    pub(super) service_namespace: Option<String>,
    pub(super) service_instance_id: Option<String>,
    pub(super) attributes_json: Option<String>,
}

impl ResourceContext {
    pub(super) fn from_attrs(attrs: &[KeyValue]) -> Self {
        Self {
            service_name: attr_field(attrs, "service.name"),
            service_namespace: attr_field(attrs, "service.namespace"),
            service_instance_id: attr_field(attrs, "service.instance.id"),
            attributes_json: attrs_json(attrs),
        }
    }
}

pub(super) struct ScopeContext {
    pub(super) name: Option<String>,
    pub(super) version: Option<String>,
    pub(super) attributes_json: Option<String>,
}

impl ScopeContext {
    pub(super) fn new(name: Option<&str>, version: Option<&str>, attrs: &[KeyValue]) -> Self {
        Self {
            name: non_empty_str(name),
            version: non_empty_str(version),
            attributes_json: attrs_json(attrs),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct MetricMeta<'a> {
    pub(super) name: &'a str,
    pub(super) description: &'a str,
    pub(super) unit: &'a str,
}
