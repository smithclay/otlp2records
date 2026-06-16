//! Shared Arrow `Field` builders.
//!
//! Both the normalized output schemas (`schema::arrow`) and the OTAP star
//! schemas (`schema::otap`) describe columns with the same handful of Arrow
//! types. These constructors keep a single definition of each column shape so
//! the two schema files cannot drift (e.g. one declaring `time_unix_nano` as a
//! plain `Int64` while the other uses a `Timestamp`).

use arrow_schema::{DataType, Field, TimeUnit};

pub(crate) fn ts_ns(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        nullable,
    )
}

pub(crate) fn duration_ns(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Duration(TimeUnit::Nanosecond), nullable)
}

pub(crate) fn utf8(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Utf8, nullable)
}

pub(crate) fn fixed(name: &str, size: i32, nullable: bool) -> Field {
    Field::new(name, DataType::FixedSizeBinary(size), nullable)
}

pub(crate) fn u32_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::UInt32, nullable)
}

pub(crate) fn u64_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::UInt64, nullable)
}

pub(crate) fn i32_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Int32, nullable)
}

pub(crate) fn i64_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Int64, nullable)
}

pub(crate) fn f64_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Float64, nullable)
}

pub(crate) fn bool_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Boolean, nullable)
}

pub(crate) fn u64_list(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::List(Field::new("item", DataType::UInt64, true).into()),
        nullable,
    )
}

pub(crate) fn f64_list(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::List(Field::new("item", DataType::Float64, true).into()),
        nullable,
    )
}
