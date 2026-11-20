/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::fmt;
use std::sync::Arc;

use beam_model::pipeline as proto;
use prost::Message;

use super::error::SchemaError;

/// Primitive (atomic) types of a Beam schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AtomicType {
    Byte,
    Int16,
    Int32,
    Int64,
    Float,
    Double,
    String,
    Boolean,
    Bytes,
}

impl fmt::Display for AtomicType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Byte => write!(f, "BYTE"),
            Self::Int16 => write!(f, "INT16"),
            Self::Int32 => write!(f, "INT32"),
            Self::Int64 => write!(f, "INT64"),
            Self::Float => write!(f, "FLOAT"),
            Self::Double => write!(f, "DOUBLE"),
            Self::String => write!(f, "STRING"),
            Self::Boolean => write!(f, "BOOLEAN"),
            Self::Bytes => write!(f, "BYTES"),
        }
    }
}

/// Type information for a field in a Beam schema.
#[derive(Debug, Clone, PartialEq)]
pub enum TypeInfo {
    Atomic(AtomicType),
    Array(Box<FieldType>),
    Iterable(Box<FieldType>),
    Map(Box<FieldType>, Box<FieldType>),
    /// A nested row. The schema is shared, so a decoder clones only the `Arc` for each row.
    Row(Arc<Schema>),
    Logical {
        urn: String,
        payload: Vec<u8>,
        representation: Box<FieldType>,
        /// The argument that parameterizes the logical type, if the type takes one.
        argument: Option<Box<TypedValue>>,
    },
}

impl fmt::Display for TypeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Atomic(a) => write!(f, "{a}"),
            Self::Array(elem) => write!(f, "ARRAY<{elem}>"),
            Self::Iterable(elem) => write!(f, "ITERABLE<{elem}>"),
            Self::Map(k, v) => write!(f, "MAP<{k}, {v}>"),
            Self::Row(schema) => write!(f, "ROW<{schema:?}>"),
            Self::Logical { urn, .. } => write!(f, "LOGICAL<{urn}>"),
        }
    }
}

/// A complete field type specification, including nullability.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldType {
    pub nullable: bool,
    pub type_info: TypeInfo,
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.nullable {
            write!(f, "{}?", self.type_info)
        } else {
            write!(f, "{}", self.type_info)
        }
    }
}

impl FieldType {
    pub fn new(type_info: TypeInfo, nullable: bool) -> Self {
        Self {
            nullable,
            type_info,
        }
    }

    pub fn atomic(atomic: AtomicType) -> Self {
        Self::new(TypeInfo::Atomic(atomic), false)
    }

    pub fn nullable_atomic(atomic: AtomicType) -> Self {
        Self::new(TypeInfo::Atomic(atomic), true)
    }

    pub fn boolean() -> Self {
        Self::atomic(AtomicType::Boolean)
    }

    pub fn byte() -> Self {
        Self::atomic(AtomicType::Byte)
    }

    pub fn int16() -> Self {
        Self::atomic(AtomicType::Int16)
    }

    pub fn int32() -> Self {
        Self::atomic(AtomicType::Int32)
    }

    pub fn int64() -> Self {
        Self::atomic(AtomicType::Int64)
    }

    pub fn float() -> Self {
        Self::atomic(AtomicType::Float)
    }

    pub fn double() -> Self {
        Self::atomic(AtomicType::Double)
    }

    pub fn string() -> Self {
        Self::atomic(AtomicType::String)
    }

    pub fn bytes() -> Self {
        Self::atomic(AtomicType::Bytes)
    }

    pub fn array(element_type: FieldType) -> Self {
        Self::new(TypeInfo::Array(Box::new(element_type)), false)
    }

    pub fn map(key_type: FieldType, val_type: FieldType) -> Self {
        Self::new(TypeInfo::Map(Box::new(key_type), Box::new(val_type)), false)
    }

    pub fn row(schema: impl Into<Arc<Schema>>) -> Self {
        Self::new(TypeInfo::Row(schema.into()), false)
    }

    /// Creates a logical type: a named interpretation of `representation` that an SDK which
    /// recognizes `urn` decodes into a richer value.
    pub fn logical(urn: impl Into<String>, payload: Vec<u8>, representation: Self) -> Self {
        Self::new(
            TypeInfo::Logical {
                urn: urn.into(),
                payload,
                representation: Box::new(representation),
                argument: None,
            },
            false,
        )
    }

    /// Creates a logical type with an argument, for example the precision of a fixed-size
    /// decimal.
    pub fn logical_with_argument(
        urn: impl Into<String>,
        payload: Vec<u8>,
        representation: Self,
        argument: TypedValue,
    ) -> Self {
        Self::new(
            TypeInfo::Logical {
                urn: urn.into(),
                payload,
                representation: Box::new(representation),
                argument: Some(Box::new(argument)),
            },
            false,
        )
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }
}

/// A named field in a Beam schema.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub name: String,
    pub description: Option<String>,
    pub field_type: FieldType,
    pub id: Option<i32>,
    pub encoding_position: Option<i32>,
    pub options: Vec<SchemaOption>,
}

impl Field {
    pub fn new(name: impl Into<String>, field_type: FieldType) -> Self {
        Self {
            name: name.into(),
            description: None,
            field_type,
            id: None,
            encoding_position: None,
            options: Vec::new(),
        }
    }

    pub fn nullable(name: impl Into<String>, field_type: FieldType) -> Self {
        Self::new(name, field_type.with_nullable(true))
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_encoding_position(mut self, pos: i32) -> Self {
        self.encoding_position = Some(pos);
        self
    }

    pub fn with_option(mut self, option: SchemaOption) -> Self {
        self.options.push(option);
        self
    }
}

/// Schema that defines the layout and types of a `Row`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Schema {
    pub fields: Vec<Field>,
    pub id: Option<String>,
    pub encoding_positions_set: bool,
    pub options: Vec<SchemaOption>,
}

impl Schema {
    /// Builds a schema with no id and no options. Sets `encoding_positions_set` when every
    /// field has an encoding position.
    pub fn new(fields: Vec<Field>) -> Self {
        let encoding_positions_set =
            !fields.is_empty() && fields.iter().all(|f| f.encoding_position.is_some());
        Self {
            fields,
            id: None,
            encoding_positions_set,
            options: Vec::new(),
        }
    }

    pub fn with_option(mut self, option: SchemaOption) -> Self {
        self.options.push(option);
        self
    }

    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::default()
    }

    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }

    pub fn field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }

    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    /// Serializes this schema to `Schema` proto bytes.
    pub fn to_proto_bytes(&self) -> Vec<u8> {
        let proto = proto::Schema::from(self);
        proto.encode_to_vec()
    }

    /// Deserializes a schema from `Schema` proto bytes.
    pub fn from_proto_bytes(bytes: &[u8]) -> Result<Self, SchemaError> {
        let proto = proto::Schema::decode(bytes)?;
        Self::try_from(proto)
    }
}

#[derive(Debug, Default)]
pub struct SchemaBuilder {
    fields: Vec<Field>,
    id: Option<String>,
    options: Vec<SchemaOption>,
}

impl SchemaBuilder {
    pub fn field(mut self, name: impl Into<String>, field_type: FieldType) -> Self {
        self.fields.push(Field::new(name, field_type));
        self
    }

    pub fn nullable_field(mut self, name: impl Into<String>, field_type: FieldType) -> Self {
        self.fields.push(Field::nullable(name, field_type));
        self
    }

    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn option(mut self, option: SchemaOption) -> Self {
        self.options.push(option);
        self
    }

    pub fn build(self) -> Schema {
        Schema {
            id: self.id,
            options: self.options,
            ..Schema::new(self.fields)
        }
    }
}

/// A named value that annotates a [`Schema`] or a [`Field`]. Other SDKs and runners read
/// options by name, so the conversion to the `Schema` proto keeps them.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaOption {
    pub name: String,
    pub value: TypedValue,
}

impl SchemaOption {
    pub fn new(name: impl Into<String>, value: TypedValue) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }
}

/// A value together with its schema type: the value of a [`SchemaOption`], or the argument
/// of a logical type.
#[derive(Debug, Clone, PartialEq)]
pub struct TypedValue {
    pub(super) field_type: FieldType,
    /// The value in the proto form, so that a value from another SDK round-trips unchanged.
    /// `None` is null.
    pub(super) value: Option<proto::FieldValue>,
}

impl TypedValue {
    pub fn field_type(&self) -> &FieldType {
        &self.field_type
    }
}
