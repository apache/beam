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

use std::sync::Arc;

use beam_model::pipeline as proto;

use super::error::SchemaError;
use super::types::{AtomicType, Field, FieldType, Schema, SchemaOption, TypeInfo, TypedValue};

impl From<AtomicType> for proto::AtomicType {
    fn from(t: AtomicType) -> Self {
        match t {
            AtomicType::Byte => proto::AtomicType::Byte,
            AtomicType::Int16 => proto::AtomicType::Int16,
            AtomicType::Int32 => proto::AtomicType::Int32,
            AtomicType::Int64 => proto::AtomicType::Int64,
            AtomicType::Float => proto::AtomicType::Float,
            AtomicType::Double => proto::AtomicType::Double,
            AtomicType::String => proto::AtomicType::String,
            AtomicType::Boolean => proto::AtomicType::Boolean,
            AtomicType::Bytes => proto::AtomicType::Bytes,
        }
    }
}

impl TryFrom<proto::AtomicType> for AtomicType {
    type Error = SchemaError;

    fn try_from(t: proto::AtomicType) -> Result<Self, Self::Error> {
        match t {
            proto::AtomicType::Byte => Ok(Self::Byte),
            proto::AtomicType::Int16 => Ok(Self::Int16),
            proto::AtomicType::Int32 => Ok(Self::Int32),
            proto::AtomicType::Int64 => Ok(Self::Int64),
            proto::AtomicType::Float => Ok(Self::Float),
            proto::AtomicType::Double => Ok(Self::Double),
            proto::AtomicType::String => Ok(Self::String),
            proto::AtomicType::Boolean => Ok(Self::Boolean),
            proto::AtomicType::Bytes => Ok(Self::Bytes),
            proto::AtomicType::Unspecified => Err(SchemaError::ProtoConversion(
                "Unspecified atomic type".into(),
            )),
        }
    }
}

impl From<FieldType> for proto::FieldType {
    fn from(ft: FieldType) -> Self {
        Self::from(&ft)
    }
}

impl From<&FieldType> for proto::FieldType {
    fn from(ft: &FieldType) -> Self {
        let type_info = match &ft.type_info {
            TypeInfo::Atomic(at) => {
                proto::field_type::TypeInfo::AtomicType(proto::AtomicType::from(*at) as i32)
            }
            TypeInfo::Array(elem) => {
                proto::field_type::TypeInfo::ArrayType(Box::new(proto::ArrayType {
                    element_type: Some(Box::new(proto::FieldType::from(elem.as_ref()))),
                }))
            }
            TypeInfo::Iterable(elem) => {
                proto::field_type::TypeInfo::IterableType(Box::new(proto::IterableType {
                    element_type: Some(Box::new(proto::FieldType::from(elem.as_ref()))),
                }))
            }
            TypeInfo::Map(k, v) => proto::field_type::TypeInfo::MapType(Box::new(proto::MapType {
                key_type: Some(Box::new(proto::FieldType::from(k.as_ref()))),
                value_type: Some(Box::new(proto::FieldType::from(v.as_ref()))),
            })),
            TypeInfo::Row(schema) => proto::field_type::TypeInfo::RowType(proto::RowType {
                schema: Some(proto::Schema::from(schema.as_ref())),
            }),
            TypeInfo::Logical {
                urn,
                payload,
                representation,
                argument,
            } => {
                let (argument_type, argument) = argument
                    .as_ref()
                    .map(|argument| {
                        let TypedValue { field_type, value } = argument.as_ref();
                        (Box::new(proto::FieldType::from(field_type)), value.clone())
                    })
                    .unzip();
                proto::field_type::TypeInfo::LogicalType(Box::new(proto::LogicalType {
                    urn: urn.clone(),
                    payload: payload.clone(),
                    representation: Some(Box::new(proto::FieldType::from(representation.as_ref()))),
                    argument_type,
                    argument: argument.flatten(),
                }))
            }
        };

        proto::FieldType {
            nullable: ft.nullable,
            type_info: Some(type_info),
        }
    }
}

impl TryFrom<proto::FieldType> for FieldType {
    type Error = SchemaError;

    fn try_from(ft: proto::FieldType) -> Result<Self, Self::Error> {
        let Some(type_info) = ft.type_info else {
            return Err(SchemaError::ProtoConversion(
                "Missing type_info in FieldType".into(),
            ));
        };

        let info = match type_info {
            proto::field_type::TypeInfo::AtomicType(raw) => {
                let proto_atomic = proto::AtomicType::try_from(raw).map_err(|_| {
                    SchemaError::ProtoConversion(format!("Unknown atomic type: {raw}"))
                })?;
                TypeInfo::Atomic(AtomicType::try_from(proto_atomic)?)
            }
            proto::field_type::TypeInfo::ArrayType(arr) => {
                let elem = arr.element_type.ok_or_else(|| {
                    SchemaError::ProtoConversion("Missing array element type".into())
                })?;
                TypeInfo::Array(Box::new(FieldType::try_from(*elem)?))
            }
            proto::field_type::TypeInfo::IterableType(iter) => {
                let elem = iter.element_type.ok_or_else(|| {
                    SchemaError::ProtoConversion("Missing iterable element type".into())
                })?;
                TypeInfo::Iterable(Box::new(FieldType::try_from(*elem)?))
            }
            proto::field_type::TypeInfo::MapType(m) => {
                let key = m
                    .key_type
                    .ok_or_else(|| SchemaError::ProtoConversion("Missing map key type".into()))?;
                let val = m
                    .value_type
                    .ok_or_else(|| SchemaError::ProtoConversion("Missing map value type".into()))?;
                TypeInfo::Map(
                    Box::new(FieldType::try_from(*key)?),
                    Box::new(FieldType::try_from(*val)?),
                )
            }
            proto::field_type::TypeInfo::RowType(r) => {
                let s = r.schema.ok_or_else(|| {
                    SchemaError::ProtoConversion("Missing nested row schema".into())
                })?;
                TypeInfo::Row(Arc::new(Schema::try_from(s)?))
            }
            proto::field_type::TypeInfo::LogicalType(logical) => {
                let proto::LogicalType {
                    urn,
                    payload,
                    representation,
                    argument_type,
                    argument,
                } = *logical;
                let rep = representation.ok_or_else(|| {
                    SchemaError::ProtoConversion("Missing logical representation type".into())
                })?;
                let argument = argument_type
                    .map(|argument_type| {
                        FieldType::try_from(*argument_type).map(|field_type| TypedValue {
                            field_type,
                            value: argument,
                        })
                    })
                    .transpose()?;
                TypeInfo::Logical {
                    urn,
                    payload,
                    representation: Box::new(FieldType::try_from(*rep)?),
                    argument: argument.map(Box::new),
                }
            }
        };

        Ok(FieldType::new(info, ft.nullable))
    }
}

impl From<Field> for proto::Field {
    fn from(f: Field) -> Self {
        Self::from(&f)
    }
}

impl From<&Field> for proto::Field {
    fn from(f: &Field) -> Self {
        proto::Field {
            name: f.name.clone(),
            description: f.description.clone().unwrap_or_default(),
            r#type: Some(proto::FieldType::from(&f.field_type)),
            id: f.id.unwrap_or_default(),
            encoding_position: f.encoding_position.unwrap_or_default(),
            options: f.options.iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<proto::Field> for Field {
    type Error = SchemaError;

    /// Position 0 is the proto3 default, so only [`Schema::try_from`] can keep `Some(0)`.
    fn try_from(f: proto::Field) -> Result<Self, Self::Error> {
        let field_type = f.r#type.ok_or_else(|| {
            SchemaError::ProtoConversion(format!("Missing type for field {}", f.name))
        })?;
        Ok(Field {
            name: f.name,
            description: (!f.description.is_empty()).then_some(f.description),
            field_type: FieldType::try_from(field_type)?,
            id: (f.id != 0).then_some(f.id),
            encoding_position: (f.encoding_position != 0).then_some(f.encoding_position),
            options: options_from_proto(f.options)?,
        })
    }
}

impl From<Schema> for proto::Schema {
    fn from(s: Schema) -> Self {
        Self::from(&s)
    }
}

impl From<&Schema> for proto::Schema {
    fn from(s: &Schema) -> Self {
        proto::Schema {
            fields: s
                .fields
                .iter()
                .map(|f| proto::Field {
                    encoding_position: if s.encoding_positions_set {
                        f.encoding_position.unwrap_or_default()
                    } else {
                        0
                    },
                    ..proto::Field::from(f)
                })
                .collect(),
            id: s.id.clone().unwrap_or_default(),
            options: s.options.iter().map(Into::into).collect(),
            encoding_positions_set: s.encoding_positions_set,
        }
    }
}

impl TryFrom<proto::Schema> for Schema {
    type Error = SchemaError;

    /// Keeps field encoding positions only when `encoding_positions_set` is true.
    fn try_from(s: proto::Schema) -> Result<Self, Self::Error> {
        let positions_set = s.encoding_positions_set;
        let fields = s
            .fields
            .into_iter()
            .map(|f| {
                let position = f.encoding_position;
                Field::try_from(f).map(|field| Field {
                    encoding_position: positions_set.then_some(position),
                    ..field
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Schema {
            fields,
            id: (!s.id.is_empty()).then_some(s.id),
            encoding_positions_set: positions_set,
            options: options_from_proto(s.options)?,
        })
    }
}

impl From<SchemaOption> for proto::Option {
    fn from(option: SchemaOption) -> Self {
        Self::from(&option)
    }
}

impl From<&SchemaOption> for proto::Option {
    fn from(option: &SchemaOption) -> Self {
        proto::Option {
            name: option.name.clone(),
            r#type: Some(proto::FieldType::from(&option.value.field_type)),
            value: option.value.value.clone(),
        }
    }
}

impl TryFrom<proto::Option> for SchemaOption {
    type Error = SchemaError;

    /// Keeps the value as it is. `TypedValue::value` checks it against the type.
    fn try_from(option: proto::Option) -> Result<Self, Self::Error> {
        let field_type = option.r#type.ok_or_else(|| {
            SchemaError::ProtoConversion(format!("Missing type for option {}", option.name))
        })?;
        let value = TypedValue {
            field_type: FieldType::try_from(field_type)?,
            value: option.value,
        };
        Ok(SchemaOption::new(option.name, value))
    }
}

fn options_from_proto(options: Vec<proto::Option>) -> Result<Vec<SchemaOption>, SchemaError> {
    options.into_iter().map(SchemaOption::try_from).collect()
}
