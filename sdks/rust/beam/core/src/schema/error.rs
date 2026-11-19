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

use thiserror::Error;

/// Errors from schema validation or row field extraction.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum SchemaError {
    #[error("Field '{0}' not found in schema")]
    FieldNotFound(String),

    #[error("Field index {0} out of bounds for schema with {1} fields")]
    IndexOutOfBounds(usize, usize),

    #[error("Type mismatch for field '{field}': expected {expected}, found {actual}")]
    TypeMismatch {
        field: String,
        expected: String,
        actual: String,
    },

    #[error("Invalid schema definition: {0}")]
    InvalidSchema(String),

    #[error("Proto conversion error: {0}")]
    ProtoConversion(String),

    #[error("Malformed schema proto: {0}")]
    Decode(#[from] prost::DecodeError),

    #[error("Value count ({actual}) does not match schema field count ({expected})")]
    ValueCountMismatch { expected: usize, actual: usize },

    /// Like [`SchemaError::TypeMismatch`], for a value-level conversion with no field name.
    #[error("Type mismatch: expected {expected}, found {actual}")]
    ValueTypeMismatch { expected: String, actual: String },

    #[error("Unexpected null value for non-nullable type {expected}")]
    UnexpectedNull { expected: String },

    #[error("Value {value} is out of range for {target}")]
    ValueOutOfRange { value: String, target: String },

    #[error("Map keys may not be null")]
    NullMapKey,

    #[error("Logical type encoding error: {0}")]
    LogicalTypeEncoding(String),
}
