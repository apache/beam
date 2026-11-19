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

//! Core traits and context definitions for Beam coders.

use std::sync::Arc;
use thiserror::Error;

/// Error encountered during element encoding or decoding.
#[derive(Error, Debug)]
pub enum CoderError {
    #[error("IO error during encoding/decoding: {0}")]
    Io(#[from] std::io::Error),
    #[error("UTF-8 decoding error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("Schema error during encoding/decoding: {0}")]
    Schema(#[from] crate::schema::SchemaError),
    #[error("Decoding format error: {0}")]
    Format(String),
    /// Error from a runner state stream backing a state-backed iterable.
    #[error("{0}")]
    StateStream(String),
}

/// Position of an element in the byte stream, which sets how it is delimited. In `Nested`
/// context (inside a tuple, a KV or a stream), a variable-length element must have a length
/// prefix. In `WholeStream` context, the element extends to the end of the stream.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Context {
    WholeStream,
    Nested,
}

/// Maps each reader schema field to its position in a writer schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldMap {
    /// The writer schema has the wire layout of the reader type.
    Same,
    /// For each reader field, the writer field index, or `None` for an absent nullable field.
    /// The `Arc` lets a lazy iterable decoder share the map with no copy.
    ByName(Arc<[Option<usize>]>),
}

/// Keeps the [`FieldMap`] of one type for the last writer schema. A hit costs one pointer
/// compare, so call [`get`](Self::get) for each element.
#[derive(Clone, Debug, Default)]
pub struct FieldMapCache {
    resolved: Option<(Arc<crate::schema::Schema>, Option<FieldMap>)>,
}

impl FieldMapCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns [`DefaultCoder::resolve_field_map`] of `T` for `schema`. Resolves again only
    /// when `schema` is a different `Arc`. Returns `Ok(None)` when `schema` is `None` or `T`
    /// has no field map.
    pub fn get<T: DefaultCoder>(
        &mut self,
        schema: Option<&Arc<crate::schema::Schema>>,
    ) -> Result<Option<&FieldMap>, crate::schema::SchemaError> {
        let Some(schema) = schema else {
            return Ok(None);
        };
        let resolved = match self.resolved.take() {
            Some((cached, map)) if Arc::ptr_eq(&cached, schema) => (cached, map),
            Some(_) | None => (Arc::clone(schema), T::resolve_field_map(schema)?),
        };
        Ok(self.resolved.insert(resolved).1.as_ref())
    }

    /// Decodes one element of `T` from `bytes` with the field map for `schema`.
    pub fn decode<T: DefaultCoder>(
        &mut self,
        bytes: &[u8],
        schema: Option<&Arc<crate::schema::Schema>>,
        state_reader: Option<&Arc<dyn super::iterable::StateStreamReader>>,
    ) -> Result<T, CoderError> {
        let map = self.get::<T>(schema)?;
        T::decode_with_context(bytes, schema, map, state_reader)
    }
}

/// Encodes and decodes values of type `T` in a Beam wire format.
pub trait Coder<T>: Send + Sync + 'static {
    /// Returns the Runner API coder URN, standard or custom.
    fn urn(&self) -> &'static str;

    fn encode(&self, value: &T, writer: &mut Vec<u8>, context: Context) -> Result<(), CoderError>;

    fn decode(&self, reader: &mut &[u8], context: Context) -> Result<T, CoderError>;
}

/// Records coders in the Runner API pipeline components.
pub trait CoderRegistry {
    fn register_coder(&self, urn: &str, component_coder_ids: Vec<String>) -> String;

    /// Registers a coder that carries a spec payload. `Pipeline` overrides this. The default
    /// drops `payload` for URN-only test registries.
    fn register_coder_with_payload(
        &self,
        urn: &str,
        component_coder_ids: Vec<String>,
        payload: Vec<u8>,
    ) -> String {
        let _ = payload;
        self.register_coder(urn, component_coder_ids)
    }
}

impl<T: ?Sized + CoderRegistry> CoderRegistry for &T {
    fn register_coder(&self, urn: &str, component_coder_ids: Vec<String>) -> String {
        (**self).register_coder(urn, component_coder_ids)
    }

    fn register_coder_with_payload(
        &self,
        urn: &str,
        component_coder_ids: Vec<String>,
        payload: Vec<u8>,
    ) -> String {
        (**self).register_coder_with_payload(urn, component_coder_ids, payload)
    }
}

/// Associates a pipeline type with its default coder and gives byte-level encode and decode.
pub trait DefaultCoder: Send + Sync + Sized + 'static {
    type Coder: Coder<Self>;
    fn coder() -> Self::Coder;

    /// Encodes this value to element bytes.
    fn encode(&self) -> Result<Vec<u8>, CoderError> {
        let mut buf = Vec::new();
        self.encode_element(&mut buf)?;
        Ok(buf)
    }

    /// Decodes an element from bytes.
    fn decode(mut bytes: &[u8]) -> Result<Self, CoderError> {
        Self::decode_element(&mut bytes)
    }

    /// Encodes this value to a byte writer in the element context of the type.
    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError>;

    /// Decodes an element from a byte reader.
    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError>;

    /// Decodes with an optional schema. The default calls [`Self::decode_element_with_context`]
    /// with no field map and no state reader.
    fn decode_element_with_schema(
        reader: &mut &[u8],
        schema: Option<&Arc<crate::schema::Schema>>,
    ) -> Result<Self, CoderError> {
        Self::decode_element_with_context(reader, schema, None, None)
    }

    /// Decodes with an optional schema, field map and state stream reader. The default calls
    /// [`Self::decode_element`]. Override it for a type that needs a schema, such as `Row`, or
    /// reads state-backed iterables, such as `BeamIterable`, or wraps one. Pass all three on to
    /// the components.
    ///
    /// `field_map` comes from [`Self::resolve_field_map`] for `schema`. `None` means that the
    /// caller did not resolve one, so a row type must compare `schema` with its own layout.
    fn decode_element_with_context(
        reader: &mut &[u8],
        _schema: Option<&Arc<crate::schema::Schema>>,
        _field_map: Option<&FieldMap>,
        _state_reader: Option<&Arc<dyn super::iterable::StateStreamReader>>,
    ) -> Result<Self, CoderError> {
        Self::decode_element(reader)
    }

    /// Decodes an element from bytes with an optional schema context.
    fn decode_with_schema(
        bytes: &[u8],
        schema: Option<&Arc<crate::schema::Schema>>,
    ) -> Result<Self, CoderError> {
        Self::decode_with_context(bytes, schema, None, None)
    }

    /// Decodes an element from bytes with an optional schema, field map and state reader.
    fn decode_with_context(
        mut bytes: &[u8],
        schema: Option<&Arc<crate::schema::Schema>>,
        field_map: Option<&FieldMap>,
        state_reader: Option<&Arc<dyn super::iterable::StateStreamReader>>,
    ) -> Result<Self, CoderError> {
        Self::decode_element_with_context(&mut bytes, schema, field_map, state_reader)
    }

    /// Builds a [`FieldMap`] for decoding rows written with `schema`.
    ///
    /// Returns `Ok(None)` when the type does not map row fields by name.
    fn resolve_field_map(
        _schema: &crate::schema::Schema,
    ) -> Result<Option<FieldMap>, crate::schema::SchemaError> {
        Ok(None)
    }

    /// Registers the coder for this type in `registry` and returns its coder ID.
    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String;

    /// Registers the coder for this value and returns its coder ID. The default calls
    /// [`Self::register_coder`]. Override it for a type whose coder depends on the value, such
    /// as a row and its schema, or that holds such a value.
    fn register_value_coder<R: CoderRegistry + ?Sized>(&self, registry: &R) -> String {
        Self::register_coder(registry)
    }
}
