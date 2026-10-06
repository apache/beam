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

//! Derive macros for Apache Beam: schemas (`BeamRow`, `BeamEnum`), `DoFn` registration
//! (`Register`), the outputs of a `DoFn` (`Outputs`), and the state cells and timer families
//! of a `StatefulDoFn` (`State`, `Timers`).
//!
//! `apache-beam-core` re-exports these macros behind its `derive` feature. Depend on
//! `apache-beam` or `apache-beam-core`, not on this crate directly.
//!
//! Expanded code references Beam types through `::beam` by default. Internal crate tests
//! override that path with `#[beam(crate = "crate")]`.

mod attrs;
mod beam_enum;
mod outputs;
mod register;
mod row;
mod state;
mod timers;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

/// Derives `BeamRow`, `BeamField`, and `DefaultCoder` for a struct with named fields.
///
/// # Container attributes
///
/// - `#[beam(id = "...")]` — sets the schema id.
/// - `#[beam(crate = "...")]` — path to the Beam crate; defaults to `::beam`.
///
/// # Field attributes
///
/// - `#[beam(rename = "...")]` — Beam field name; defaults to the Rust field name.
/// - `#[beam(skip)]` — omits the field from the schema and reconstructs it with [`Default`].
/// - `#[beam(bytes)]` — maps a `Vec<u8>` field to `BYTES` instead of an array.
/// - `#[beam(encoding_position = N)]` — sets the wire encoding position.
#[proc_macro_derive(BeamRow, attributes(beam))]
pub fn derive_beam_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    row::expand_row(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Derives `BeamField` for a fieldless enum, mapping variant names to `STRING`.
///
/// Beam has no portable enum logical type, so variants encode as their identifier strings.
#[proc_macro_derive(BeamEnum, attributes(beam))]
pub fn derive_beam_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    beam_enum::expand_enum(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Registers a `DoFn`, `StatefulDoFn`, `SplittableDoFn` or `CombineFn` type, so the worker
/// builds it from the pipeline graph instead of a rebuilt pipeline.
///
/// The type must also implement `serde::Serialize` and `serde::Deserialize`, because its
/// fields travel in the graph. A field with `#[serde(skip)]` gets its `Default` value on the
/// worker; build it in `setup`. A type without this derive stays on the rebuild path.
///
/// # Container attributes
///
/// - `#[beam(id = "...")]` — the id that the worker finds the type by. Defaults to the
///   module path and the type name. Set it only when one binary builds the graph and a
///   different binary runs it, and the type moves between the two.
/// - `#[beam(crate = "...")]` — path to the Beam crate; defaults to `::beam`.
#[proc_macro_derive(Register, attributes(beam))]
pub fn derive_register(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    register::expand_register(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Derives the state cells of a `StatefulDoFn`: one cell per named field.
///
/// The field type is the cell type (`ValueState<V>`, `BagState<V>`, `MapState<K, V>`,
/// `MultimapState<K, V>`, `SetState<T>`, `OrderedListState<T>`), and the field name is the
/// state id.
///
/// # Field attributes
///
/// - `#[state(id = "...")]` — the state id. Set it to keep the state of a running job when
///   you rename the field.
///
/// # Container attributes
///
/// - `#[beam(crate = "...")]` — path to the Beam crate; defaults to `::beam`.
#[proc_macro_derive(State, attributes(state, beam))]
pub fn derive_state(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    state::expand_state(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Derives the timer families of a `StatefulDoFn`: one family per enum variant.
///
/// A unit variant is a family without tags. A variant with one `String` field carries the
/// dynamic tag of the timer. The variant name is the family id.
///
/// # Variant attributes
///
/// - `#[timer(event_time)]` or `#[timer(processing_time)]` — the time domain. Required.
/// - `#[timer(id = "...")]` — the family id. Set it to keep the timers of a running job when
///   you rename the variant.
///
/// # Container attributes
///
/// - `#[beam(crate = "...")]` — path to the Beam crate; defaults to `::beam`.
#[proc_macro_derive(Timers, attributes(timer, beam))]
pub fn derive_timers(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    timers::expand_timers(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Derives the outputs of a `DoFn` from an enum: one output per variant, whose single field
/// is the element type. It also generates `<Enum>PCollections`, with one `PCollection` field
/// per variant, named in snake case, which applying the `DoFn` returns.
///
/// # Variant attributes
///
/// - `#[output(id = "...")]` — the output tag; defaults to the field name.
///
/// # Container attributes
///
/// - `#[beam(crate = "...")]` — path to the Beam crate; defaults to `::beam`.
#[proc_macro_derive(Outputs, attributes(output, beam))]
pub fn derive_outputs(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    outputs::expand_outputs(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
