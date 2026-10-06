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

//! The fields of a `#[derive(BeamRow)]` struct: their attributes, their generated expressions
//! and their checks.

use std::collections::HashSet;

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{GenericArgument, Ident, LitInt, LitStr, Path, PathArguments, Type};

/// Options parsed from field-level `#[beam(...)]` attributes.
#[derive(Default)]
struct FieldOpts {
    rename: Option<String>,
    skip: bool,
    bytes: bool,
    encoding_position: Option<i32>,
}

impl FieldOpts {
    fn parse(field: &syn::Field) -> syn::Result<Self> {
        let mut opts = Self::default();

        for attr in field.attrs.iter().filter(|a| a.path().is_ident("beam")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("rename") {
                    opts.rename = Some(meta.value()?.parse::<LitStr>()?.value());
                } else if meta.path.is_ident("skip") {
                    opts.skip = true;
                } else if meta.path.is_ident("bytes") {
                    opts.bytes = true;
                } else if meta.path.is_ident("encoding_position") {
                    opts.encoding_position = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
                } else {
                    return Err(meta.error("unsupported beam field attribute"));
                }
                Ok(())
            })?;
        }

        Ok(opts)
    }
}

/// A field that participates in the schema, paired with its generated expressions.
pub(super) struct SchemaField {
    pub(super) ident: Ident,
    pub(super) ty: Type,
    pub(super) is_bytes: bool,
    pub(super) beam_name: String,
    pub(super) field_type: TokenStream2,
    pub(super) matches_field_type: TokenStream2,
    pub(super) nullable_const: TokenStream2,
    pub(super) is_null: TokenStream2,
    pub(super) to_value: TokenStream2,
    pub(super) encode_wire: TokenStream2,
    pub(super) decode_wire: TokenStream2,
    pub(super) decode_null: TokenStream2,
    pub(super) encoding_position: Option<i32>,
}

impl SchemaField {
    fn from_syn(
        field: &syn::Field,
        ident: Ident,
        opts: FieldOpts,
        krate: &Path,
    ) -> syn::Result<Self> {
        let ty = field.ty.clone();
        if opts.bytes {
            reject_non_vec_u8(&ty)?;
        }

        let beam_name = opts.rename.unwrap_or_else(|| ident.to_string());
        let (
            field_type,
            matches_field_type,
            nullable_const,
            is_null,
            to_value,
            encode_wire,
            decode_wire,
            decode_null,
        ) = if opts.bytes {
            (
                quote!(#krate::schema::derive_support::bytes_field_type()),
                quote!(#krate::schema::derive_support::bytes_matches_field_type),
                quote!(false),
                quote!(false),
                quote!(#krate::schema::derive_support::bytes_to_field_value(&self.#ident)?),
                quote!(#krate::schema::derive_support::bytes_encode_field(&self.#ident, writer)?),
                quote!(#krate::schema::derive_support::bytes_decode_field(reader)?),
                quote!(#krate::schema::derive_support::bytes_decode_null()?),
            )
        } else {
            (
                quote!(<#ty as #krate::schema::BeamField>::beam_field_type()),
                quote!(<#ty as #krate::schema::BeamField>::matches_field_type),
                quote!(<#ty as #krate::schema::BeamField>::NULLABLE),
                quote!(<#ty as #krate::schema::BeamField>::is_null(&self.#ident)),
                quote!(<#ty as #krate::schema::BeamField>::to_field_value(&self.#ident)?),
                quote!(<#ty as #krate::schema::BeamField>::encode_field(&self.#ident, writer)?),
                quote!(<#ty as #krate::schema::BeamField>::decode_field(reader)?),
                quote!(<#ty as #krate::schema::BeamField>::decode_null()?),
            )
        };

        Ok(Self {
            ident,
            ty,
            is_bytes: opts.bytes,
            beam_name,
            field_type,
            matches_field_type,
            nullable_const,
            is_null,
            to_value,
            encode_wire,
            decode_wire,
            decode_null,
            encoding_position: opts.encoding_position,
        })
    }

    pub(super) fn decode_slot(&self, slot_expr: &TokenStream2, krate: &Path) -> TokenStream2 {
        if self.is_bytes {
            quote!(#krate::schema::derive_support::bytes_from_field_value(#slot_expr)?)
        } else {
            let ty = &self.ty;
            quote!(<#ty as #krate::schema::BeamField>::from_field_value(#slot_expr)?)
        }
    }
}

/// A struct field annotated `#[beam(skip)]`.
pub(super) struct SkippedField {
    pub(super) ident: Ident,
    pub(super) ty: Type,
}

/// Splits struct fields into active schema fields and `#[beam(skip)]` fields.
pub(super) fn partition_fields(
    fields: &syn::punctuated::Punctuated<syn::Field, syn::Token![,]>,
    krate: &Path,
) -> syn::Result<(Vec<SchemaField>, Vec<SkippedField>)> {
    let parsed: Vec<_> = fields
        .iter()
        .map(|field| {
            let opts = FieldOpts::parse(field)?;
            let ident = field
                .ident
                .clone()
                .ok_or_else(|| syn::Error::new_spanned(field, "expected a named field"))?;
            Ok((field, ident, opts))
        })
        .collect::<syn::Result<_>>()?;

    let (skipped_entries, active_entries): (Vec<_>, Vec<_>) =
        parsed.into_iter().partition(|(_, _, opts)| opts.skip);

    let skipped = skipped_entries
        .into_iter()
        .map(|(field, ident, _)| SkippedField {
            ident,
            ty: field.ty.clone(),
        })
        .collect();

    let schema_fields = active_entries
        .into_iter()
        .map(|(field, ident, opts)| SchemaField::from_syn(field, ident, opts, krate))
        .collect::<syn::Result<Vec<_>>>()?;

    Ok((schema_fields, skipped))
}

/// Maps wire position `0..n` to declaration index `0..n`.
pub(super) fn compute_wire_order(fields: &[SchemaField]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..fields.len()).collect();
    if fields.iter().any(|f| f.encoding_position.is_some()) {
        order.sort_by_key(|&idx| fields[idx].encoding_position.unwrap_or(0));
    }
    order
}

/// Rejects two schema fields with the same Beam name, also after `#[beam(rename)]`.
pub(super) fn reject_duplicate_names(fields: &[SchemaField]) -> syn::Result<()> {
    let mut seen = HashSet::new();
    fields
        .iter()
        .find(|f| !seen.insert(f.beam_name.as_str()))
        .map_or(Ok(()), |f| {
            Err(syn::Error::new_spanned(
                &f.ident,
                format!(
                    "the Beam field name '{}' is used twice; \
                     give each field a unique #[beam(rename = \"...\")]",
                    f.beam_name
                ),
            ))
        })
}

/// Checks that `#[beam(encoding_position = N)]` is on every schema field or on none, and
/// that the positions are `0..n` in some order. The row coder reads the null bitmask and
/// the values in position order.
pub(super) fn check_encoding_positions(fields: &[SchemaField]) -> syn::Result<()> {
    if fields.iter().all(|f| f.encoding_position.is_none()) {
        return Ok(());
    }
    let count = fields.len();
    let mut seen = vec![false; count];
    fields.iter().try_for_each(|f| {
        let error = |message: String| Err(syn::Error::new_spanned(&f.ident, message));
        let Some(position) = f.encoding_position else {
            return error(
                "set #[beam(encoding_position = N)] on every field, or on none; \
                 this field has no position"
                    .to_string(),
            );
        };
        match usize::try_from(position).ok().filter(|&p| p < count) {
            None => error(format!(
                "encoding_position {position} is out of range; \
                 use 0..{count}, one position per field"
            )),
            Some(index) if seen[index] => {
                error(format!("encoding_position {position} is used twice"))
            }
            Some(index) => {
                seen[index] = true;
                Ok(())
            }
        }
    })
}

/// Rejects `#[beam(bytes)]` when the field syntax is not `Vec<u8>`.
fn reject_non_vec_u8(ty: &Type) -> syn::Result<()> {
    if is_vec_u8(ty) {
        Ok(())
    } else {
        Err(syn::Error::new_spanned(
            ty,
            "#[beam(bytes)] applies only to Vec<u8> fields; \
             use bytes::Bytes for a BYTES field of another type",
        ))
    }
}

/// Checks whether `ty` has the syntax `Vec<u8>` (with any path prefix).
fn is_vec_u8(ty: &Type) -> bool {
    fn last_segment(ty: &Type) -> Option<&syn::PathSegment> {
        match ty {
            Type::Path(path) if path.qself.is_none() => path.path.segments.last(),
            _ => None,
        }
    }

    let Some(vec) = last_segment(ty).filter(|segment| segment.ident == "Vec") else {
        return false;
    };
    let PathArguments::AngleBracketed(generics) = &vec.arguments else {
        return false;
    };
    generics.args.len() == 1
        && matches!(
            generics.args.first(),
            Some(GenericArgument::Type(element))
                if last_segment(element)
                    .is_some_and(|seg| seg.ident == "u8" && seg.arguments.is_none())
        )
}
