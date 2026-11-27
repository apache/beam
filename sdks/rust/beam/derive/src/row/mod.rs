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

//! `#[derive(BeamRow)]`. [`beam_row`] writes the `BeamRow` impl, which depends on the fields.
//! [`delegates`] writes the other impls, which call generic functions of the core crate.

mod beam_row;
mod delegates;
mod field;
mod generics;

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::{Data, DeriveInput, Field, Fields, Generics, Ident, Path, Token};

use crate::attrs::ContainerOpts;
use field::{
    SchemaField, SkippedField, check_encoding_positions, compute_wire_order, partition_fields,
    reject_duplicate_names,
};
use generics::{build_impl_generics, collect_type_params};

/// The struct of one `#[derive(BeamRow)]`, after the checks.
struct RowDerive<'a> {
    krate: &'a Path,
    name: &'a Ident,
    id: Option<&'a str>,
    has_type_params: bool,
    fields: Vec<SchemaField>,
    skipped: Vec<SkippedField>,
    /// The declaration index of the field at each wire position.
    wire_order: Vec<usize>,
}

impl RowDerive<'_> {
    /// The path of the functions that the generated code calls.
    fn support(&self) -> TokenStream2 {
        let krate = self.krate;
        quote!(#krate::schema::derive_support)
    }

    /// The start of an impl of `trait_path` for the struct, with the bounds of `generics`.
    fn impl_header(&self, generics: &Generics, trait_path: &TokenStream2) -> TokenStream2 {
        let name = self.name;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
        quote!(impl #impl_generics #trait_path for #name #ty_generics #where_clause)
    }

    /// The initializers of the `#[beam(skip)]` fields.
    fn skipped_inits(&self) -> Vec<TokenStream2> {
        self.skipped
            .iter()
            .map(|s| {
                let ident = &s.ident;
                quote!(#ident: ::core::default::Default::default())
            })
            .collect()
    }
}

pub(crate) fn expand_row(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let opts = ContainerOpts::parse(input)?;
    let type_params = collect_type_params(&input.generics)?;
    let (fields, skipped) = partition_fields(named_fields(input)?, &opts.krate)?;
    reject_duplicate_names(&fields)?;
    check_encoding_positions(&fields)?;

    let (row_generics, coder_generics) = build_impl_generics(
        &input.generics,
        &type_params,
        &fields,
        &skipped,
        &opts.krate,
    );
    let derive = RowDerive {
        krate: &opts.krate,
        name: &input.ident,
        id: opts.id.as_deref(),
        has_type_params: !type_params.is_empty(),
        wire_order: compute_wire_order(&fields),
        fields,
        skipped,
    };
    let beam_row = derive.beam_row_impl(&row_generics);
    let delegates = derive.delegate_impls(&input.generics, &row_generics, &coder_generics);
    Ok(quote! {
        #beam_row
        #delegates
    })
}

/// The named fields of the struct, or an error for another kind of type.
fn named_fields(input: &DeriveInput) -> syn::Result<&Punctuated<Field, Token![,]>> {
    let error = |message| Err(syn::Error::new_spanned(&input.ident, message));
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => Ok(&named.named),
            Fields::Unnamed(_) | Fields::Unit => error(
                "BeamRow requires a struct with named fields; \
                 tuple and unit structs have no field names to map to a Beam schema",
            ),
        },
        Data::Enum(_) => error(
            "BeamRow does not support enums; \
             use #[derive(BeamEnum)] for a fieldless enum",
        ),
        Data::Union(_) => error("BeamRow does not support unions"),
    }
}
