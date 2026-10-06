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

//! `#[derive(State)]`.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Ident, Type};

use crate::attrs::{ContainerOpts, parse_id, reject_duplicates};

/// Expands `#[derive(State)]`: one state cell per named field.
pub(crate) fn expand_state(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let opts = ContainerOpts::parse(input)?;
    if opts.id.is_some() {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "State has no id; set the id of a cell with #[state(id = \"...\")]",
        ));
    }
    let krate = &opts.krate;
    let name = &input.ident;
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            name,
            "State needs a struct with one named field per state cell",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(
            name,
            "State needs named fields; the field names are the state ids",
        ));
    };
    let cells = fields
        .named
        .iter()
        .map(|field| {
            let ident = field
                .ident
                .clone()
                .ok_or_else(|| syn::Error::new_spanned(field, "expected a named field"))?;
            let id =
                parse_id(&field.attrs, "state", |_| false)?.unwrap_or_else(|| ident.to_string());
            Ok((ident, id, field.ty.clone()))
        })
        .collect::<syn::Result<Vec<(Ident, String, Type)>>>()?;
    reject_duplicates(cells.iter().map(|(ident, id, _)| (id, ident)), "state")?;

    let specs = cells
        .iter()
        .map(|(_, id, ty)| quote!(<#ty as #krate::internals::StateCell>::spec(#id)));
    let binds = cells.iter().map(
        |(ident, id, ty)| quote!(#ident: <#ty as #krate::internals::StateCell>::bind(#id, binding)?),
    );
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics #krate::internals::StateCells for #name #ty_generics #where_clause {
            fn specs() -> ::std::vec::Vec<
                ::std::sync::Arc<dyn #krate::internals::AnyStateSpec>
            > {
                ::std::vec![#(#specs),*]
            }

            fn bind(
                binding: &#krate::internals::StateBinding<'_>,
            ) -> #krate::Result<Self> {
                ::core::result::Result::Ok(Self { #(#binds),* })
            }
        }
    })
}
