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

//! The generic parameters and `where` clauses of the impls of `#[derive(BeamRow)]`.

use std::collections::HashSet;

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{GenericArgument, GenericParam, Generics, Ident, Path, PathArguments, Type};

use super::field::{SchemaField, SkippedField};

/// Collects type parameter identifiers and rejects lifetime or const generic parameters.
pub(super) fn collect_type_params(generics: &Generics) -> syn::Result<Vec<Ident>> {
    generics
        .params
        .iter()
        .map(|param| match param {
            GenericParam::Type(tp) => Ok(tp.ident.clone()),
            GenericParam::Lifetime(lt) => Err(syn::Error::new_spanned(
                lt,
                "BeamRow does not support lifetime parameters; decoded rows own their values",
            )),
            GenericParam::Const(c) => Err(syn::Error::new_spanned(
                c,
                "BeamRow does not support const generic parameters",
            )),
        })
        .collect()
}

/// Builds the impl generics for `BeamRow`/`BeamField` and `DefaultCoder`.
pub(super) fn build_impl_generics(
    generics: &Generics,
    type_params: &[Ident],
    schema_fields: &[SchemaField],
    skipped: &[SkippedField],
    krate: &Path,
) -> (Generics, Generics) {
    if type_params.is_empty() {
        return (generics.clone(), generics.clone());
    }
    let param_set: HashSet<&Ident> = type_params.iter().collect();
    let mut row_generics = generics.clone();
    {
        let where_clause = row_generics.make_where_clause();
        let mut seen_beam = HashSet::new();
        for f in schema_fields.iter().filter(|f| !f.is_bytes) {
            if mentions_type_param(&f.ty, &param_set) {
                let ty = &f.ty;
                if seen_beam.insert(quote!(#ty).to_string()) {
                    where_clause
                        .predicates
                        .push(syn::parse_quote!(#ty: #krate::schema::BeamField));
                }
            }
        }
        let mut seen_default = HashSet::new();
        for s in skipped {
            let ty = &s.ty;
            if mentions_type_param(ty, &param_set) && seen_default.insert(quote!(#ty).to_string()) {
                where_clause
                    .predicates
                    .push(syn::parse_quote!(#ty: ::core::default::Default));
            }
        }
    }

    let mut coder_generics = row_generics.clone();
    {
        let where_clause = coder_generics.make_where_clause();
        let mut seen_sync = HashSet::new();
        for ty in schema_fields
            .iter()
            .map(|f| &f.ty)
            .chain(skipped.iter().map(|s| &s.ty))
        {
            if mentions_type_param(ty, &param_set) && seen_sync.insert(quote!(#ty).to_string()) {
                where_clause.predicates.push(syn::parse_quote!(
                    #ty: ::core::marker::Send + ::core::marker::Sync + 'static
                ));
            }
        }
        for param in type_params {
            if !seen_sync.contains(&param.to_string()) {
                where_clause
                    .predicates
                    .push(syn::parse_quote!(#param: 'static));
            }
        }
    }

    (row_generics, coder_generics)
}

/// Returns true when `ty` references one of `params`.
fn mentions_type_param(ty: &Type, params: &HashSet<&Ident>) -> bool {
    match ty {
        Type::Path(tp) => {
            if let Some(qself) = &tp.qself
                && mentions_type_param(&qself.ty, params)
            {
                return true;
            }
            if tp.qself.is_none()
                && let Some(first) = tp.path.segments.first()
                && params.contains(&first.ident)
            {
                return true;
            }
            tp.path.segments.iter().any(|seg| match &seg.arguments {
                PathArguments::None => false,
                PathArguments::AngleBracketed(ab) => ab.args.iter().any(|arg| match arg {
                    GenericArgument::Type(inner) => mentions_type_param(inner, params),
                    GenericArgument::AssocType(assoc) => mentions_type_param(&assoc.ty, params),
                    _ => false,
                }),
                PathArguments::Parenthesized(p) => tokens_mention_param(quote!(#p), params),
            })
        }
        Type::Array(a) => mentions_type_param(&a.elem, params),
        Type::Group(g) => mentions_type_param(&g.elem, params),
        Type::Paren(p) => mentions_type_param(&p.elem, params),
        Type::Ptr(p) => mentions_type_param(&p.elem, params),
        Type::Reference(r) => mentions_type_param(&r.elem, params),
        Type::Slice(s) => mentions_type_param(&s.elem, params),
        Type::Tuple(t) => t.elems.iter().any(|elem| mentions_type_param(elem, params)),
        other => tokens_mention_param(quote!(#other), params),
    }
}

fn tokens_mention_param(tokens: TokenStream2, params: &HashSet<&Ident>) -> bool {
    tokens.into_iter().any(|tree| match tree {
        proc_macro2::TokenTree::Ident(ident) => params.contains(&ident),
        proc_macro2::TokenTree::Group(group) => tokens_mention_param(group.stream(), params),
        _ => false,
    })
}
