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

//! `#[derive(Register)]`.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::DeriveInput;

use crate::attrs::ContainerOpts;

/// Expands `#[derive(Register)]`: submits the type to the link-time registry.
pub(crate) fn expand_register(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let opts = ContainerOpts::parse(input)?;
    let krate = &opts.krate;
    let name = &input.ident;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "Register needs a concrete type, because the worker finds the type by name; \
             remove the derive, and the type uses the rebuild path",
        ));
    }
    let id = match &opts.id {
        Some(id) => quote!(#id),
        None => quote!(::core::concat!(
            ::core::module_path!(),
            "::",
            ::core::stringify!(#name)
        )),
    };
    Ok(quote! {
        #krate::internals::inventory::submit! {
            #krate::internals::FnRegistration {
                id: #id,
                type_id: ::core::any::TypeId::of::<#name>,
                encode: #krate::internals::encode_registered::<#name>,
                build: #krate::internals::build_registered::<#name, _>,
            }
        }
    })
}
