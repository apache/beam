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

//! `#[derive(Timers)]`.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Ident};

use crate::attrs::{ContainerOpts, parse_id, reject_duplicates};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Domain {
    Event,
    Processing,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    Unit,
    Tagged,
}

/// One timer family: a variant of the enum.
struct Family {
    ident: Ident,
    id: String,
    domain: Domain,
    shape: Shape,
}

impl Family {
    fn parse(variant: &syn::Variant) -> syn::Result<Self> {
        let mut domains = Vec::new();
        let id = parse_id(&variant.attrs, "timer", |flag| {
            if flag == "event_time" {
                domains.push(Domain::Event);
                true
            } else if flag == "processing_time" {
                domains.push(Domain::Processing);
                true
            } else {
                false
            }
        })?;
        let domain = match domains.as_slice() {
            [domain] => *domain,
            _ => {
                return Err(syn::Error::new_spanned(
                    variant,
                    "set the time domain once: #[timer(event_time)] or \
                     #[timer(processing_time)]",
                ));
            }
        };
        let shape = match &variant.fields {
            Fields::Unit => Shape::Unit,
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => Shape::Tagged,
            _ => {
                return Err(syn::Error::new_spanned(
                    variant,
                    "a timer variant has no fields, or one `String` field for a dynamic tag",
                ));
            }
        };
        Ok(Self {
            ident: variant.ident.clone(),
            id: id.unwrap_or_else(|| variant.ident.to_string()),
            domain,
            shape,
        })
    }
}

/// Expands `#[derive(Timers)]`: one timer family per variant.
pub(crate) fn expand_timers(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let opts = ContainerOpts::parse(input)?;
    let krate = &opts.krate;
    let name = &input.ident;
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            name,
            "Timers needs an enum with one variant per timer family",
        ));
    };
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "Timers does not support generic enums",
        ));
    }
    let families = data
        .variants
        .iter()
        .map(Family::parse)
        .collect::<syn::Result<Vec<_>>>()?;
    reject_duplicates(families.iter().map(|f| (&f.id, &f.ident)), "timer family")?;

    let domain_token = |d: Domain| match d {
        Domain::Event => quote!(#krate::internals::TimeDomain::EventTime),
        Domain::Processing => quote!(#krate::internals::TimeDomain::ProcessingTime),
    };
    let pattern = |f: &Family| {
        let ident = &f.ident;
        match f.shape {
            Shape::Tagged => quote!(Self::#ident(_)),
            Shape::Unit => quote!(Self::#ident),
        }
    };
    let specs = families.iter().map(|f| {
        let (id, domain) = (&f.id, domain_token(f.domain));
        quote!(#krate::internals::TimerFamilySpec::new(#id, #domain))
    });
    let family_arms = families.iter().map(|f| {
        let (pattern, id) = (pattern(f), &f.id);
        quote!(#pattern => #id)
    });
    let tag_arms = families.iter().map(|f| {
        let ident = &f.ident;
        match f.shape {
            Shape::Tagged => quote!(Self::#ident(tag) => tag.as_ref()),
            Shape::Unit => quote!(Self::#ident => ""),
        }
    });
    let domain_arms = families.iter().map(|f| {
        let (pattern, domain) = (pattern(f), domain_token(f.domain));
        quote!(#pattern => #domain)
    });
    let fired_arms = families.iter().map(|f| {
        let (ident, id) = (&f.ident, &f.id);
        match f.shape {
            Shape::Tagged => quote!(#id => ::core::option::Option::Some(
                Self::#ident(::core::convert::From::from(tag))
            )),
            Shape::Unit => quote!(#id => ::core::option::Option::Some(Self::#ident)),
        }
    });
    let empty = families.is_empty();
    let body = |arms: Vec<TokenStream2>| {
        if empty {
            quote!(match *self {})
        } else {
            quote!(match self { #(#arms),* })
        }
    };
    let family_body = body(family_arms.collect());
    let tag_body = body(tag_arms.collect());
    let domain_body = body(domain_arms.collect());
    Ok(quote! {
        impl #krate::internals::TimerFamilies for #name {
            fn specs() -> ::std::vec::Vec<#krate::internals::TimerFamilySpec> {
                ::std::vec![#(#specs),*]
            }

            fn family(&self) -> &'static str {
                #family_body
            }

            fn tag(&self) -> &str {
                #tag_body
            }

            fn time_domain(&self) -> #krate::internals::TimeDomain {
                #domain_body
            }

            fn from_fired(family: &str, tag: &str) -> ::core::option::Option<Self> {
                let _ = tag;
                match family {
                    #(#fired_arms,)*
                    _ => ::core::option::Option::None,
                }
            }
        }
    })
}
