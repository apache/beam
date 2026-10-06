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

//! `#[derive(BeamEnum)]`.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Data, DeriveInput, Fields};

use crate::attrs::ContainerOpts;

/// Expands `#[derive(BeamEnum)]`: variant names encode as `STRING`.
pub(crate) fn expand_enum(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let opts = ContainerOpts::parse(input)?;
    let krate = &opts.krate;
    let name = &input.ident;

    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "BeamEnum only supports enums; use #[derive(BeamRow)] for structs",
        ));
    };

    // Beam has no portable tagged-union schema type, so variants cannot carry payloads.
    let variants = data
        .variants
        .iter()
        .map(|variant| match variant.fields {
            Fields::Unit => Ok(&variant.ident),
            _ => Err(syn::Error::new_spanned(
                variant,
                "BeamEnum supports only fieldless variants; \
                 Beam has no portable tagged-union type",
            )),
        })
        .collect::<syn::Result<Vec<_>>>()?;

    // `to_field_value` and `encode_field` share the arms, so they are collected.
    let (to_arms, from_arms): (Vec<_>, Vec<_>) = variants
        .iter()
        .map(|ident| {
            let label = ident.to_string();
            (
                quote!(Self::#ident => #label),
                quote!(#label => ::core::result::Result::Ok(Self::#ident)),
            )
        })
        .unzip();
    let expected = variants
        .iter()
        .map(|ident| ident.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    Ok(quote! {
        impl #krate::schema::BeamField for #name {
            fn beam_field_type() -> #krate::schema::FieldType {
                #krate::schema::FieldType::string()
            }

            fn to_field_value(&self) -> ::core::result::Result<
                ::core::option::Option<#krate::schema::FieldValue>,
                #krate::schema::SchemaError,
            > {
                let label = match self { #(#to_arms),* };
                ::core::result::Result::Ok(::core::option::Option::Some(
                    #krate::schema::FieldValue::String(label.to_string()),
                ))
            }

            fn from_field_value(
                value: ::core::option::Option<&#krate::schema::FieldValue>,
            ) -> ::core::result::Result<Self, #krate::schema::SchemaError> {
                match value {
                    ::core::option::Option::Some(
                        #krate::schema::FieldValue::String(label)
                    ) => match label.as_str() {
                        #(#from_arms,)*
                        other => ::core::result::Result::Err(
                            #krate::schema::SchemaError::ValueTypeMismatch {
                                expected: ::std::format!(
                                    "one of [{}]", #expected
                                ),
                                actual: other.to_string(),
                            },
                        ),
                    },
                    ::core::option::Option::Some(other) => ::core::result::Result::Err(
                        #krate::schema::SchemaError::ValueTypeMismatch {
                            expected: "STRING".to_string(),
                            actual: ::std::format!("{other:?}"),
                        },
                    ),
                    ::core::option::Option::None => ::core::result::Result::Err(
                        #krate::schema::SchemaError::UnexpectedNull {
                            expected: "STRING".to_string(),
                        },
                    ),
                }
            }

            fn decode_null() -> ::core::result::Result<Self, #krate::coders::CoderError> {
                ::core::result::Result::Err(#krate::coders::CoderError::Schema(
                    #krate::schema::SchemaError::UnexpectedNull {
                        expected: "STRING".to_string(),
                    },
                ))
            }

            fn encode_field(
                &self,
                writer: &mut ::std::vec::Vec<u8>,
            ) -> ::core::result::Result<(), #krate::coders::CoderError> {
                let label = match self { #(#to_arms),* };
                #krate::schema::derive_support::encode_str_field(label, writer)
            }

            fn decode_field(
                reader: &mut &[u8],
            ) -> ::core::result::Result<Self, #krate::coders::CoderError> {
                let label = #krate::schema::derive_support::read_str_field(reader)?;
                match label {
                    #(#from_arms,)*
                    other => ::core::result::Result::Err(#krate::coders::CoderError::Schema(
                        #krate::schema::SchemaError::ValueTypeMismatch {
                            expected: ::std::format!("one of [{}]", #expected),
                            actual: other.to_string(),
                        },
                    )),
                }
            }

            fn matches_type_info(type_info: &#krate::schema::TypeInfo) -> bool {
                ::core::matches!(
                    type_info,
                    #krate::schema::TypeInfo::Atomic(#krate::schema::AtomicType::String)
                )
            }
        }

        impl #krate::schema::NotNullable for #name {}
    })
}
