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

//! The `BeamField`, `NotNullable` and `DefaultCoder` impls of `#[derive(BeamRow)]`. They do
//! not depend on the fields, so each method calls `BeamRow` or a generic function of
//! `derive_support`.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::Generics;

use super::RowDerive;

impl RowDerive<'_> {
    /// `plain` has the bounds of the struct, `row` adds the `BeamRow` bounds and `coder` the
    /// `DefaultCoder` bounds.
    pub(super) fn delegate_impls(
        &self,
        plain: &Generics,
        row: &Generics,
        coder: &Generics,
    ) -> TokenStream2 {
        let krate = self.krate;
        let beam_field = self.beam_field_impl(row);
        let not_nullable = self.impl_header(plain, &quote!(#krate::schema::NotNullable));
        let default_coder = self.default_coder_impl(coder);
        quote! {
            #beam_field
            #not_nullable {}
            #default_coder
        }
    }

    /// Each type gets its own impl: a blanket `impl<T: BeamRow> BeamField for T` conflicts
    /// with the `BeamField` impls of the primitive types.
    fn beam_field_impl(&self, generics: &Generics) -> TokenStream2 {
        let krate = self.krate;
        let support = self.support();
        let header = self.impl_header(generics, &quote!(#krate::schema::BeamField));
        quote! {
            #header {
                fn beam_field_type() -> #krate::schema::FieldType {
                    #krate::schema::FieldType::row(<Self as #krate::schema::BeamRow>::beam_schema())
                }

                fn to_field_value(&self) -> ::core::result::Result<
                    ::core::option::Option<#krate::schema::FieldValue>,
                    #krate::schema::SchemaError,
                > {
                    #support::row_to_field_value(self)
                }

                fn from_field_value(
                    value: ::core::option::Option<&#krate::schema::FieldValue>,
                ) -> ::core::result::Result<Self, #krate::schema::SchemaError> {
                    #support::row_from_field_value(value)
                }

                fn decode_null() -> ::core::result::Result<Self, #krate::coders::CoderError> {
                    #support::row_decode_null()
                }

                fn encode_field(
                    &self,
                    writer: &mut ::std::vec::Vec<u8>,
                ) -> ::core::result::Result<(), #krate::coders::CoderError> {
                    <Self as #krate::schema::BeamRow>::encode_row_bytes(self, writer)
                }

                fn decode_field(
                    reader: &mut &[u8],
                ) -> ::core::result::Result<Self, #krate::coders::CoderError> {
                    <Self as #krate::schema::BeamRow>::decode_row_bytes(reader)
                }

                fn decode_field_with_type(
                    field_type: &#krate::schema::FieldType,
                    reader: &mut &[u8],
                ) -> ::core::result::Result<Self, #krate::coders::CoderError> {
                    #support::row_decode_field_with_type(field_type, reader)
                }

                fn matches_type_info(type_info: &#krate::schema::TypeInfo) -> bool {
                    #support::row_matches_type_info::<Self>(type_info)
                }
            }
        }
    }

    fn default_coder_impl(&self, generics: &Generics) -> TokenStream2 {
        let krate = self.krate;
        let support = self.support();
        let header = self.impl_header(generics, &quote!(#krate::coders::DefaultCoder));
        quote! {
            #header {
                type Coder = #krate::coders::RowStructCoder<Self>;

                fn coder() -> Self::Coder {
                    #krate::coders::RowStructCoder::new()
                }

                fn encode_element(
                    &self,
                    writer: &mut ::std::vec::Vec<u8>,
                ) -> ::core::result::Result<(), #krate::coders::CoderError> {
                    <Self as #krate::schema::BeamRow>::encode_row_bytes(self, writer)
                }

                fn decode_element(
                    reader: &mut &[u8],
                ) -> ::core::result::Result<Self, #krate::coders::CoderError> {
                    <Self as #krate::schema::BeamRow>::decode_row_bytes(reader)
                }

                fn decode_element_with_context(
                    reader: &mut &[u8],
                    schema: ::core::option::Option<&::std::sync::Arc<#krate::schema::Schema>>,
                    field_map: ::core::option::Option<&#krate::coders::FieldMap>,
                    _state_reader: ::core::option::Option<
                        &::std::sync::Arc<dyn #krate::coders::StateStreamReader>,
                    >,
                ) -> ::core::result::Result<Self, #krate::coders::CoderError> {
                    #support::row_decode_element(reader, schema, field_map)
                }

                fn resolve_field_map(
                    schema: &#krate::schema::Schema,
                ) -> ::core::result::Result<
                    ::core::option::Option<#krate::coders::FieldMap>,
                    #krate::schema::SchemaError,
                > {
                    #support::row_resolve_field_map::<Self>(schema)
                }

                fn register_coder<__R: #krate::coders::CoderRegistry + ?Sized>(
                    registry: &__R,
                ) -> ::std::string::String {
                    #support::row_register_coder::<Self, __R>(registry)
                }
            }
        }
    }
}
