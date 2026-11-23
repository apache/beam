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

//! The `BeamRow` impl of `#[derive(BeamRow)]`: one function for each generated method.

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::Generics;

use super::RowDerive;
use super::field::SchemaField;

impl RowDerive<'_> {
    pub(super) fn beam_row_impl(&self, generics: &Generics) -> TokenStream2 {
        let krate = self.krate;
        let header = self.impl_header(generics, &quote!(#krate::schema::BeamRow));
        let methods = [
            self.emit_beam_schema(),
            self.emit_to_row(),
            self.emit_from_row(),
            self.emit_matches_schema(),
            self.emit_from_row_mapped(),
            self.emit_encode_row_bytes(),
            self.emit_decode_row_bytes(),
            self.emit_decode_row_bytes_mapped(),
        ];
        quote! {
            #header {
                #(#methods)*
            }
        }
    }

    /// A type without type parameters builds its schema once, in a static.
    fn emit_beam_schema(&self) -> TokenStream2 {
        let krate = self.krate;
        let id = match self.id {
            Some(id) => quote!(::core::option::Option::Some(#id.to_string())),
            None => quote!(::core::option::Option::None),
        };
        let field_decls = self.fields.iter().map(|f| {
            let beam_name = &f.beam_name;
            let field_type = &f.field_type;
            match f.encoding_position {
                Some(pos) => quote! {
                    #krate::schema::Field::new(#beam_name, #field_type)
                        .with_encoding_position(#pos)
                },
                None => quote!(#krate::schema::Field::new(#beam_name, #field_type)),
            }
        });
        let build_schema = quote! {
            let mut schema = #krate::schema::Schema::new(::std::vec![
                #(#field_decls),*
            ]);
            schema.id = #id;
            ::std::sync::Arc::new(schema)
        };
        let body = if self.has_type_params {
            build_schema
        } else {
            quote! {
                static SCHEMA: ::std::sync::OnceLock<
                    ::std::sync::Arc<#krate::schema::Schema>
                > = ::std::sync::OnceLock::new();

                ::std::sync::Arc::clone(SCHEMA.get_or_init(|| {
                    #build_schema
                }))
            }
        };
        quote! {
            fn beam_schema() -> ::std::sync::Arc<#krate::schema::Schema> {
                #body
            }
        }
    }

    fn emit_to_row(&self) -> TokenStream2 {
        let krate = self.krate;
        let to_values = self.fields.iter().map(|f| &f.to_value);
        quote! {
            fn to_row(&self) -> ::core::result::Result<
                #krate::schema::Row,
                #krate::schema::SchemaError,
            > {
                let values = ::std::vec![#(#to_values),*];
                #krate::schema::Row::new(
                    <Self as #krate::schema::BeamRow>::beam_schema(),
                    values,
                )
            }
        }
    }

    /// Reads the fields by position when the row has the field names of the type in order.
    /// Else it reads them by name.
    fn emit_from_row(&self) -> TokenStream2 {
        let krate = self.krate;
        let support = self.support();
        let num_fields = self.fields.len();
        let skipped_inits = self.skipped_inits();
        let same_name_checks = self.fields.iter().enumerate().map(|(index, f)| {
            let beam_name = &f.beam_name;
            quote!(&& __fields[#index].name == #beam_name)
        });
        let positional_inits = self.fields.iter().enumerate().map(|(index, f)| {
            let ident = &f.ident;
            let val = f.decode_slot(&quote!(#support::row_slot(__values, #index)?), krate);
            quote!(#ident: #val)
        });
        let by_name_inits = self.fields.iter().map(|f| {
            let beam_name = &f.beam_name;
            self.mapped_init(f, &quote!(row.schema().field_index(#beam_name)))
        });
        quote! {
            fn from_row(row: &#krate::schema::Row) -> ::core::result::Result<
                Self,
                #krate::schema::SchemaError,
            > {
                let __fields = &row.schema().fields;
                let __values = row.values();
                if __fields.len() == #num_fields #(#same_name_checks)* {
                    ::core::result::Result::Ok(Self {
                        #(#positional_inits,)*
                        #(#skipped_inits,)*
                    })
                } else {
                    ::core::result::Result::Ok(Self {
                        #(#by_name_inits,)*
                        #(#skipped_inits,)*
                    })
                }
            }
        }
    }

    /// The initializer of `f` from the row value at `writer_index`, an `Option<usize>`
    /// expression that is `None` if the row does not have the field.
    fn mapped_init(&self, f: &SchemaField, writer_index: &TokenStream2) -> TokenStream2 {
        let support = self.support();
        let SchemaField {
            ident,
            beam_name,
            nullable_const,
            ..
        } = f;
        let slot = quote! {
            #support::mapped_slot(__values, #writer_index, #nullable_const, #beam_name)?
        };
        let val = f.decode_slot(&slot, self.krate);
        quote!(#ident: #val)
    }

    fn emit_matches_schema(&self) -> TokenStream2 {
        let krate = self.krate;
        let num_fields = self.fields.len();
        let positions_set = self.fields.iter().any(|f| f.encoding_position.is_some());
        let checks = self.fields.iter().enumerate().map(|(index, f)| {
            let beam_name = &f.beam_name;
            let matches_ft = &f.matches_field_type;
            let pos_check = f.encoding_position.map(|pos| {
                quote!(&& schema.fields[#index].encoding_position == ::core::option::Option::Some(#pos))
            });
            quote! {
                && schema.fields[#index].name == #beam_name
                #pos_check
                && #matches_ft(&schema.fields[#index].field_type)
            }
        });
        quote! {
            fn matches_schema(schema: &#krate::schema::Schema) -> bool {
                schema.fields.len() == #num_fields
                    && schema.encoding_positions_set == #positions_set
                    #(#checks)*
            }
        }
    }

    fn emit_from_row_mapped(&self) -> TokenStream2 {
        let krate = self.krate;
        let skipped_inits = self.skipped_inits();
        let mapped_inits =
            self.fields.iter().enumerate().map(|(index, f)| {
                self.mapped_init(f, &quote!(indices.get(#index).copied().flatten()))
            });
        quote! {
            fn from_row_mapped(
                row: &#krate::schema::Row,
                indices: &[::core::option::Option<usize>],
            ) -> ::core::result::Result<Self, #krate::schema::SchemaError> {
                let __values = row.values();
                ::core::result::Result::Ok(Self {
                    #(#mapped_inits,)*
                    #(#skipped_inits,)*
                })
            }
        }
    }

    /// The fields of the struct in wire order, each with its wire position.
    fn wire_fields(&self) -> impl Iterator<Item = (usize, usize, &SchemaField)> {
        self.wire_order
            .iter()
            .enumerate()
            .map(|(pos, &idx)| (pos, idx, &self.fields[idx]))
    }

    fn emit_encode_row_bytes(&self) -> TokenStream2 {
        let krate = self.krate;
        let support = self.support();
        let num_fields = self.fields.len();
        let null_flags = self.wire_fields().map(|(_, _, f)| &f.is_null);
        // A `#[beam(bytes)]` field is never null, so it has no check.
        let encodes = self.wire_fields().map(|(pos, _, f)| {
            let encode_wire = &f.encode_wire;
            if f.is_bytes {
                quote!(#encode_wire;)
            } else {
                quote! {
                    if !__nulls[#pos] {
                        #encode_wire;
                    }
                }
            }
        });
        quote! {
            fn encode_row_bytes(
                &self,
                writer: &mut ::std::vec::Vec<u8>,
            ) -> ::core::result::Result<(), #krate::coders::CoderError> {
                let __nulls: [bool; #num_fields] = [#(#null_flags),*];
                #support::write_row_header(#num_fields, |__position| __nulls[__position], writer);
                #(#encodes)*
                ::core::result::Result::Ok(())
            }
        }
    }

    fn emit_decode_row_bytes(&self) -> TokenStream2 {
        let krate = self.krate;
        let support = self.support();
        let num_fields = self.fields.len();
        let skipped_inits = self.skipped_inits();
        let decodes = self.wire_fields().map(|(pos, idx, f)| {
            let var = format_ident!("__field_{idx}");
            let decode_null = &f.decode_null;
            let decode_wire = &f.decode_wire;
            quote! {
                let #var = if #support::bit_is_null(__null_bits, #pos) {
                    #decode_null
                } else {
                    #decode_wire
                };
            }
        });
        let struct_inits = self.fields.iter().enumerate().map(|(idx, f)| {
            let ident = &f.ident;
            let var = format_ident!("__field_{idx}");
            quote!(#ident: #var)
        });
        quote! {
            fn decode_row_bytes(
                reader: &mut &[u8],
            ) -> ::core::result::Result<Self, #krate::coders::CoderError> {
                let __null_bits = #support::read_row_header(#num_fields, reader)?;
                #(#decodes)*
                ::core::result::Result::Ok(Self {
                    #(#struct_inits,)*
                    #(#skipped_inits,)*
                })
            }
        }
    }

    /// Decodes each writer field into an `Option` slot of its reader field, then builds the
    /// struct from the slots.
    fn emit_decode_row_bytes_mapped(&self) -> TokenStream2 {
        let krate = self.krate;
        let support = self.support();
        let skipped_inits = self.skipped_inits();
        let slots = (0..self.fields.len()).map(|idx| {
            let var = format_ident!("__field_{idx}");
            quote!(let mut #var = ::core::option::Option::None;)
        });
        let dispatch = self.mapped_dispatch();
        let finish_inits = self.fields.iter().enumerate().map(|(idx, f)| {
            let ident = &f.ident;
            let var = format_ident!("__field_{idx}");
            let beam_name = &f.beam_name;
            let writer_idx = quote!(indices.get(#idx).copied().flatten());
            let val = if f.is_bytes {
                quote!(#support::finish_mapped_bytes_field(#var, #writer_idx, #beam_name)?)
            } else {
                quote!(#support::finish_mapped_field(#var, #writer_idx, #beam_name)?)
            };
            quote!(#ident: #val)
        });
        quote! {
            fn decode_row_bytes_mapped(
                writer: &::std::sync::Arc<#krate::schema::Schema>,
                indices: &[::core::option::Option<usize>],
                reader: &mut &[u8],
            ) -> ::core::result::Result<Self, #krate::coders::CoderError> {
                #(#slots)*
                #support::decode_mapped_fields(
                    writer,
                    indices,
                    reader,
                    |__reader_index, __field_type, __reader| {
                        #dispatch
                        ::core::result::Result::Ok(())
                    },
                )?;
                ::core::result::Result::Ok(Self {
                    #(#finish_inits,)*
                    #(#skipped_inits,)*
                })
            }
        }
    }

    /// The statement that decodes the writer field `__reader_index` into its slot.
    fn mapped_dispatch(&self) -> TokenStream2 {
        let krate = self.krate;
        let support = self.support();
        let decodes: Vec<TokenStream2> = self
            .fields
            .iter()
            .enumerate()
            .map(|(idx, f)| {
                let var = format_ident!("__field_{idx}");
                let decode = if f.is_bytes {
                    quote!(#support::bytes_decode_matched_field(__field_type, __reader)?)
                } else {
                    let ty = &f.ty;
                    quote! {
                        <#ty as #krate::schema::BeamField>::decode_field_with_type(
                            __field_type,
                            __reader,
                        )?
                    }
                };
                quote!(#var = ::core::option::Option::Some(#decode);)
            })
            .collect();
        match decodes.as_slice() {
            // No reader field: the caller skips each writer field.
            [] => quote!(let _ = (__reader_index, __field_type, __reader);),
            // One reader field needs no `match`.
            [only] => quote! {
                if __reader_index == 0 {
                    #only
                }
            },
            all => {
                let indices = 0..all.len();
                quote! {
                    match __reader_index {
                        #(#indices => { #all })*
                        _ => {}
                    }
                }
            }
        }
    }
}
