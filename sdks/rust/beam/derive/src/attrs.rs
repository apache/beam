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

//! Attribute parsing shared by the derives.

use std::collections::HashSet;

use syn::{Attribute, DeriveInput, Ident, LitStr, Path};

/// Options parsed from container-level `#[beam(...)]` attributes.
pub(crate) struct ContainerOpts {
    pub(crate) id: Option<String>,
    pub(crate) krate: Path,
}

impl ContainerOpts {
    pub(crate) fn parse(input: &DeriveInput) -> syn::Result<Self> {
        let mut id = None;
        let mut krate: Path = syn::parse_quote!(::beam);

        for attr in input.attrs.iter().filter(|a| a.path().is_ident("beam")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("id") {
                    id = Some(meta.value()?.parse::<LitStr>()?.value());
                    Ok(())
                } else if meta.path.is_ident("crate") {
                    krate = meta.value()?.parse::<LitStr>()?.parse()?;
                    Ok(())
                } else {
                    Err(meta.error("unsupported beam container attribute"))
                }
            })?;
        }

        Ok(Self { id, krate })
    }
}

/// Reads `#[<attr>(id = "...")]` from `attrs`, plus the flags that `flag` accepts.
pub(crate) fn parse_id(
    attrs: &[Attribute],
    attr: &str,
    mut flag: impl FnMut(&Ident) -> bool,
) -> syn::Result<Option<String>> {
    let mut id = None;
    for attribute in attrs.iter().filter(|a| a.path().is_ident(attr)) {
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("id") {
                id = Some(meta.value()?.parse::<LitStr>()?.value());
                Ok(())
            } else if meta.path.get_ident().is_some_and(&mut flag) {
                Ok(())
            } else {
                Err(meta.error(format!("unsupported {attr} attribute")))
            }
        })?;
    }
    Ok(id)
}

/// Returns an error for the first id in `ids` that is not unique.
pub(crate) fn reject_duplicates<'a>(
    ids: impl IntoIterator<Item = (&'a String, &'a Ident)>,
    what: &str,
) -> syn::Result<()> {
    let mut seen = HashSet::new();
    ids.into_iter()
        .find(|(id, _)| !seen.insert(id.as_str()))
        .map_or(Ok(()), |(id, ident)| {
            Err(syn::Error::new_spanned(
                ident,
                format!("the {what} id '{id}' is used twice"),
            ))
        })
}
