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

//! Coders for the standard Beam wire format, which the runner and the SDK harness use to
//! exchange PCollection elements, window metadata and KV records.

mod iterable;
mod standard;
mod traits;

pub use iterable::*;
pub use standard::*;
pub use traits::*;

pub const URN_BYTES: &str = "beam:coder:bytes:v1";
pub const URN_BOOL: &str = "beam:coder:bool:v1";
pub const URN_STRING_UTF8: &str = "beam:coder:string_utf8:v1";
pub const URN_VARINT: &str = "beam:coder:varint:v1";
pub const URN_DOUBLE: &str = "beam:coder:double:v1";
