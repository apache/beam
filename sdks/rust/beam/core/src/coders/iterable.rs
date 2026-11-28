// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Runner state stream reader trait for state-backed iterables.

/// Iterator over raw byte pages from runner state.
pub type PageStream = Box<dyn Iterator<Item = Result<Vec<u8>, String>> + Send>;

/// Opens runner state streams for continuation tokens.
pub trait StateStreamReader: Send + Sync {
    /// Opens the state pages for `token`. Each page holds nested-context elements.
    fn stream_runner_pages(&self, token: &[u8]) -> Result<PageStream, String>;
}
