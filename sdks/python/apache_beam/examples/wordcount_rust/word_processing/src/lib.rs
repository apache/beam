//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use pyo3::prelude::*;

/// A Python module implemented in Rust.
#[pymodule]
mod word_processing {
    use pyo3::prelude::*;
    use regex::Regex;

    /// Builds the map of string to tuple(string, int).
    #[pyfunction]
    fn map_to_int(a: String) -> PyResult<(String, u32)> {
        Ok((a, 1))
    }

    /// Extracts individual words from a line of text.
    #[pyfunction]
    fn extract_words(a: String) -> PyResult<Vec<String>> {
        let re = Regex::new(r"[\w\']+").unwrap();
        Ok(re.find_iter(&a).map(|m| m.as_str().to_string()).collect())
    }
}
