<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

# Composite Transform

Transforms can have a nested structure, where a complex transform performs multiple simpler
transforms (such as more than one ParDo, Combine, GroupByKey, or even other composite transforms).
These transforms are called composite transforms. Nesting multiple transforms inside a single
composite transform can make your code more modular and easier to understand.  Additionally,
scopes may be augmented with custom naming for monitoring purposes.

**Kata:** This kata has two tasks.  One is to implement a composite transform that extracts characters
from a list of strings, excludes any spaces and returns a PCollection of type KV<string, int>
associating a character with its count in the sample input.
Second is to create a sub-scope in the composite transform with the name "CountCharacters".

<div class="hint">
  Use <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#ParDo">
  beam.ParDo</a>
  with a <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#hdr-DoFns">
  DoFn</a> in your composite transform to extract non space characters from the input.
</div>

<div class="hint">
  You can use <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam/transforms/stats/#Count">
  stats.Count</a>
  to count the number of appearances of each character in the PCollection&lt;string&gt; input.
</div>

<div class="hint">
  Use the method <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#Scope.Scope">
  Scope</a> to create a sub-scope in the composite transform.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#composite-transforms">
    "Composite transforms"</a> section for more information.
</div>
