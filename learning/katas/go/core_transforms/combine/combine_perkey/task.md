<!--
  ~  Licensed to the Apache Software Foundation (ASF) under one
  ~  or more contributor license agreements.  See the NOTICE file
  ~  distributed with this work for additional information
  ~  regarding copyright ownership.  The ASF licenses this file
  ~  to you under the Apache License, Version 2.0 (the
  ~  "License"); you may not use this file except in compliance
  ~  with the License.  You may obtain a copy of the License at
  ~
  ~      http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~  Unless required by applicable law or agreed to in writing, software
  ~  distributed under the License is distributed on an "AS IS" BASIS,
  ~  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~  See the License for the specific language governing permissions and
  ~  limitations under the License.
  -->

# CombinePerKey

After creating a keyed PCollection, a common pattern  is to combine the collection of values associated with
each key into a single, merged value. The Go SDK inserts a GroupByKey and per-key Combine transform into the pipeline.

**Kata:** Implement the sum of scores per player using
[beam.CombinePerKey](https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#CombinePerKey).

<div class="hint">
  Implement the sum of scores per player using
  <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#CombinePerKey">
    beam.CombinePerKey</a>.
</div>

<div class="hint">
  Provide a function to
  <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#CombinePerKey">
    beam.CombinePerKey</a>
    that returns a sum of two numbers.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#combining-values-in-a-keyed-pcollection">
    "Combining values in a keyed PCollection"</a> section for more information.
</div>
