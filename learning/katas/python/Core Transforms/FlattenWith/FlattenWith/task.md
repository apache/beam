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

FlattenWith
-------

FlattenWith is a Beam transform that merges multiple PCollection objects into
a single logical PCollection. It allows for the combination of both root
PCollection-producing transforms (like Create and Read) and existing PCollections.

**Kata:** Implement a
[FlattenWith](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.FlattenWith)
transform that merges two PCollection of words into a single PCollection,
optimized for chaining operations.

<div class="hint">
  Refer to <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.FlattenWith">
  FlattenWith</a> to solve this problem.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#flattenwith">
    "FlattenWith"</a> section for more information.
</div>
