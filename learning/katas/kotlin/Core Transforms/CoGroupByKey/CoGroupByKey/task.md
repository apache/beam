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

CoGroupByKey
------------

CoGroupByKey performs a relational join of two or more key/value PCollections that have the same
key type.

**Kata:** Implement a
[CoGroupByKey](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/join/CoGroupByKey.html)
transform that join words by its first alphabetical letter, and then produces the toString()
representation of the WordsAlphabet model.

<div class="hint">
  Refer to <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/join/CoGroupByKey.html">
  CoGroupByKey</a>,
  <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/values/TupleTag.html">
    TupleTag</a>, and
  <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/join/CoGbkResult.html">
    CoGbkResult</a>.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#cogroupbykey">
    "CoGroupByKey"</a> section for more information.
</div>
