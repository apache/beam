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

Combine - Combine PerKey
------------------------

After creating a keyed PCollection (for example, by using a GroupByKey transform), a common pattern
is to combine the collection of values associated with each key into a single, merged value. This
pattern of a GroupByKey followed by merging the collection of values is equivalent to Combine
PerKey transform. The combine function you supply to Combine PerKey must be an associative
reduction function or a subclass of CombineFn.

**Kata:** Implement the sum of scores per player using
[Combine.perKey](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/CombineFnBase.GlobalCombineFn.html).

<div class="hint">
  Use <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/CombineFnBase.GlobalCombineFn.html">
  Combine.perKey(GlobalCombineFn)</a>.
</div>

<div class="hint">
  Extend the
  <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/Combine.BinaryCombineFn.html">
    Combine.BinaryCombineFn</a> class that counts the sum of the number.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#combining-values-in-a-keyed-pcollection">
    "Combining values in a keyed PCollection"</a> section for more information.
</div>
