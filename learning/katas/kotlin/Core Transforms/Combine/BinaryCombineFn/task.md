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

Combine - BinaryCombineFn
-------------------------

Combine is a Beam transform for combining collections of elements or values in your data. When you
apply a Combine transform, you must provide the function that contains the logic for combining the
elements or values. The combining function should be commutative and associative, as the function
is not necessarily invoked exactly once on all values with a given key. Because the input data
(including the value collection) may be distributed across multiple workers, the combining function
might be called multiple times to perform partial combining on subsets of the value collection.

BinaryCombineFn is used for implementing combiners that are more easily expressed as binary
operations.

**Kata:** Implement the summation of BigInteger using
[Combine.BinaryCombineFn](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/Combine.BinaryCombineFn.html).

<div class="hint">
  Extend the
  <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/Combine.BinaryCombineFn.html">
    Combine.BinaryCombineFn</a> class that counts the sum of the number.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#combine">
    "Combine"</a> section for more information.
</div>
