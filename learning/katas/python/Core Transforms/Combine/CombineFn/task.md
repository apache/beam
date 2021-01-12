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

Combine - CombineFn
-------------------

Combine is a Beam transform for combining collections of elements or values in your data. When you
apply a Combine transform, you must provide the function that contains the logic for combining the
elements or values. The combining function should be commutative and associative, as the function
is not necessarily invoked exactly once on all values with a given key. Because the input data
(including the value collection) may be distributed across multiple workers, the combining function
 might be called multiple times to perform partial combining on subsets of the value collection.

Complex combination operations might require you to create a subclass of CombineFn that has an
accumulation type distinct from the input/output type. You should use CombineFn if the combine
function requires a more sophisticated accumulator, must perform additional pre- or post-processing,
might change the output type, or takes the key into account.

**Kata:** Implement the average of numbers using
[Combine.CombineFn](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.CombineFn).

<div class="hint">
  Extend the
  <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.CombineFn">
    CombineFn</a> class that counts the average of the number.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#advanced-combines">
    "Advanced combinations using CombineFn"</a> section for more information.
</div>
