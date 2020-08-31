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

Partition
---------

Partition is a Beam transform for PCollection objects that store the same data type. Partition
splits a single PCollection into a fixed number of smaller collections.

Partition divides the elements of a PCollection according to a partitioning function that you
provide. The partitioning function contains the logic that determines how to split up the elements
of the input PCollection into each resulting partition PCollection.

**Kata:** Implement a
[Partition](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.Partition)
transform that splits a PCollection of numbers into two PCollections. The first PCollection
contains numbers greater than 100, and the second PCollection contains the remaining numbers.

<div class="hint">
  Refer to <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.Partition">
  Partition</a> to solve this problem.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#partition">
    "Partition"</a> section for more information.
</div>
