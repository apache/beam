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

FlatMapElements
---------------

The Beam SDKs provide language-specific ways to simplify how you provide your DoFn implementation.

FlatMap can be used to simplify DoFn that maps an element to multiple elements (one to many).

**Kata:** Implement a function that maps each input sentence into words tokenized by 
whitespace (" ") using 
[FlatMap](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.FlatMap).

<div class="hint">
  Use <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.FlatMap">
  FlatMap</a> with a lambda.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#lightweight-dofns">
  "Lightweight DoFns and other abstractions"</a> section for more information.
</div>
