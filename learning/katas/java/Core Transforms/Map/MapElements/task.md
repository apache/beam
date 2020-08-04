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

MapElements
-----------

The Beam SDKs provide language-specific ways to simplify how you provide your DoFn implementation.

MapElements can be used to simplify a DoFn that maps an element to another element (one to one).

**Kata:** Implement a simple map function that multiplies all input elements by 5 using 
[MapElements.into(...).via(...)](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/MapElements.html).

<div class="hint">
  Use <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/MapElements.html">
  MapElements.into(...).via(...)</a>.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#lightweight-dofns">
    "Lightweight DoFns and other abstractions"</a> section for more information.
</div>
