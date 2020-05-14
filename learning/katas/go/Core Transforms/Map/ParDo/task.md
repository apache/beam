<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# ParDo

ParDo is a Beam transform for generic parallel processing. The ParDo processing paradigm is similar to the “Map” 
phase of a Map/Shuffle/Reduce-style algorithm: a ParDo transform considers each element in the input PCollection,
performs some processing function (your user code) on that element, and emits zero, one, or multiple elements to an 
output PCollection.

**Kata:** Please write a simple ParDo that maps the input element by multiplying it by 10.

<div class="hint">
  Use <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#ParDo">
  beam.ParDo</a>
  with a <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#hdr-DoFns">
  DoFn</a>.
</div>
<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#pardo">"ParDo"</a> section for
  more information.
</div>