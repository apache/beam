<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

Side Output
-----------

While ParDo always produces a main output PCollection (as the return value from apply), you can 
also have your ParDo produce any number of additional output PCollections. If you choose to have 
multiple outputs, your ParDo returns all of the output PCollections (including the main output) 
bundled together.

**Kata:** Implement additional output to your ParDo for numbers bigger than 100.

<div class="hint">
  Use <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.MultiOutputReceiver.html">
  MultiOutputReceiver</a> and
  <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/ParDo.SingleOutput.html#withOutputTags-org.apache.beam.sdk.values.TupleTag-org.apache.beam.sdk.values.TupleTagList-">
  .withOutputTags</a> to output multiple tagged-outputs in a
  <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/ParDo.html">
  ParDo.</a>
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#additional-outputs">
  "Additional outputs"</a> section for more information.
</div>
