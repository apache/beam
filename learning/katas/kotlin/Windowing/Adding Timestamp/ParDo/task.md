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

Adding Timestamp - ParDo
------------------------

Bounded sources (such as a file from TextIO) do not provide timestamps for elements. If you need 
timestamps, you must add them to your PCollection’s elements.

You can assign new timestamps to the elements of a PCollection by applying a ParDo transform that 
outputs new elements with timestamps that you set.

**Kata:** Please assign each element a timestamp based on the the `Event.date`.

<div class="hint">
  Use <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/ParDo.html">
  ParDo</a>
  with <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.html">
  DoFn</a>.
</div>

<div class="hint">
  Use <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.OutputReceiver.html#outputWithTimestamp-T-org.joda.time.Instant-">
  OutputReceiver.outputWithTimestamp</a> method to assign timestamp to the element.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#adding-timestamps-to-a-pcollections-elements">
    "Adding timestamps to a PCollection’s elements"</a> section for more information.
</div>
