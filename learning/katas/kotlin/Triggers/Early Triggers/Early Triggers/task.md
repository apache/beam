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

Early Triggers
--------------

Triggers allow Beam to emit early results, before all the data in a given window has arrived. For 
example, emitting after a certain amount of time elapses, or after a certain number of elements 
arrives.

**Kata:** Given that events are being generated every second and a fixed window of 1-day duration, 
please implement an early trigger that emits the number of events count immediately after new 
element is processed.

<div class="hint">
  Use <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/windowing/AfterWatermark.AfterWatermarkEarlyAndLate.html#withEarlyFirings-org.apache.beam.sdk.transforms.windowing.Trigger.OnceTrigger-">
  withEarlyFirings</a> to set early firing triggers.
</div>

<div class="hint">
  Use <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/windowing/FixedWindows.html">
  FixedWindows</a> with 1-day duration using
  <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/windowing/AfterWatermark.html#pastEndOfWindow--">
    AfterWatermark.pastEndOfWindow()</a> trigger.
</div>

<div class="hint">
  Set the <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/windowing/Window.html#withAllowedLateness-org.joda.time.Duration-">
  allowed lateness</a> to 0 with
  <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/windowing/Window.html#discardingFiredPanes--">
    discarding accumulation mode</a>.
</div>

<div class="hint">
  Use <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/Combine.html#globally-org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn-">
  Combine.globally</a> and
  <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/Count.html#combineFn--">
    Count.combineFn</a> to calculate the count of events.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#event-time-triggers">
    "Event time triggers"</a> section for more information.
</div>
