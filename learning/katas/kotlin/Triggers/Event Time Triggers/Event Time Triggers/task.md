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

Event Time Triggers
-------------------

When collecting and grouping data into windows, Beam uses triggers to determine when to emit the
aggregated results of each window (referred to as a pane). If you use Beam’s default windowing
configuration and default trigger, Beam outputs the aggregated result when it estimates all data
has arrived, and discards all subsequent data for that window.

You can set triggers for your PCollections to change this default behavior. Beam provides a number
of pre-built triggers that you can set:

*   Event time triggers
*   Processing time triggers
*   Data-driven triggers
*   Composite triggers

Event time triggers operate on the event time, as indicated by the timestamp on each data element.
Beam’s default trigger is event time-based.

The AfterWatermark trigger operates on event time. The AfterWatermark trigger emits the contents
of a window after the watermark passes the end of the window, based on the timestamps attached to
the data elements. The watermark is a global progress metric, and is Beam’s notion of input
completeness within your pipeline at any given point. AfterWatermark.pastEndOfWindow() only fires
when the watermark passes the end of the window.

**Kata:** Given that events are being generated every second, please implement a trigger that emits
the number of events count within a fixed window of 5-second duration.

<div class="hint">
  Use <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/windowing/FixedWindows.html">
  FixedWindows</a> with 5-second duration using
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
