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

Window Accumulation Mode
------------------------

When you specify a trigger, you must also set the the window’s accumulation mode. When a trigger 
fires, it emits the current contents of the window as a pane. Since a trigger can fire multiple 
times, the accumulation mode determines whether the system accumulates the window panes as the 
trigger fires, or discards them.

**Kata:** Given that events are being generated every second and a fixed window of 1-day duration, 
please implement an early trigger that emits the number of events count immediately after new 
element is processed in accumulating mode.

<div class="hint">
  Use <a href="https://beam.apache.org/releases/javadoc/2.13.0/org/apache/beam/sdk/transforms/windowing/Window.html#accumulatingFiredPanes--">
  accumulatingFiredPanes()</a> to set a window to accumulate the panes that are produced when the
  trigger fires.
</div>

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
  allowed lateness</a> to 0.
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
