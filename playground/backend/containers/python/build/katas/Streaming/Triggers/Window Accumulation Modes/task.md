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

When you specify a trigger, you must also set the window’s accumulation mode. When a trigger
fires, it emits the current contents of the window as a pane. Since a trigger can fire multiple
times, the accumulation mode determines whether the system accumulates the window panes as the
trigger fires, or discards them.

**Kata:** Given that sample events generated with one second granularity and a fixed window of 1-day duration,
please implement an early trigger that emits the number of events count immediately after new
element is processed in accumulating mode.

<div class="hint">
  Use <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.trigger.html#apache_beam.transforms.trigger.AccumulationMode">
    <code>ACCUMULATING</code> accumulation mode</a> to set a window to accumulate the panes that are produced when the
  trigger fires.
</div>

<div class="hint">
  Use <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.trigger.html#apache_beam.transforms.trigger.AfterWatermark">
  the early parameter</a> to set early firing triggers.
</div>

<div class="hint">
  Use <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.window.html#apache_beam.transforms.window.FixedWindows">
  FixedWindows</a> with 1-day duration using
  <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.trigger.html#apache_beam.transforms.trigger.AfterWatermark">
  AfterWatermark</a> trigger.
</div>

<div class="hint">
  Set the <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html?highlight=allowed_lateness#apache_beam.transforms.core.Windowing">
  allowed lateness</a> to 0
</div>

<div class="hint">
  Use <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.CombineGlobally">
  CombineGlobally</a> and
  <code>CountCombineFn</code> to calculate the count of events.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#event-time-triggers">
    "Event time triggers"</a> section for more information.
</div>
