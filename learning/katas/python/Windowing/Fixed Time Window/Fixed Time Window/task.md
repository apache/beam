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

Fixed Time Window
-----------------

Windowing subdivides a PCollection according to the timestamps of its individual elements. 
Transforms that aggregate multiple elements, such as GroupByKey and Combine, work implicitly on a 
per-window basis — they process each PCollection as a succession of multiple, finite windows, 
though the entire collection itself may be of unbounded size.

In the Beam model, any PCollection (including unbounded PCollections) can be subdivided into 
logical windows. Each element in a PCollection is assigned to one or more windows according to the 
PCollection’s windowing function, and each individual window contains a finite number of elements. 
Grouping transforms then consider each PCollection’s elements on a per-window basis. GroupByKey, 
for example, implicitly groups the elements of a PCollection by key and window.

Beam provides several windowing functions, including:

*   Fixed Time Windows
*   Sliding Time Windows
*   Per-Session Windows
*   Single Global Window

The simplest form of windowing is using fixed time windows. A fixed time window represents a 
consistent duration, non overlapping time interval in the data stream.

**Kata:** Please count the number of events that happened based on fixed window with 1-day duration.

<div class="hint">
  Use <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.window.html#apache_beam.transforms.window.FixedWindows">
  FixedWindows</a> with 1-day duration.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#fixed-time-windows">
    "Fixed time windows"</a> section for more information.
</div>
