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

# Windowing

This lesson introduces the concept of windowed PCollection elements.  A window is a view into a fixed beginning and
fixed end to a set of data.  In the beam model, windowing subdivides a PCollection according to the
timestamps of its individual elements.  An element can be a part of one or more windows.

A DoFn can request timestamp and windowing information about the element it is processing.  All the previous lessons
had this information available as well.  This lesson makes use of these parameters.  The simple dataset
has five git commit messages and their timestamps from the
[Apache Beam public repository](https://github.com/apache/beam).  Timestamps have been applied to this PCollection
input according to the date and time of these messages.

**Kata:** This lesson challenges you to apply an hourly fixed window to a PCollection.  You are then to
apply a ParDo to that hourly fixed windowed PCollection to produce a PCollection of a Commit struct.  The
Commit struct is provided for you.  You are encouraged to run the pipeline at cmd/main.go of this task
to visualize the windowing and timestamps.

<div class="hint">
    Use <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#ParDo">
    beam.ParDo</a>
    with a <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#hdr-DoFns">
    DoFn</a> to accomplish this lesson.
</div>

<div class="hint">
    Use <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#WindowInto">
    beam.WindowInto</a>
    with <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam/core/graph/window#NewFixedWindows">
    window.NewFixedWindows(time.Hour)</a>
    on your PCollection input to apply an hourly windowing strategy to each element.
</div>

<div class="hint">
    To access <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#Window">
    beam.Window</a>
    and <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#EventTime">
    beam.EventTime</a> in your DoFn, add the parameters in the set order.

```
func doFn(iw beam.Window, et beam.EventTime, element X) Y {
    // do something with iw, et and element to return Y
}
```
</div>

<div class="hint">
    The Commit struct provided for you has a MaxTimestampWindow property that can be set from
    <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#Window">
    beam.Window</a>'s MaxTimestamp().
</div>

<div class="hint">
    Refer to the Beam Programming Guide for additional information about
    <a href="https://beam.apache.org/documentation/programming-guide/#other-dofn-parameters">
    additional DoFn parameters</a> and
    <a href="https://beam.apache.org/documentation/programming-guide/#windowing">
    windowing</a>.
</div>

