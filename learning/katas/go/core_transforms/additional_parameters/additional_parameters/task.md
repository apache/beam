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

# Additional Parameters - Window and Timestamp

This lesson introduces the concept of windowing and timestamped PCollection elements.
Before discussing windowing, we need to distinguish bounded from unbounded data.
Bounded data is of a fixed size such as a file or database query.  Unbounded data comes
from a continuously updated source such as a subscription or stream.

A window is a view into a fixed beginning and fixed end to a set of data.  In the beam model, windowing subdivides 
a PCollection according to the timestamps of its individual elements.  This is useful
for unbounded data because it allows the model to work with fixed element sizes.  Note that windowing
is not unique to unbounded data.  The beam model windows all data whether it is bounded or unbounded.
Yet, when you read from a fixed size source such as a file, beam applies the same timestamp to all the elements.

Beam will include information about the window and timestamp to your elements in your DoFn.  All your previous
lessons' DoFn had this information provided, yet you never made use of it in your DoFn parameters.  In this 
lesson you will.  The simple toy dataset has five git commit messages and their timestamps 
from the [Apache Beam public repository](https://github.com/apache/beam).  Their timestamps have been
applied to the PCollection input to simulate an unbounded dataset.

**Kata:** This lesson challenges you to apply an hourly fixed window to a PCollection.  You are then to 
apply a ParDo to the hourly fixed windowed PCollection to produce a PCollection of a Commit struct.  The
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

