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

# Assigning Timestamps

A DoFn can assign timestamps to elements of a PCollection.  We will see the use these timestamps in the next lesson
on windowing.  A simple dataset in this lesson has five git commit messages and their timestamps from the
[Apache Beam public repository](https://github.com/apache/beam).

**Kata:** Assign a timestamp to PCollection&lt;Commit&gt; elements based on the datetime of the commit message.

<div class="hint">
    Use <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#ParDo">
    beam.ParDo</a>
    with a <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#hdr-DoFns">
    DoFn</a> to accomplish this lesson.
</div>

<div class="hint">
    Use <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime#FromTime">
    mtime.FromTime</a> to assign a timestamp to the PCollection element.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#adding-timestamps-to-a-pcollections-elements">
    "Adding timestamps to a PCollection’s elements"</a> section for more information.
</div>
