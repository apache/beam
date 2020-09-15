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

# Additional Outputs

In previous lessons, we mainly applied a DoFn to a ParDo that outputted a single PCollection.  Optionally, a ParDo
transform can produce zero or multiple output PCollections which we will explore in this lesson.

**Kata:** Implement additional output to your ParDo for numbers bigger than 100.

<div class="hint">
  Use <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#ParDo2">
  beam.ParDo2</a> to output two PCollections.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#additional-outputs">
  "Additional outputs"</a> section for more information.
</div>

<div class="hint">
    <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#ParDo2">
    beam.ParDo2</a> expects a DoFn that looks like the following.

```
func doFn(element X, emit1 func(Y), emit2 func(Y)) {
    // Element of type X comes from your PCollection input
    // Call emit1 to emit to the first PCollection output
    // Call emit2 to emit to the second PCollection output
}
```
</div>
