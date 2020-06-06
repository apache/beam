<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Testing in Apache Beam

Now that you have a simple pipeline up and running, let's talk about testing in Apache Beam using the Golang SDK.

Testing your pipeline is a particularly important step in developing an effective data processing solution. The indirect nature of the Beam model, in which your user code constructs a pipeline graph to be executed remotely, can make debugging-failed runs a non-trivial task. Often it is faster and simpler to perform local unit testing on your pipeline code than to debug a pipeline’s remote execution.

You can read more about testing on the Apache Beam website:
https://beam.apache.org/documentation/pipelines/test-your-pipeline/.

**Kata:** Develop test case assertion that checks whether the PCollection elements are equal to the given input from task.HelloBeam.

<div class="hint">
  <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam/testing/passert#Equals">passert.Equals</a>
  from the package passert verifies the given collection has the same values as the given values, under coder equality.
  The values can be provided as a single PCollection.
</div>

<div class="hint">
  <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam/testing/ptest#Run">ptest.Run</a>
  from the package ptest runs a pipeline for testing.  The semantics of the pipeline is expected to be verified
  through passert.
</div>