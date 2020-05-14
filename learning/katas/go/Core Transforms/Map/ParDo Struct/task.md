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
# ParDo - using a struct as a DoFn

In previous katas, we provided our ParDo a `func` as a DoFn.  In this example, we will explore
the use of a `struct` to in our transform.

**Kata:** Implement a simple map function that multiplies all input elements by 5.  Use a `struct` as your DoFn with
a `ProcessElement` method.

<div class="hint">
  Use <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#ParDo">
  beam.ParDo</a>
  with <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#hdr-DoFns">
  DoFn</a> as a struct with the ProcessElement method 
  <a href="https://github.com/apache/beam/blob/master/sdks/go/examples/contains/contains.go#L66">
  as in this example</a>.
</div>
