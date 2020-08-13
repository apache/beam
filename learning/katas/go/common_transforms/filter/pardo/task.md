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

# Filter using ParDo

This lesson introduces transformations for removing elements based on various conditions by using
a ParDo with a DoFn.  Subsequent lessons will use the package 
[filter](https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam/transforms/filter).

**Kata:** Implement a filter function that filters out the even numbers. 

<div class="hint">
  Use <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#ParDo">
  beam.ParDo</a>
  with a <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam#hdr-DoFns">
  DoFn</a>.
</div>
