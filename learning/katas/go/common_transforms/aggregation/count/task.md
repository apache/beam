<!--
  ~  Licensed to the Apache Software Foundation (ASF) under one
  ~  or more contributor license agreements.  See the NOTICE file
  ~  distributed with this work for additional information
  ~  regarding copyright ownership.  The ASF licenses this file
  ~  to you under the Apache License, Version 2.0 (the
  ~  "License"); you may not use this file except in compliance
  ~  with the License.  You may obtain a copy of the License at
  ~
  ~      http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~  Unless required by applicable law or agreed to in writing, software
  ~  distributed under the License is distributed on an "AS IS" BASIS,
  ~  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~  See the License for the specific language governing permissions and
  ~  limitations under the License.
  -->

# Aggregation - Count

In the Beam model, metrics provide some insight into the current state of a user pipeline.
In this lesson we introduce the [stats](https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam/transforms/stats)
package that contains transforms for statistical processing.

**Kata:** Count the number of elements from an input.

<div class="hint">
  Use <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam/transforms/stats#CountElms">
  stats.CountElms</a>.
</div>
