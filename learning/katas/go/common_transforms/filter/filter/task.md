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

Filter
------

In the previous lesson we filtered numbers using a ParDo.  This lesson introduces the [filter](https://pkg.go.dev/github.com/apache/beam/sdks/go/pkg/beam/transforms/filter?tab=doc) package that contains transformations for removing pipeline elements based on various conditions.

**Kata:** Implement a filter function that filters out the odd numbers by using a method from the [filter](https://pkg.go.dev/github.com/apache/beam/sdks/go/pkg/beam/transforms/filter?tab=doc) package.

<div class="hint">
  Use <a href="https://pkg.go.dev/github.com/apache/beam/sdks/go/pkg/beam/transforms/filter?tab=doc#Exclude">
  filter.Exclude</a>.
</div>
