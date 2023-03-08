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

ParDo OneToMany
---------------

**Kata:** Please write a ParDo that maps each input sentence into words tokenized by whitespace
(" ").

<div class="hint">
  You can call <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.OutputReceiver.html">
  OutputReceiver</a> multiple times in a
  <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/ParDo.html">
  ParDo</a>.
</div>

<div class="hint">
  If you're using Beam version before v2.5.0, you can call
  <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.WindowedContext.html#output-OutputT-">
  DoFn.ProcessContext.output(..)</a> multiple times in a
  <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/ParDo.html">ParDo</a>.
</div>
