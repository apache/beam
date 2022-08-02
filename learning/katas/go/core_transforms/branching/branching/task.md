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

# Branching

The Beam SDK allows you to apply multiple transforms, branching an input into multiple resulting outputs.
A visual representation of this is shown below.  Two transforms are applied to a single PCollection
of database table rows.

![A branching pipeline](https://beam.apache.org/images/design-your-pipeline-multiple-pcollections.svg)

In this lesson, we will apply two different transforms to the same string input.

**Kata:** Branch out the strings to two different transforms: the first transform should reverse each word
in the PCollection while the second should convert each word to uppercase.

<div class="hint">
  Refer to the Beam Design Your Pipeline Guide
  <a href="https://beam.apache.org/documentation/pipelines/design-your-pipeline/#multiple-transforms-process-the-same-pcollection">
    "Multiple transforms process the same PCollection"</a> section for more information.
</div>
