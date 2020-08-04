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

Composite Transform
-------------------

Transforms can have a nested structure, where a complex transform performs multiple simpler 
transforms (such as more than one ParDo, Combine, GroupByKey, or even other composite transforms). 
These transforms are called composite transforms. Nesting multiple transforms inside a single 
composite transform can make your code more modular and easier to understand.

To create your own composite transform, create a subclass of the PTransform class and override the 
expand method to specify the actual processing logic. You can then use this transform just as you 
would a built-in transform from the Beam SDK. For the PTransform class type parameters, you pass 
the PCollection types that your transform takes as input, and produces as output. Within your 
PTransform subclass, you’ll need to override the expand method. The expand method is where you add 
the processing logic for the PTransform. Your override of expand must accept the appropriate type 
of input PCollection as a parameter, and specify the output PCollection as the return value.

**Kata:** Please implement a composite transform "ExtractAndMultiplyNumbers" that extracts numbers 
from comma separated line and then multiplies each number by 10.

<div class="hint">
  Refer to <a href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/PTransform.html">
  PTransform</a>.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#composite-transforms">
    "Composite transforms"</a> section for more information.
</div>
