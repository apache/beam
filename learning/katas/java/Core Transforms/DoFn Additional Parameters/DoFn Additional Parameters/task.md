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

DoFn Additional Parameters
--------------------------

In addition to the element and the OutputReceiver, Beam will populate other parameters to your
DoFn’s @ProcessElement method. Any combination of these parameters can be added to your process
method in any order.

*   **Timestamp**: To access the timestamp of an input element, add a parameter annotated with
@Timestamp of type Instant
*   **Window**: To access the window an input element falls into, add a parameter of the type of
the window used for the input PCollection.
*   **PaneInfo**: When triggers are used, Beam provides a PaneInfo object that contains information
about the current firing. Using PaneInfo you can determine whether this is an early or a late
firing, and how many times this window has already fired for this key.
*   **PipelineOptions**: The PipelineOptions for the current pipeline can always be accessed in a
process method by adding it as a parameter.

Refer to the Beam Programming Guide
["Accessing additional parameters in your DoFn"](https://beam.apache.org/documentation/programming-guide/#other-dofn-parameters)
section for more information.
