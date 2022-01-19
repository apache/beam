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

# Example multi-language pipelines

* Example 1 - addprefix: A python pipeline that reads a text file and attaches a prefix in Java side to each input.
* Example 2 - javacount: A Python pipeline that counts words using the Java 'Count.perElement()' transform.
* Example 3 - javadatagenerator: A Python pipeline that produces a set of strings that were generated from Java. This example demonstrates the 'JavaExternalTransform' API.

## Instructions for running the pipelines

* Start the expansion service.
  * Download the latest released 'beam-examples-multi-language' jar.
  * Run the following command
    $ java -jar beam-examples-multi-language-<version>.jar <port> --javaClassLookupAllowlistFile='*'

* Setup a Python virtual environment for Apache Beam.
  * See [here](https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python)
    for more information.

* Execute the Python pipeline.
  * In a new shell, run the corresponding Python pipelines in the './python' directory using
    a Beam runner that supports multi-language pipelines.
  * Python programs in the './python' directory contains more details on actual commands to run.

