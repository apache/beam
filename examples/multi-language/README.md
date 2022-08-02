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

This project provides examples of Apache Beam
[multi-language pipelines](https://beam.apache.org/documentation/programming-guide/#multi-language-pipelines):

* **python/addprefix** - A Python pipeline that reads a text file and attaches a prefix on the Java side to each input.
* **python/javacount** - A Python pipeline that counts words using the Java `Count.perElement()` transform.
* **python/javadatagenerator** - A Python pipeline that produces a set of strings generated from Java.
                                  This example demonstrates the `JavaExternalTransform` API.

## Instructions for running the pipelines

### 1) Start the expansion service

1. Download the latest 'beam-examples-multi-language' JAR. Starting with Apache Beam 2.36.0,
   you can find it in [the Maven Central Repository](https://search.maven.org/search?q=g:org.apache.beam).
2. Run the following command, replacing `<version>` and `<port>` with valid values:
  `java -jar beam-examples-multi-language-<version>.jar <port> --javaClassLookupAllowlistFile='*'`

### 2) Set up a Python virtual environment for Beam

1. See [the Python quickstart](https://beam.apache.org/get-started/quickstart-py/)
   for more information.

### 3) Execute the Python pipeline

1. In a new shell, run a pipeline in the **python** directory using a Beam runner that supports
   multi-language pipelines.

   The Python files contain details about the actual commands to run.

