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

# Microbenchmarks for parts of the Beam SDK

To run benchmarks:

 1. Run `mvn install` in the top directory to install the SDK.

 2. Build the benchmark package:

        cd microbenchmarks
        mvn package

 3. run benchmark harness:

        java -jar target/microbenchmarks.jar

 4. (alternate to step 3)
    to run just a subset of benchmarks, pass a regular expression that
    matches the benchmarks you want to run (this can match against the class
    name, or the method name).  E.g., to run any benchmarks with
    "DoFnReflector" in the name:

        java -jar target/microbenchmarks.jar ".*DoFnReflector.*"

