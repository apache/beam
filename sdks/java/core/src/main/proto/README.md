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

## Protocol Buffers in Apache Beam

This directory contains the Protocol Buffer messages used in Apache Beam.

They aren't, however, used during the Maven build process, and are included here
for completeness only. Instead, the following artifact on Maven Central contains
the binary version of the generated code from these Protocol Buffers:

    <dependency>
      <groupId>com.google.cloud.dataflow</groupId>
      <artifactId>google-cloud-dataflow-java-proto-library-all</artifactId>
      <version>LATEST</version>
    </dependency>

Please follow this process for testing changes:

* Make changes to the Protocol Buffer messages in this directory.
* Use `protoc` to generate the new code, and compile it into a new Java library.
* Install that Java library into your local Maven repository.
* Update SDK's `pom.xml` to pick up the newly installed library, instead of
downloading it from Maven Central.

Once the changes are ready for submission, please separate them into two
commits. The first commit should update the Protocol Buffer messages only. After
that, we need to update the generated artifact on Maven Central. Finally,
changes that make use of the Protocol Buffer changes may be committed.
