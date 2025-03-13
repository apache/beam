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

This recreates the proto descriptor set included in this resource directory.

```bash
export PROTO_INCLUDE=<proto_include_dir>
```
Execute the following command to create the pb files, in the beam root folder:

```bash
protoc \
 -Isdks/java/extensions/protobuf/src/test/resources/ \
 -I$PROTO_INCLUDE \
 --descriptor_set_out=sdks/java/extensions/protobuf/src/test/resources/org/apache/beam/sdk/extensions/protobuf/test_option_v1.pb \
 --include_imports \
 sdks/java/extensions/protobuf/src/test/resources/test/option/v1/simple.proto
```
```bash
protoc \
 -Isdks/java/extensions/protobuf/src/test/resources/ \
 --descriptor_set_out=sdks/java/extensions/protobuf/src/test/resources/proto_byte/file_descriptor/proto_byte_utils.pb \
 sdks/java/extensions/protobuf/src/test/resources/proto_byte/proto_byte_utils.proto
```
