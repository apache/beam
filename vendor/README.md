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

# Vendored Dependencies Release

The upgrading of the vendored dependencies should be performed in two steps:
- Firstly, we need to perform a formal release of the vendored dependency.
  The [release process](http://s.apache.org/beam-release-vendored-artifacts) of the vendored dependency
  is separate from the release of Apache Beam.
- When the release of the vendored dependency is out, we can migrate Apache Beam to use the newly released
  vendored dependency.

# How to validate the vendored dependencies

The [linkage tool](https://lists.apache.org/thread.html/eb5d95b9a33d7e32dc9bcd0f7d48ba8711d42bd7ed03b9cf0f1103f1%40%3Cdev.beam.apache.org%3E)
is useful for the vendored dependency upgrades. It reports the linkage errors across multiple Apache Beam artifact ids.

For example, when we upgrade the version of gRPC to 1.26.0 and the version of the vendored gRPC is 0.1-SNAPSHOT,
we could run the linkage tool as following:
```
./gradlew -PvendoredDependenciesOnly -Ppublishing -PjavaLinkageArtifactIds=beam-vendor-grpc-1_26_0:0.1-SNAPSHOT :checkJavaLinkage
```
