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

## Linkage Tool
The [linkage tool](https://lists.apache.org/thread.html/eb5d95b9a33d7e32dc9bcd0f7d48ba8711d42bd7ed03b9cf0f1103f1%40%3Cdev.beam.apache.org%3E)
is useful for the vendored dependency upgrades. It reports the linkage errors across multiple Apache Beam artifact ids.

For example, when we upgrade the version of gRPC to 1.54.0 and the version of the vendored gRPC is 0.1-SNAPSHOT,
we could run the linkage tool as following:

```
$ ./gradlew -p vendor/grpc-1_60_1 publishMavenJavaPublicationToMavenLocal -Ppublishing -PvendoredDependenciesOnly
$ ./gradlew -PvendoredDependenciesOnly -Ppublishing -PjavaLinkageArtifactIds=beam-vendor-grpc-1_60_1:0.1-SNAPSHOT :checkJavaLinkage
```

### Known Linkage Errors in the Vendored gRPC Dependencies

It's expected that the task outputs some linkage errors.
While the `checkJavaLinkage` task does not retrieve optional dependencies to avoid bloated
dependency trees, Netty (one of gRPC dependencies) has various optional features through optional
dependencies.
Therefore the task outputs the linkage errors on the references to missing classes in the optional
dependencies when applied for the vendored gRPC artifact.

As long as Beam's use of gRPC does not touch these optional Netty features or the classes are
available at runtime, it's fine to have the
references to the missing classes. Here are the known linkage errors:

- References to `org.junit.*`: `io.grpc.testing.GrpcCleanupRule` and `io.grpc.testing.GrpcServerRule`
  uses JUnit classes, which are present when we run Beam's tests.
- References from `io.netty.handler.ssl`: Netty users can choose SSL implementation based
  on the platform ([Netty documentation](https://netty.io/wiki/forked-tomcat-native.html#wiki-h2-4)).
  Beam's vendored gRPC uses `netty-tcnative-boringssl-static`, which contains the static libraries
  for all supported OS architectures (x86_64 and aarch64).
  The `io.netty.handler.ssl` package has classes that have references to missing classes in other
  unused optional SSL implementations.
- References from `io.netty.handler.codec.compression`: Beam does not use the optional dependencies
  for compression algorithms (brotli, jzlib, lzma, lzf, and zstd) through Netty's features.
- References to `com.google.protobuf.nano` and `org.jboss.marshalling`: Beam does not use the
  optional serialization algorithms.
- References from `io.netty.util.internal.logging`: Netty's logging framework can choose available
  loggers at runtime. The logging implementations are optional dependencies and thus are not needed
  to be included in the vendored artifact. Slf4j-api is available at Beam's runtime.
- References to `reactor.blockhound`: When enabled, Netty's BlockHound integration can detect
  unexpected blocking calls. Beam does not use it.

## Create testing PR against new artifacts

Once you've verified using the linkage tool, you can test new artifacts by running unit and integration tests against a PR.

Example PRs:
- Updating gRPC version (large) https://github.com/apache/beam/pull/16460
- Testing updated gRPC version (large) https://github.com/apache/beam/pull/22595
- Updating protobuf for calcite (minor version update): https://github.com/apache/beam/pull/16476

Steps:

1. Generate new artifact files with `publishMavenJavaPublicationToMavenLocal` and
   copy to the `tempLib` folder in Beam:

```
./gradlew -p vendor/grpc-1_60_1 publishMavenJavaPublicationToMavenLocal -Ppublishing -PvendoredDependenciesOnly

mkdir -p tempLib/org/apache/beam

# Copy files (jar/poms/metadata) to your beam repository
cp -R ~/.m2/repository/org/apache/beam/beam-vendor-grpc-1_60_1` \
      tempLib/org/apache/beam/
```

2. Add the folder to the expected project repositories:

```
repositories {
    maven { url "${project.rootDir}/tempLib" }
    maven {
      ...
    }
}
```

3. Migrate all references from the old dependency to the new dependency, including imports if needed.

4. Commit any added or changed files and create a PR to run unit and integration tests on. This can be a draft PR, as you will not merge this PR.
