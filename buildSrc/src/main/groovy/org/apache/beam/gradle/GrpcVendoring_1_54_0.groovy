/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.gradle

/**
 * Utilities for working with our vendored version of gRPC.
 *
 * To update:
 * 1. Determine the set of io.grpc libraries we want to include, most likely a superset of
 *    of the previous vendored gRPC version.
 * 2. Use mvn dependency:tree and https://search.maven.org/search?q=g:io.grpc
 *    to determine dependency tree. You may need to search for optional dependencies
 *    and determine if they are needed (e.g. conscrypt).
 * 3. Validate built artifacts by running linkage tool
 *    (https://github.com/apache/beam/tree/master/vendor#how-to-validate-the-vendored-dependencies)
 *    and unit and integration tests in a PR (e.g. https://github.com/apache/beam/pull/16460,
 *    https://github.com/apache/beam/pull/16459)
 */
class GrpcVendoring_1_54_0 {
  static def grpc_version = "1.54.0"

  // See https://github.com/grpc/grpc-java/blob/v1.54.0/gradle/libs.versions.toml
  // or https://search.maven.org/search?q=g:io.grpc%201.54.0
  static def guava_version = "31.1-jre"
  static def protobuf_version = "3.21.7"
  static def gson_version = "2.9.0"
  static def google_auth_version = "1.4.0"
  static def opencensus_version = "0.31.0"
  static def conscrypt_version = "2.5.2"
  static def proto_google_common_protos_version = "2.9.0"
  static def netty_version = "4.1.87.Final"
  static def netty_tcnative_version = "2.0.56.Final"

  /** Returns the list of implementation time dependencies. */
  static List<String> dependencies() {
    return [
      "com.google.guava:guava:$guava_version",
      "com.google.protobuf:protobuf-java:$protobuf_version",
      "com.google.protobuf:protobuf-java-util:$protobuf_version",
      "com.google.code.gson:gson:$gson_version",
      "io.grpc:grpc-auth:$grpc_version",
      "io.grpc:grpc-core:$grpc_version",
      "io.grpc:grpc-context:$grpc_version",
      "io.grpc:grpc-netty:$grpc_version",
      "io.grpc:grpc-protobuf:$grpc_version",
      "io.grpc:grpc-stub:$grpc_version",
      "io.grpc:grpc-testing:$grpc_version",
      // Use a classifier to ensure we get the jar containing native libraries. In the future
      // hopefully netty releases a single jar containing native libraries for all architectures.
      "io.netty:netty-transport-native-epoll:$netty_version:linux-x86_64",
      "io.netty:netty-tcnative-boringssl-static:$netty_tcnative_version",
      "com.google.auth:google-auth-library-credentials:$google_auth_version",
      "com.google.api.grpc:proto-google-common-protos:$proto_google_common_protos_version",
      "io.opencensus:opencensus-api:$opencensus_version",
      "io.opencensus:opencensus-contrib-grpc-metrics:$opencensus_version",
    ]
  }

  /**
   * Returns the list of runtime time dependencies that should be exported as runtime
   * dependencies within the vendored jar.
   */
  static List<String> runtimeDependencies() {
    return [
      'com.google.errorprone:error_prone_annotations:2.14.0',
      // TODO(BEAM-9288): Enable relocation for conscrypt
      "org.conscrypt:conscrypt-openjdk-uber:$conscrypt_version"
    ]
  }

  /**
   * Returns the list of test dependencies.
   */
  static List<String> testDependencies() {
    return [
      'junit:junit:4.12',
    ]
  }

  static Map<String, String> relocations() {
    // The relocation paths below specifically use gRPC and the full version string as
    // the code relocation prefix. See https://lists.apache.org/thread.html/4c12db35b40a6d56e170cd6fc8bb0ac4c43a99aa3cb7dbae54176815@%3Cdev.beam.apache.org%3E
    // for further details.

    // To produce the list of necessary relocations, one needs to start with a set of target
    // packages that one wants to vendor, find all necessary transitive dependencies of that
    // set and provide relocations for each such that all necessary packages and their
    // dependencies are relocated. Any optional dependency that doesn't need relocation
    // must be excluded via an 'exclude' rule. There is additional complexity of libraries that use
    // JNI or reflection and have to be handled on case by case basis by learning whether
    // they support relocation and how would one go about doing it by reading any documentation
    // those libraries may provide. The 'validateShadedJarDoesntLeakNonOrgApacheBeamClasses'
    // ensures that there are no classes outside of the 'org.apache.beam' namespace.

    String version = "v" + grpc_version.replace(".", "p")
    String prefix = "org.apache.beam.vendor.grpc.${version}";
    List<String> packagesToRelocate = [
      // guava uses the com.google.common and com.google.thirdparty package namespaces
      "com.google.common",
      "com.google.thirdparty",
      "com.google.protobuf",
      "com.google.gson",
      "com.google.auth",
      "com.google.api",
      "com.google.cloud",
      "com.google.logging",
      "com.google.longrunning",
      "com.google.rpc",
      "com.google.type",
      "com.google.geo.type",
      "io.grpc",
      "io.netty",
      "io.opencensus",
      "io.perfmark",
    ]

    return packagesToRelocate.collectEntries {
      [ (it): "${prefix}.${it}" ]
    } + [
      // Adapted from https://github.com/grpc/grpc-java/blob/e283f70ad91f99c7fee8b31b605ef12a4f9b1690/netty/shaded/build.gradle#L41
      // We       "io.netty": "${prefix}.io.netty",have to be careful with these replacements as they must not match any
      // string in NativeLibraryLoader, else they cause corruption. Note that
      // this includes concatenation of string literals and constants.
      'META-INF/native/libnetty': "META-INF/native/liborg_apache_beam_vendor_grpc_${version}_netty",
      'META-INF/native/netty': "META-INF/native/org_apache_beam_vendor_grpc_${version}_netty",
      'META-INF/native/lib-netty': "META-INF/native/lib-org-apache-beam-vendor-grpc-${version}-netty",
    ]
  }

  /** Returns the list of shading exclusions. */
  static List<String> exclusions() {
    return [
      // Don't include in the vendored jar:
      // android annotations, errorprone, checkerframework, JDK8 annotations, objenesis, junit,
      // commons-logging, log4j, slf4j and mockito
      "android/annotation/**/",
      "com/google/errorprone/**",
      "com/google/instrumentation/**",
      "com/google/j2objc/annotations/**",
      "io/netty/handler/codec/marshalling/**",
      "io/netty/handler/codec/spdy/**",
      "io/netty/handler/codec/compression/JZlib*",
      "io/netty/handler/codec/compression/Lz4*",
      "io/netty/handler/codec/compression/Lzf*",
      "io/netty/handler/codec/compression/Lzma*",
      "io/netty/handler/codec/protobuf/Protobuf*Nano.class",
      "io/netty/util/internal/logging/CommonsLogger*",
      "io/netty/util/internal/logging/LocationAwareSlf4JLogger*",
      "io/netty/util/internal/logging/Log4JLogger*",
      "io/netty/util/internal/logging/Log4J2Logger*",
      "javax/annotation/**",
      "junit/**",
      "module-info.class",
      "org/checkerframework/**",
      "org/codehaus/mojo/animal_sniffer/**",
      "org/conscrypt/**",
      "META-INF/native/libconscrypt**",
      "META-INF/native/conscrypt**",
      "org/hamcrest/**",
      "org/junit/**",
      "org/mockito/**",
      "org/objenesis/**",
    ]
  }

  /**
   * Returns a closure contaning the dependencies map used for shading gRPC within the main
   * Apache Beam project.
   */
  static Object dependenciesClosure() {
    return {
      dependencies().each { implementation it }
      runtimeDependencies().each { shadow it }
    }
  }

  /**
   * Returns a closure with the code relocation configuration for shading gRPC within the main
   * Apache Beam project.
   */
  static Object shadowClosure() {
    return {
      relocations().each { srcNamespace, destNamespace ->
        relocate srcNamespace, destNamespace
      }
      exclusions().each { exclude it }
    }
  }
}
