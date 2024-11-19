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
class GrpcVendoring_1_60_1 {
  static def grpc_version = "1.60.1"

  // See https://github.com/grpc/grpc-java/blob/v1.60.1/gradle/libs.versions.toml
  // or https://search.maven.org/search?q=io.grpc%201.60.1
  static def guava_version = "32.0.1-jre"
  static def protobuf_version = "4.28.1"
  static def gson_version = "2.10.1"
  static def google_auth_version = "1.4.0"
  static def opencensus_version = "0.31.1"
  static def conscrypt_version = "2.5.2"
  static def proto_google_common_protos_version = "2.22.0"

  /** Returns the list of implementation time dependencies. */
  static List<String> dependencies() {
    return [
      "com.google.guava:guava:$guava_version",
      "com.google.protobuf:protobuf-java:$protobuf_version",
      "com.google.protobuf:protobuf-java-util:$protobuf_version",
      "com.google.code.gson:gson:$gson_version",
      "io.grpc:grpc-alts:$grpc_version",
      "io.grpc:grpc-auth:$grpc_version",
      "io.grpc:grpc-context:$grpc_version",
      "io.grpc:grpc-core:$grpc_version",
      "io.grpc:grpc-netty-shaded:$grpc_version",
      "io.grpc:grpc-protobuf:$grpc_version",
      "io.grpc:grpc-services:$grpc_version",
      "io.grpc:grpc-stub:$grpc_version",
      "io.grpc:grpc-testing:$grpc_version",
      "io.grpc:grpc-util:$grpc_version",
      "com.google.auth:google-auth-library-credentials:$google_auth_version",
      "com.google.api.grpc:proto-google-common-protos:$proto_google_common_protos_version",
      "io.opencensus:opencensus-api:$opencensus_version",
      "io.opencensus:opencensus-contrib-grpc-metrics:$opencensus_version",
    ]
  }

  /**
   * Returns the list of dependencies that should be exported as runtime
   * dependencies within the vendored jar.
   */
  static List<String> runtimeDependencies() {
    return [
      'com.google.auto.value:auto-value-annotations:1.8.2',
      'com.google.errorprone:error_prone_annotations:2.20.0',
      // transient dependencies of grpc-alts->google-auth-library-oauth2-http->google-http-client:
      'org.apache.httpcomponents:httpclient:4.5.13',
      'org.apache.httpcomponents:httpcore:4.4.15',
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
    String prefix = "org.apache.beam.vendor.grpc.${version}"
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
      "io.opencensus",
      "io.perfmark",
    ]

    return packagesToRelocate.collectEntries {
      [ (it): "${prefix}.${it}" ]
    } + [
      // Redirect io.grpc.netty.shaded to top.
      // To keep namespace consistency before switching from io.grpc:grpc-netty.
      "io.grpc.netty.shaded": "${prefix}",
    ] + [
      // Adapted from https://github.com/grpc/grpc-java/blob/e283f70ad91f99c7fee8b31b605ef12a4f9b1690/netty/shaded/build.gradle#L41
      // We have to be careful with these replacements as they must not match any
      // string in NativeLibraryLoader, else they cause corruption. Note that
      // this includes concatenation of string literals and constants.
      'META-INF/native/io_grpc_netty_shaded_netty': "META-INF/native/org_apache_beam_vendor_grpc_${version}_netty",
      'META-INF/native/libio_grpc_netty_shaded_netty': "META-INF/native/liborg_apache_beam_vendor_grpc_${version}_netty",
    ]
  }

  static Map<String, List<String>> relocationExclusions() {
    // sub-package excluded from relocation
    return [
      "io.grpc": ["io.grpc.netty.shaded.**"],
    ]
  }

  /** Returns the list of shading exclusions. */
  static List<String> exclusions() {
    return [
      // Don't include in the vendored jar:
      // android annotations, autovalue annotations, errorprone, checkerframework, JDK8 annotations, objenesis, junit,
      // apache commons, log4j, slf4j and mockito
      "android/annotation/**/",
      "com/google/auto/value/**",
      "com/google/errorprone/**",
      "com/google/instrumentation/**",
      "com/google/j2objc/annotations/**",
      "io/grpc/netty/shaded/io/netty/handler/codec/marshalling/**",
      "io/grpc/netty/shaded/io/netty/handler/codec/spdy/**",
      "io/grpc/netty/shaded/io/netty/handler/codec/compression/JZlib*",
      "io/grpc/netty/shaded/io/netty/handler/codec/compression/Lz4*",
      "io/grpc/netty/shaded/io/netty/handler/codec/compression/Lzf*",
      "io/grpc/netty/shaded/io/netty/handler/codec/compression/Lzma*",
      "io/grpc/netty/shaded/io/netty/handler/codec/protobuf/Protobuf*Nano.class",
      "io/grpc/netty/shaded/io/netty/util/internal/logging/CommonsLogger*",
      "io/grpc/netty/shaded/io/netty/util/internal/logging/LocationAwareSlf4JLogger*",
      "io/grpc/netty/shaded/io/netty/util/internal/logging/Log4JLogger*",
      "io/grpc/netty/shaded/io/netty/util/internal/logging/Log4J2Logger*",
      "javax/annotation/**",
      "junit/**",
      "module-info.class",
      "org/apache/commons/logging/**",
      "org/apache/commons/codec/**",
      "org/apache/http/**",
      "org/checkerframework/**",
      "org/codehaus/mojo/animal_sniffer/**",
      "org/conscrypt/**",
      "META-INF/native/libconscrypt**",
      "META-INF/native/conscrypt**",
      "org/hamcrest/**",
      "org/junit/**",
      "org/mockito/**",
      "org/objenesis/**",
      // proto source files
      "google/**/*.proto",
      "grpc/**/*.proto",
    ]
  }

  /**
   * Returns a closure containing the dependencies map used for shading gRPC within the main
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
    def relocExclusions = relocationExclusions()
    return {
      relocations().each { srcNamespace, destNamespace ->
        relocate(srcNamespace, destNamespace) {
          if (relocExclusions.containsKey(srcNamespace)) {
            relocExclusions.get(srcNamespace).each { toExclude ->
              exclude toExclude
            }
          }
        }
      }
      exclusions().each { exclude it }
    }
  }
}
