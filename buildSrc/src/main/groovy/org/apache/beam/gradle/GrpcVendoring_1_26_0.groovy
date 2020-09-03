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
 */
class GrpcVendoring_1_26_0 {

  static def guava_version = "26.0-jre"
  static def protobuf_version = "3.11.0"
  static def grpc_version = "1.26.0"
  static def gson_version = "2.8.6"
  static def netty_version = "4.1.51.Final"
  static def google_auth_version = "0.18.0"
  static def proto_google_common_protos_version = "1.12.0"
  static def opencensus_version = "0.24.0"
  static def perfmark_version = "0.19.0"
  static def lzma_java_version = "1.3"
  static def protobuf_javanano_version = "3.0.0-alpha-5"
  static def jzlib_version = "1.1.3"
  static def compress_lzf_version = "1.0.3"
  static def lz4_version = "1.3.0"
  static def bouncycastle_version = "1.54"
  static def conscrypt_version = "1.3.0"
  static def alpn_api_version = "1.1.2.v20150522"
  static def npn_api_version = "1.1.1.v20141010"
  static def jboss_marshalling_version = "1.4.11.Final"
  static def jboss_modules_version = "1.1.0.Beta1"

  /** Returns the list of compile time dependencies. */
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
      "io.netty:netty-transport-native-epoll:$netty_version",
      // tcnative version from https://github.com/grpc/grpc-java/blob/master/SECURITY.md#netty
      "io.netty:netty-tcnative-boringssl-static:2.0.33.Final",
      "com.google.auth:google-auth-library-credentials:$google_auth_version",
      "io.grpc:grpc-testing:$grpc_version",
      "com.google.api.grpc:proto-google-common-protos:$proto_google_common_protos_version",
      "io.opencensus:opencensus-api:$opencensus_version",
      "io.opencensus:opencensus-contrib-grpc-metrics:$opencensus_version",
      "io.perfmark:perfmark-api:$perfmark_version",
      "com.github.jponge:lzma-java:$lzma_java_version",
      "com.google.protobuf.nano:protobuf-javanano:$protobuf_javanano_version",
      "com.jcraft:jzlib:$jzlib_version",
      "com.ning:compress-lzf:$compress_lzf_version",
      "net.jpountz.lz4:lz4:$lz4_version",
      "org.bouncycastle:bcpkix-jdk15on:$bouncycastle_version",
      "org.bouncycastle:bcprov-jdk15on:$bouncycastle_version",
      "org.eclipse.jetty.alpn:alpn-api:$alpn_api_version",
      "org.eclipse.jetty.npn:npn-api:$npn_api_version",
      "org.jboss.marshalling:jboss-marshalling:$jboss_marshalling_version",
      "org.jboss.modules:jboss-modules:$jboss_modules_version"
    ]
  }

  /**
   * Returns the list of runtime time dependencies that should be exported as runtime
   * dependencies within the vendored jar.
   */
  static List<String> runtimeDependencies() {
    return [
      'com.google.errorprone:error_prone_annotations:2.3.3',
      'commons-logging:commons-logging:1.2',
      'org.apache.logging.log4j:log4j-api:2.6.2',
      'org.slf4j:slf4j-api:1.7.30',
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

    String version = "v1p26p0";
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
      "io.grpc",
      "io.netty",
      "io.opencensus",
      "io.perfmark",
      "com.google.protobuf.nano",
      "com.jcraft",
      "com.ning",
      "com.sun",
      "lzma",
      "net.jpountz",
      "org.bouncycastle",
      "org.cservenak.streams",
      "org.eclipse.jetty.alpn",
      "org.eclipse.jetty.npn",
      "org.jboss.marshalling",
      "org.jboss.modules"
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
    ]
  }

  /** Returns the list of shading exclusions. */
  static List<String> exclusions() {
    return [
      // Don't include android annotations, errorprone, checkerframework, JDK8 annotations, objenesis, junit,
      // commons-logging, log4j, slf4j and mockito in the vendored jar
      "android/annotation/**/",
      "com/google/errorprone/**",
      "com/google/instrumentation/**",
      "com/google/j2objc/annotations/**",
      "javax/annotation/**",
      "junit/**",
      "module-info.class",
      "org/apache/commons/logging/**",
      "org/apache/log/**",
      "org/apache/log4j/**",
      "org/apache/logging/log4j/**",
      "org/checkerframework/**",
      "org/codehaus/mojo/animal_sniffer/**",
      "org/conscrypt/**",
      "META-INF/native/libconscrypt**",
      "META-INF/native/conscrypt**",
      "org/hamcrest/**",
      // This Main class prevents shading (BEAM-9252)
      "org/jboss/modules/Main*",
      "org/junit/**",
      "org/mockito/**",
      "org/objenesis/**",
      "org/slf4j/**",
    ]
  }

  /**
   * Returns a closure contaning the dependencies map used for shading gRPC within the main
   * Apache Beam project.
   */
  static Object dependenciesClosure() {
    return {
      dependencies().each { compile it }
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
