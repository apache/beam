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

plugins {
  id("org.apache.beam.module")
  id("java-library")
}

description = "Apache Beam :: SDKs :: Java :: ML :: RemoteInference"

dependencies {
  // Core Beam SDK
  implementation(project(":sdks:java:core"))

  implementation("com.openai:openai-java:4.3.0")
  compileOnly("com.google.auto.value:auto-value-annotations:1.11.0")
  compileOnly("org.checkerframework:checker-qual:3.42.0")
  annotationProcessor("com.google.auto.value:auto-value:1.11.0")
  implementation("com.fasterxml.jackson.core:jackson-core:2.20.0")
  implementation("org.apache.beam:beam-vendor-guava-32_1_2-jre:0.1")
  implementation("org.slf4j:slf4j-api:2.0.9")
  implementation("org.slf4j:slf4j-simple:2.0.9")
  implementation("joda-time:joda-time:2.11.1")

  // testing
  testImplementation(project(":runners:direct-java"))
  testImplementation("org.hamcrest:hamcrest:2.2")
  testImplementation("org.mockito:mockito-core:5.8.0")
  testImplementation("org.mockito:mockito-junit-jupiter:5.8.0")
  testImplementation("junit:junit:4.13.2")
  testImplementation(project(":sdks:java:testing:test-utils"))
}

