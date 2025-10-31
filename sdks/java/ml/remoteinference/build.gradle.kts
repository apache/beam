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
  implementation("com.google.auto.value:auto-value:1.11.0")
  implementation("com.google.auto.value:auto-value-annotations:1.11.0")
  implementation("com.fasterxml.jackson.core:jackson-core:2.20.0")

  // testing
  testImplementation(project(":runners:direct-java"))
  testImplementation("junit:junit:4.13.2")
  testImplementation(project(":sdks:java:testing:test-utils"))
}

