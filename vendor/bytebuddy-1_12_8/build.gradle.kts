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

plugins { id("org.apache.beam.vendor-java") }

description = "Apache Beam :: Vendored Dependencies :: ByteBuddy :: 1.12.0"

group = "org.apache.beam"
version = "0.1"

val vendorJava = project.extensions.extraProperties.get("vendorJava") as groovy.lang.Closure<*>
vendorJava(
  mapOf(
    "dependencies" to listOf("net.bytebuddy:byte-buddy:1.12.10"),
    "relocations" to mapOf(
            "net.bytebuddy" to "org.apache.beam.vendor.bytebuddy.v1_12_8.net.bytebuddy"),
    "exclusions" to listOf(
            "**/module-info.class"
    ),
    "groupId" to group,
    "artifactId" to "beam-vendor-bytebuddy-1_12_8",
    "version" to version
  )
)
