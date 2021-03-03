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

import org.apache.beam.gradle.Dependencies.getLibrary

plugins {
    id("org.apache.beam.vendor-java")
}

description = "Apache Beam :: Vendored Dependencies :: Calcite 1.20.0"

group = "org.apache.beam"
version = "0.2"

val calciteVersion = "1.20.0"
val avaticaVersion = "1.16.0"
val prefix = "org.apache.beam.vendor.calcite.v1_20_0"

val packagesToRelocate = listOf(
        "com.esri",
        "com.google.common",
        "com.google.thirdparty",
        "com.google.protobuf",
        "com.fasterxml",
        "com.jayway",
        "com.yahoo",
        "org.apache.calcite",
        "org.apache.commons",
        "org.apache.http",
        "org.codehaus",
        "org.pentaho",
        "org.yaml"
)

val vendorJava = project.extensions.extraProperties.get("vendorJava") as groovy.lang.Closure<*>
vendorJava(
    mapOf(
        "dependencies" to listOf(
            "org.apache.calcite:calcite-core:$calciteVersion",
            "org.apache.calcite:calcite-linq4j:$calciteVersion",
            "org.apache.calcite.avatica:avatica-core:$avaticaVersion",
            getLibrary().getValue("java").getValue("protobuf_java"),
            getLibrary().getValue("java").getValue("slf4j_api")
        ),
        "relocations" to (packagesToRelocate.map {
            (it) to "${prefix}.${it}"
        } + ("jdbc:calcite:" to "jdbc:beam-vendor-calcite:")).toMap(),
        "exclusions" to listOf(
            "org/slf4j/**",
            "**/module-info.class"
        ),
        "groupId" to group,
        "artifactId" to "beam-vendor-calcite-1_20_0",
        "version" to version
    )
)
