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

plugins { id 'org.apache.beam.vendor-java' }

description = "Apache Beam :: Vendored Dependencies :: Calcite 1.20.0"

group = "org.apache.beam"
version = "0.2"

def calcite_version = "1.20.0"
def avatica_version = "1.16.0"
def prefix = "org.apache.beam.vendor.calcite.v1_20_0"

List<String> packagesToRelocate = [
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
]

vendorJava(
        dependencies: [
                "org.apache.calcite:calcite-core:$calcite_version",
                "org.apache.calcite:calcite-linq4j:$calcite_version",
                "org.apache.calcite.avatica:avatica-core:$avatica_version",
                library.java.protobuf_java,
                library.java.slf4j_api
        ],
        relocations: packagesToRelocate.collectEntries {
            [ (it): "${prefix}.${it}" ] + [ "jdbc:calcite:": "jdbc:beam-vendor-calcite:"]
        },
        exclusions: [
                "org/slf4j/**",
                "**/module-info.class"
        ],
        groupId: group,
        artifactId: "beam-vendor-calcite-1_20_0",
        version: version,
)
