/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

import java.util.Properties

pluginManagement {
    val parentProperties = java.util.Properties().apply {
        val file = file("../gradle.properties")
        if (file.exists()) {
            file.inputStream().use { load(it) }
        }
    }

    val mavenCentralMirrorUrl = parentProperties.getProperty("mavenCentralMirrorUrl")
    val isCi = System.getenv("GITHUB_ACTIONS") != null || System.getenv("JENKINS_HOME") != null
    val useMirror = isCi && !mavenCentralMirrorUrl.isNullOrBlank()

    if (useMirror) {
        logger.lifecycle("Running in CI. Mirroring Maven Central repositories via Google Maven Mirror for buildSrc plugins.")
    }

    repositories {
        if (useMirror) {
            maven { url = uri(mavenCentralMirrorUrl!!) }
        }
        gradlePluginPortal()
    }
}
