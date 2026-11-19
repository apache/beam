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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.gradle.rust

import groovy.transform.CompileStatic
import org.gradle.api.Project

/**
 * The Beam versions that each cargo command gets.
 *
 * <p>All come from gradle.properties, which release/src/main/scripts/set_version.sh bumps.
 * A plain {@code cargo} build reads the same keys, so both paths report the same version.
 */
@CompileStatic
class BeamVersions {
  /** The Gradle project version, for example X.Y.Z-SNAPSHOT. {@code -Pver} overrides it. */
  final String beamVersion
  /** The release that it becomes, for example X.Y.Z. {@code -PbeamReleaseVersion} overrides it. */
  final String releaseVersion
  /** The SDK version in binaries and container tags, for example X.Y.Z.dev. */
  final String sdkVersion

  BeamVersions(Project project) {
    beamVersion = (project.findProperty('ver') ?: project.version).toString()
    releaseVersion = (project.findProperty('beamReleaseVersion')
        ?: beamVersion.replace('-SNAPSHOT', '')).toString()
    sdkVersion = project.property('sdk_version').toString()
  }

  /** The environment variables that carry the versions to cargo. */
  Map<String, String> cargoEnv() {
    return [
      BEAM_SDK_VERSION    : sdkVersion,
      BEAM_RELEASE_VERSION: releaseVersion,
      BEAM_VERSION        : beamVersion,
    ]
  }
}
