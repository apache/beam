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

import org.gradle.api.Plugin
import org.gradle.api.Project

/**
 * This plugin checks if build is executed on jenkins server and provides a flag to switch
 * configuration accordingly. This is done by check system environment for existence of the
 * variables ('HUDSON_HOME', 'BUILD_ID' and 'BUILD_NUMBER'). This behaviour can be overridden
 * by setting {@code -PciBuild=false} or {@code -PciBuild=true} respectively.
 *
 * <p>Example usage:
 *
 * <pre>
 * apply plugin: org.apache.beam.gradle.BeamJenkinsPlugin
 *
 * tasks.withType(SpotBugs) {
 *   reports {
 *     html.enabled = !jenkins.isCIBuild
 *     xml.enabled = jenkins.isCIBuild
 *   }
 * }
 * </pre>
 */
class BeamJenkinsPlugin implements Plugin<Project> {

  @Override
  void apply(Project project) {
    def extension = project.extensions.create('jenkins', JenkinsPluginExtension)
    extension.isCIBuild = project.findProperty("ciBuild")?.toBoolean() ?:
        // try to deduce from system env. if all variables are set, we probably on jenkins
        (System.env['HUDSON_HOME'] && System.env['BUILD_ID'] && System.env['BUILD_NUMBER'])
  }

  static class JenkinsPluginExtension {
    Boolean isCIBuild
  }
}
