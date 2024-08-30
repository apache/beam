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

import org.gradle.api.Project
import org.gradle.api.tasks.testing.Test

import javax.inject.Inject

class IoPerformanceTestUtilities {
  abstract static class IoPerformanceTest extends Test {
    @Inject
    IoPerformanceTest(Project runningProject, String module, String testClass, Map<String,String> systemProperties){
      group = "Verification"
      description = "Runs IO Performance Test for $testClass"
      outputs.upToDateWhen { false }
      testClassesDirs = runningProject.findProject(":it:${module}").sourceSets.test.output.classesDirs
      classpath =  runningProject.sourceSets.test.runtimeClasspath + runningProject.findProject(":it:${module}").sourceSets.test.runtimeClasspath

      include "**/${testClass}.class"

      systemProperty 'exportDataset', System.getenv('exportDataset')
      systemProperty 'exportTable', System.getenv('exportTable')

      for (entry in systemProperties){
        systemProperty entry.key, entry.value
      }
    }
  }
}
