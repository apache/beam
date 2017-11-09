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

import common_job_properties

// This is the Java Jenkins job which builds artifacts for downstream jobs to consume.
mavenJob('beam_Java_Build') {
  description('Builds Beam Java SDK and archives artifacts. Meant to be run as part of a pipeline.')

  // Set standard properties for a job which is part of a pipeline.
  common_job_properties.setPipelineJobProperties(delegate, 30, "Java Build")
  // Set standard properties for a pipeline job which needs to pull from GitHub instead of an
  // upstream job.
  common_job_properties.setPipelineBuildJobProperties(delegate)

  configure { project ->
    // The CopyArtifact plugin doesn't support the job DSL so we have to configure it manually.
    project / 'properties' / 'hudson.plugins.copyartifact.CopyArtifactPermissionProperty' / 'projectNameList' {
      'string' "beam_*"
    }
    // The Build Discarder also doesn't support the job DSL in the right way so we have to configure it manually.
    // -1 indicates that a property is "infinite".
    project / 'properties' / 'jenkins.model.BuildDiscarderProperty' / 'strategy'(class:'hudson.tasks.LogRotator') {
      'daysToKeep'(-1)
      'numToKeep'(-1)
      'artifactDaysToKeep'(1)
      'artifactNumToKeep'(-1)
    }
  }

  // Construct Maven goals for this job.
  args = [
    '-B',
    '-e',
    'clean',
    'install',
    "-pl '!sdks/python,!sdks/java/javadoc'",
    '-DskipTests',
    '-Dcheckstyle.skip',
  ]
  goals(args.join(' '))

  // This job publishes artifacts so that downstream jobs can use them.
  publishers {
    archiveArtifacts {
      pattern('.repository/org/apache/beam/**/*')
      pattern('.test-infra/**/*')
      pattern('.github/**/*')
      pattern('examples/**/*')
      pattern('runners/**/*')
      pattern('sdks/**/*')
      pattern('target/**/*')
      pattern('pom.xml')
      exclude('examples/**/*.jar,runners/**/*.jar,sdks/**/*.jar,target/**/*.jar')
      onlyIfSuccessful()
      defaultExcludes()
    }
  }
}
