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

// This is the Java precommit which runs a maven install, and the current set
// of precommit tests.
mavenJob('beam_PreCommit_Java_Build') {
  description('Part of the PreCommit Pipeline. Builds Java SDK and archives artifacts.')

  // Set properties common to PreCommit Pipeline subjobs.
  common_job_properties.setPipelineJobProperties(delegate, 15, "Java Build")
  // Set properties common to PreCommit initial jobs.
  common_job_properties.setPipelineBuildJobProperties(delegate)

  configure { project ->
    // The CopyArtifact plugin doesn't support the job DSL so we have to configure it manually.
    project / 'properties' / 'hudson.plugins.copyartifact.CopyArtifactPermissionProperty' / 'projectNameList' {
      'string' "beam_*"
    }
    // The Build Discarder also doesn't support the job DSL in the right way so we have to configure it manually.
    project / 'properties' / 'jenkins.model.BuildDiscarderProperty' / 'strategy'(class:'hudson.tasks.LogRotator') {
      'daysToKeep'(-1)
      'numToKeep'(-1)
      'artifactDaysToKeep'(1)
      'artifactNumToKeep'(-1)
    }
  }

  // Construct Maven goals for this job.
  profiles = [
    'direct-runner',
    'dataflow-runner',
    'spark-runner',
    'flink-runner',
    'apex-runner'
  ]
  args = [
    '-B',
    '-e',
    "-P${profiles.join(',')}",
    'clean',
    'install',
    "-pl '!sdks/python,!sdks/java/javadoc'",
    '-DskipTests',
    '-Dcheckstyle.skip',
    '-Dmaven.javadoc.skip', // TODO: Javadoc still seems to run, figure out what also to skip.
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
