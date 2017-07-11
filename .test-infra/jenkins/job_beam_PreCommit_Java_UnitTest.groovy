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
mavenJob('beam_PreCommit_Java_UnitTest') {
  description('Part of the PreCommit Pipeline. Runs Java Surefire unit tests.')

  common_job_properties.setPipelineJobProperties(delegate, 20, "Java Unit Tests")
  common_job_properties.setPipelineDownstreamJobProperties(delegate, 'beam_PreCommit_Java_Build')

  // Construct Maven goals for this job.
  profiles = [ // TODO: Are all of these necessary?
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
    'surefire:test@default-test',
    'coveralls:report', // TODO: Will this work?
    "-pl '!sdks/python'",
    '-DrepoToken=$COVERALLS_REPO_TOKEN',
    '-DpullRequest=$ghprbPullId',
  ]
  goals(args.join(' '))
}
