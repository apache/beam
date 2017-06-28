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

// This is the Python Jenkins job which runs a maven install, and the current set of precommit
// tests.
mavenJob('beam_Python_UnitTest') {
  description('Runs Python unit tests on a specific commit. Designed to be run by a pipeline job.')

  // Set standard properties for a job which is part of a pipeline.
  common_job_properties.setPipelineJobProperties(delegate, 25, "Python Unit Tests")
  // Set standard properties for a pipeline job which needs to pull from GitHub instead of an
  // upstream job.
  common_job_properties.setPipelineBuildJobProperties(delegate)

  // Construct Maven goals for this job.
  args = [
    '-B',
    '-e',
    'clean install',
    '-pl sdks/python',
  ]
  goals(args.join(' '))
}
