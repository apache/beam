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

// This job defines the Python precommit which runs a maven install on python-sdk branch.
mavenjob('beam_PreCommit_Python_MavenInstall') {
  description('Runs an maven install on the python-sdk branch.')

  previousNames('beam_PreCommit_MavenVerify')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  common_job_properties.setTopLevelJobProperties(delegate, 'python-sdk')

  // Sets that this is a PreCommit job.
  common_job_properties.setPreCommit(delegate, ['python-sdk'], 'Jenkins: Python PreCommit')

  // Maven goals for this job.
  goals('-B -e help:effective-settings clean install')
}
