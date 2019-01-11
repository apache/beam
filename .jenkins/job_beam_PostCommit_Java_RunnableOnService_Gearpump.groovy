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

// This job runs the suite of RunnableOnService tests against the Gearpump
// runner.
mavenJob('beam_PostCommit_Java_RunnableOnService_Gearpump') {
  description('Runs the RunnableOnService suite on the Gearpump runner.')

  previousNames('beam_PostCommit_RunnableOnService_GearpumpLocal')

  // Set common parameters.
  common_job_properties.setTopLevelJobProperties(delegate, 'gearpump-runner')

  // Set maven parameters.
  common_job_properties.setMavenConfig(delegate)

  // Sets that this is a PostCommit job.
  // 0 5 31 2 * will run on Feb 31 (i.e. never) according to job properties.
  // This job triggers only on SCM.
  common_job_properties.setPostCommit(delegate, '0 5 31 2 *')

  // Maven goals for this job.
  goals('-B -e clean verify -am -pl runners/gearpump -DforkCount=0 -DrunnableOnServicePipelineOptions=\'[ "--runner=TestGearpumpRunner", "--streaming=false" ]\'')
}
