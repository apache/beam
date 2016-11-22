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

// Defines a job.
mavenJob('beam_PostCommit_Java_RunnableOnService_Gearpump') {
  description('Runs the RunnableOnService suite on the Gearpump runner.')

  // Set common parameters.
  common_job_properties.setTopLevelJobProperties(delegate, 'gearpump-runner')

  // Set maven paramaters.
  common_job_properties.setMavenConfig(delegate)

  // Set build triggers
  triggers {
    scm('* * * * *')
  }
  
  goals('-B -e clean verify -am -pl runners/gearpump -DforkCount=0 -DrunnableOnServicePipelineOptions=\'[ "--runner=TestGearpumpRunner", "--streaming=false" ]\'')

  publishers {
    // Notify the mailing list for each failed build.
    // mailer('commits@beam.incubator.apache.org', false, true)
  }
}

