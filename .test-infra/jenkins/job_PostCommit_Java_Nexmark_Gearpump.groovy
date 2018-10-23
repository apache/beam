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

import CommonJobProperties as commonJobProperties
import NexmarkBigqueryProperties
import NexmarkBuilder as Nexmark
import NoPhraseTriggeringPostCommitBuilder
import PhraseTriggeringPostCommitBuilder

// This job runs the suite of Nexmark tests against the Gearpump runner.
NoPhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_Gearpump',
        'Gearpump Runner Nexmark Tests', this) {
  description('Runs the Nexmark suite on the Gearpump runner.')

  // Set common parameters.
  commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240)

  // Gradle goals for this job.
  steps {
    shell('echo *** RUN NEXMARK IN STREAMING MODE USING GEARPUMP RUNNER ***')
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':beam-sdks-java-nexmark:run')
      commonJobProperties.setGradleSwitches(delegate)
      switches('-Pnexmark.runner=":beam-runners-gearpump"' +
              ' -Pnexmark.args="' +
              [NexmarkBigqueryProperties.nexmarkBigQueryArgs,
              '--runner=TestGearpumpRunner',
              '--streaming=true',
              '--suite=SMOKE',
              '--streamTimeout=60' ,
              '--manageResources=false',
              '--monitorJobs=true'].join(' '))
    }
  }
}

PhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_Gearpump', 'Run Gearpump Nexmark Tests',
        'Gearpump Runner Nexmark Tests', this) {
  description('Runs the Nexmark suite on the Gearpump runner against a Pull Request, on demand.')

  // Set common parameters.
  commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240)

  def final JOB_SPECIFIC_OPTIONS = [
          'suite' : 'STRESS',
          'numWorkers' : 4,
          'maxNumWorkers' : 4,
          'autoscalingAlgorithm' : 'NONE',
          'nexmarkParallel' : 16,
          'enforceEncodability' : true,
          'enforceImmutability' : true
  ]
  Nexmark.standardJob(delegate, Nexmark.Runner.GEARPUMP, JOB_SPECIFIC_OPTIONS, Nexmark.TriggeringContext.PR)
}
