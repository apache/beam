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
import CommonTestProperties.TriggeringContext
import NexmarkBigqueryProperties
import NexmarkBuilder as Nexmark
import NoPhraseTriggeringPostCommitBuilder
import PhraseTriggeringPostCommitBuilder

// This job runs the suite of ValidatesRunner tests against the Spark runner.
NoPhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_Spark',
        'Spark Runner Nexmark Tests', this) {
  description('Runs the Nexmark suite on the Spark runner.')

  // Set common parameters.
  commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240, true, 'beam-perf')

  // Gradle goals for this job.
  steps {
    shell('echo *** RUN NEXMARK IN BATCH MODE USING SPARK RUNNER ***')
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':beam-sdks-java-nexmark:run')
      commonJobProperties.setGradleSwitches(delegate)
      switches('-Pnexmark.runner=":beam-runners-spark"' +
              ' -Pnexmark.args="' +
              [NexmarkBigqueryProperties.nexmarkBigQueryArgs,
              '--runner=SparkRunner',
              '--streaming=false',
              '--suite=SMOKE',
              '--streamTimeout=60' ,
              '--manageResources=false',
              '--monitorJobs=true'].join(' '))
    }
    shell('echo *** RUN NEXMARK SQL IN BATCH MODE USING SPARK RUNNER ***')
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':beam-sdks-java-nexmark:run')
      commonJobProperties.setGradleSwitches(delegate)
      switches('-Pnexmark.runner=":beam-runners-spark"' +
              ' -Pnexmark.args="' +
              [NexmarkBigqueryProperties.nexmarkBigQueryArgs,
              '--runner=SparkRunner',
              '--queryLanguage=sql',
              '--streaming=false',
              '--suite=SMOKE',
              '--streamTimeout=60' ,
              '--manageResources=false',
              '--monitorJobs=true'].join(' '))
    }
  }
}

PhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_Spark',
        'Run Spark Runner Nexmark Tests', 'Spark Runner Nexmark Tests', this) {

  description('Runs the Nexmark suite on the Spark runner against a Pull Request, on demand.')

  commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240)

  def final JOB_SPECIFIC_OPTIONS = [
          'suite'        : 'SMOKE',
          'streamTimeout': 60
  ]

  // Spark doesn't run streaming jobs, therefore run only batch variants.
  Nexmark.batchOnlyJob(delegate, JOB_SPECIFIC_OPTIONS, TriggeringContext.PR)
}
