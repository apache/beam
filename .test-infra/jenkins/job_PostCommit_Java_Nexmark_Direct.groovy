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
import CommonTestProperties.Runner
import CommonTestProperties.SDK
import CommonTestProperties.TriggeringContext
import NexmarkBigqueryProperties
import NexmarkBuilder as Nexmark
import NoPhraseTriggeringPostCommitBuilder
import PhraseTriggeringPostCommitBuilder

NoPhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_Direct',
        'Direct Runner Nexmark Tests', this) {
  description('Runs the Nexmark suite on the Direct runner.')

  // Set common parameters.
  commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240, true, 'beam-perf')

  // Gradle goals for this job.
  steps {
    shell('echo *** RUN NEXMARK IN BATCH MODE USING DIRECT RUNNER ***')
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':sdks:java:testing:nexmark:run')
      commonJobProperties.setGradleSwitches(delegate)
      switches('-Pnexmark.runner=":runners:direct-java"' +
              ' -Pnexmark.args="' +
              [NexmarkBigqueryProperties.nexmarkBigQueryArgs,
              '--runner=DirectRunner',
              '--streaming=false',
              '--suite=SMOKE',
              '--manageResources=false',
              '--monitorJobs=true',
              '--enforceEncodability=true',
              '--enforceImmutability=true"'].join(' '))
    }
    shell('echo *** RUN NEXMARK IN STREAMING MODE USING DIRECT RUNNER ***')
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':sdks:java:testing:nexmark:run')
      commonJobProperties.setGradleSwitches(delegate)
      switches('-Pnexmark.runner=":runners:direct-java"' +
              ' -Pnexmark.args="' +
              [NexmarkBigqueryProperties.nexmarkBigQueryArgs,
              '--runner=DirectRunner',
              '--streaming=true',
              '--suite=SMOKE',
              '--manageResources=false',
              '--monitorJobs=true',
              '--enforceEncodability=true',
              '--enforceImmutability=true"'].join(' '))
    }
    shell('echo *** RUN NEXMARK IN SQL BATCH MODE USING DIRECT RUNNER ***')
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':sdks:java:testing:nexmark:run')
      commonJobProperties.setGradleSwitches(delegate)
      switches('-Pnexmark.runner=":runners:direct-java"' +
              ' -Pnexmark.args="' +
              [NexmarkBigqueryProperties.nexmarkBigQueryArgs,
              '--runner=DirectRunner',
              '--queryLanguage=sql',
              '--streaming=false',
              '--suite=SMOKE',
              '--manageResources=false',
              '--monitorJobs=true',
              '--enforceEncodability=true',
              '--enforceImmutability=true"'].join(' '))
    }
    shell('echo *** RUN NEXMARK IN SQL STREAMING MODE USING DIRECT RUNNER ***')
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':sdks:java:testing:nexmark:run')
      commonJobProperties.setGradleSwitches(delegate)
      switches('-Pnexmark.runner=":runners:direct-java"' +
              ' -Pnexmark.args="' +
              [NexmarkBigqueryProperties.nexmarkBigQueryArgs,
              '--runner=DirectRunner',
              '--queryLanguage=sql',
              '--streaming=true',
              '--suite=SMOKE',
              '--manageResources=false',
              '--monitorJobs=true',
              '--enforceEncodability=true',
              '--enforceImmutability=true"'].join(' '))
    }
  }
}

PhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_Direct',
        'Run Direct Runner Nexmark Tests', 'Direct Runner Nexmark Tests', this) {

  description('Runs the Nexmark suite on the Direct runner against a Pull Request, on demand.')

  commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240)

  def final JOB_SPECIFIC_OPTIONS = [
          'suite' : 'SMOKE',
          'enforceEncodability' : true,
          'enforceImmutability' : true
  ]
  Nexmark.standardJob(delegate, Runner.DIRECT, SDK.JAVA, JOB_SPECIFIC_OPTIONS, TriggeringContext.PR)
}
