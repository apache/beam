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
import NexmarkBuilder as Nexmark
import NoPhraseTriggeringPostCommitBuilder
import PhraseTriggeringPostCommitBuilder
import InfluxDBCredentialsHelper

import static NexmarkDatabaseProperties.nexmarkBigQueryArgs
import static NexmarkDatabaseProperties.nexmarkInfluxDBArgs

// This job runs the suite of ValidatesRunner tests against the Flink runner.
NoPhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_Flink',
    'Flink Runner Nexmark Tests', this) {
      description('Runs the Nexmark suite on the Flink runner.')

      // Set common parameters.
      commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240, true, 'beam-perf')
      InfluxDBCredentialsHelper.useCredentials(delegate)

      // Gradle goals for this job.
      steps {
        shell('echo "*** RUN NEXMARK IN BATCH MODE USING FLINK RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:nexmark:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Pnexmark.runner=":runners:flink:1.10"' +
              ' -Pnexmark.args="' +
              [
                commonJobProperties.mapToArgString(nexmarkBigQueryArgs),
                commonJobProperties.mapToArgString(nexmarkInfluxDBArgs),
                '--runner=FlinkRunner',
                '--streaming=false',
                '--suite=SMOKE',
                '--streamTimeout=60' ,
                '--manageResources=false',
                '--monitorJobs=true"'
              ].join(' '))
        }
        shell('echo "*** RUN NEXMARK IN STREAMING MODE USING FLINK RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:nexmark:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Pnexmark.runner=":runners:flink:1.10"' +
              ' -Pnexmark.args="' +
              [
                commonJobProperties.mapToArgString(nexmarkBigQueryArgs),
                commonJobProperties.mapToArgString(nexmarkInfluxDBArgs),
                '--runner=FlinkRunner',
                '--streaming=true',
                '--suite=SMOKE',
                '--streamTimeout=60' ,
                '--manageResources=false',
                '--monitorJobs=true"'
              ].join(' '))
        }
        shell('echo "*** RUN NEXMARK IN SQL BATCH MODE USING FLINK RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:nexmark:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Pnexmark.runner=":runners:flink:1.10"' +
              ' -Pnexmark.args="' +
              [
                commonJobProperties.mapToArgString(nexmarkBigQueryArgs),
                commonJobProperties.mapToArgString(nexmarkInfluxDBArgs),
                '--runner=FlinkRunner',
                '--queryLanguage=sql',
                '--streaming=false',
                '--suite=SMOKE',
                '--streamTimeout=60' ,
                '--manageResources=false"'
              ].join(' '))
        }
        shell('echo "*** RUN NEXMARK IN SQL STREAMING MODE USING FLINK RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:nexmark:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Pnexmark.runner=":runners:flink:1.10"' +
              ' -Pnexmark.args="' +
              [
                commonJobProperties.mapToArgString(nexmarkBigQueryArgs),
                commonJobProperties.mapToArgString(nexmarkInfluxDBArgs),
                '--runner=FlinkRunner',
                '--queryLanguage=sql',
                '--streaming=true',
                '--suite=SMOKE',
                '--streamTimeout=60' ,
                '--manageResources=false',
                '--monitorJobs=true"'
              ].join(' '))
        }
        shell('echo "*** RUN NEXMARK IN ZETASQL BATCH MODE USING FLINK RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:nexmark:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Pnexmark.runner=":runners:flink:1.10"' +
              ' -Pnexmark.args="' +
              [
                commonJobProperties.mapToArgString(nexmarkBigQueryArgs),
                commonJobProperties.mapToArgString(nexmarkInfluxDBArgs),
                '--runner=FlinkRunner',
                '--queryLanguage=zetasql',
                '--streaming=false',
                '--suite=SMOKE',
                '--streamTimeout=60' ,
                '--manageResources=false"'
              ].join(' '))
        }
        shell('echo "*** RUN NEXMARK IN ZETASQL STREAMING MODE USING FLINK RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:nexmark:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Pnexmark.runner=":runners:flink:1.10"' +
              ' -Pnexmark.args="' +
              [
                commonJobProperties.mapToArgString(nexmarkBigQueryArgs),
                commonJobProperties.mapToArgString(nexmarkInfluxDBArgs),
                '--runner=FlinkRunner',
                '--queryLanguage=zetasql',
                '--streaming=true',
                '--suite=SMOKE',
                '--streamTimeout=60' ,
                '--manageResources=false',
                '--monitorJobs=true"'
              ].join(' '))
        }
      }
    }

PhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_Flink',
    'Run Flink Runner Nexmark Tests', 'Flink Runner Nexmark Tests', this) {

      description('Runs the Nexmark suite on the Flink runner against a Pull Request, on demand.')

      commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240)

      def final JOB_SPECIFIC_OPTIONS = [
        'suite' : 'SMOKE',
        'streamTimeout' : 60,
      ]

      Nexmark.standardJob(delegate, Runner.FLINK, SDK.JAVA, JOB_SPECIFIC_OPTIONS, TriggeringContext.PR)
    }
