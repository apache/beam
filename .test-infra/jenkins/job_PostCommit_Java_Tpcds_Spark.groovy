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
import NoPhraseTriggeringPostCommitBuilder
import PhraseTriggeringPostCommitBuilder
import InfluxDBCredentialsHelper

// This job runs the Tpcds benchmark suite against the Spark runner.
NoPhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Tpcds_Spark',
    'Spark Runner Tpcds Tests', this) {
      description('Runs the Tpcds suite on the Spark runner.')

      // Set common parameters.
      commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240, true, 'beam-perf')
      InfluxDBCredentialsHelper.useCredentials(delegate)

      // Gradle goals for this job.
      steps {
        shell('echo "*** RUN TPCDS IN BATCH MODE USING SPARK 2 RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:tpcds:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Ptpcds.runner=":runners:spark:2"' +
              ' -Ptpcds.args="' +
              [
                '--dataSize=1GB',
                '--sourceType=PARQUET',
                '--dataDirectory=gs://beam-tpcds/datasets/parquet/partitioned',
                '--resultsDirectory=gs://beam-tpcds/results/',
                '--tpcParallel=1',
                '--runner=SparkRunner',
                '--queries=3'
              ].join(' '))
        }
        shell('echo "*** RUN TPCDS IN BATCH MODE USING SPARK 3 RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:tpcds:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Ptpcds.runner=":runners:spark:3"' +
              ' -Ptpcds.args="' +
              [
                '--dataSize=1GB',
                '--sourceType=PARQUET',
                '--dataDirectory=gs://beam-tpcds/datasets/parquet/partitioned',
                '--resultsDirectory=gs://beam-tpcds/results/',
                '--tpcParallel=1',
                '--runner=SparkRunner',
                '--queries=3'
              ].join(' '))
        }

        shell('echo "*** RUN TPCDS IN BATCH MODE USING SPARK 2 STRUCTURED STREAMING RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:tpcds:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Ptpcds.runner=":runners:spark:2"' +
              ' -Ptpcds.args="' +
              [
                '--dataSize=1GB',
                '--sourceType=PARQUET',
                '--dataDirectory=gs://beam-tpcds/datasets/parquet/partitioned',
                '--resultsDirectory=gs://beam-tpcds/results/',
                '--tpcParallel=1',
                '--runner=SparkRunner',
                '--queries=3'
              ].join(' '))
        }
        shell('echo "*** RUN TPCDS IN BATCH MODE USING SPARK 3 STRUCTURED STREAMING RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:tpcds:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Ptpcds.runner=":runners:spark:3"' +
              ' -Ptpcds.args="' +
              [
                '--dataSize=1GB',
                '--sourceType=PARQUET',
                '--dataDirectory=gs://beam-tpcds/datasets/parquet/partitioned',
                '--resultsDirectory=gs://beam-tpcds/results/',
                '--tpcParallel=1',
                '--runner=SparkRunner',
                '--queries=3'
              ].join(' '))
        }
      }
    }

PhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Tpcds_Spark',
    'Run Spark Runner Tpcds Tests', 'Spark Runner Tpcds Tests', this) {

      description('Runs the Tpcds suite on the Spark runner against a Pull Request, on demand.')

      // Set common parameters.
      commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240, true, 'beam-perf')
      InfluxDBCredentialsHelper.useCredentials(delegate)

      // Gradle goals for this job.
      steps {
        shell('echo "*** RUN TPCDS IN BATCH MODE USING SPARK 2 RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:tpcds:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Ptpcds.runner=":runners:spark:2"' +
              ' -Ptpcds.args="' +
              [
                '--dataSize=1GB',
                '--sourceType=PARQUET',
                '--dataDirectory=gs://beam-tpcds/datasets/parquet/partitioned',
                '--resultsDirectory=gs://beam-tpcds/results/',
                '--tpcParallel=1',
                '--runner=SparkRunner',
                '--queries=3'
              ].join(' '))
        }
        shell('echo "*** RUN TPCDS IN BATCH MODE USING SPARK 3 RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:tpcds:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Ptpcds.runner=":runners:spark:3"' +
              ' -Ptpcds.args="' +
              [
                '--dataSize=1GB',
                '--sourceType=PARQUET',
                '--dataDirectory=gs://beam-tpcds/datasets/parquet/partitioned',
                '--resultsDirectory=gs://beam-tpcds/results/',
                '--tpcParallel=1',
                '--runner=SparkRunner',
                '--queries=3'
              ].join(' '))
        }

        shell('echo "*** RUN TPCDS IN BATCH MODE USING SPARK 2 STRUCTURED STREAMING RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:tpcds:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Ptpcds.runner=":runners:spark:2"' +
              ' -Ptpcds.args="' +
              [
                '--dataSize=1GB',
                '--sourceType=PARQUET',
                '--dataDirectory=gs://beam-tpcds/datasets/parquet/partitioned',
                '--resultsDirectory=gs://beam-tpcds/results/',
                '--tpcParallel=1',
                '--runner=SparkRunner',
                '--queries=3'
              ].join(' '))
        }
        shell('echo "*** RUN TPCDS IN BATCH MODE USING SPARK 3 STRUCTURED STREAMING RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:tpcds:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Ptpcds.runner=":runners:spark:3"' +
              ' -Ptpcds.args="' +
              [
                '--dataSize=1GB',
                '--sourceType=PARQUET',
                '--dataDirectory=gs://beam-tpcds/datasets/parquet/partitioned',
                '--resultsDirectory=gs://beam-tpcds/results/',
                '--tpcParallel=1',
                '--runner=SparkRunner',
                '--queries=3'
              ].join(' '))
        }
      }
    }
