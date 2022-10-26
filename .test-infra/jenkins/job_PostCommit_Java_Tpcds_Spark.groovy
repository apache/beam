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

import static TpcdsDatabaseProperties.tpcdsBigQueryArgs
import static TpcdsDatabaseProperties.tpcdsInfluxDBArgs
import static TpcdsDatabaseProperties.tpcdsQueriesArg

// This job runs the Tpcds benchmark suite against the Spark runner.
NoPhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Tpcds_Spark',
    'Spark Runner Tpcds Tests', this) {
      description('Runs the Tpcds suite on the Spark runner.')

      // Set common parameters.
      commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240, true, 'beam-perf')
      InfluxDBCredentialsHelper.useCredentials(delegate)

      // Gradle goals for this job.
      steps {
        shell('echo "*** RUN TPC-DS USING SPARK 3 RDD RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:tpcds:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Ptpcds.runner=":runners:spark:3"' +
              ' -Ptpcds.args="' +
              [
                commonJobProperties.mapToArgString(tpcdsBigQueryArgs),
                commonJobProperties.mapToArgString(tpcdsInfluxDBArgs),
                '--runner=SparkRunner',
                '--dataSize=1GB',
                '--sourceType=PARQUET',
                '--dataDirectory=gs://beam-tpcds/datasets/parquet/nonpartitioned',
                '--resultsDirectory=gs://beam-tpcds/results/spark3-rdd/',
                '--tpcParallel=1',
                '--queries=' + tpcdsQueriesArg
              ].join(' '))
        }
        shell('echo "*** RUN TPC-DS USING SPARK 3 DATASET RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:tpcds:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Ptpcds.runner=":runners:spark:3"' +
              ' -Ptpcds.args="' +
              [
                commonJobProperties.mapToArgString(tpcdsBigQueryArgs),
                commonJobProperties.mapToArgString(tpcdsInfluxDBArgs),
                '--runner=SparkStructuredStreamingRunner',
                '--dataSize=1GB',
                '--sourceType=PARQUET',
                '--dataDirectory=gs://beam-tpcds/datasets/parquet/nonpartitioned',
                '--resultsDirectory=gs://beam-tpcds/results/spark3-dataset/',
                '--tpcParallel=1',
                '--queries=' + tpcdsQueriesArg
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
        shell('echo "*** RUN TPC-DS USING SPARK 3 RDD RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:tpcds:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Ptpcds.runner=":runners:spark:3"' +
              ' -Ptpcds.args="' +
              [
                commonJobProperties.mapToArgString(tpcdsBigQueryArgs),
                commonJobProperties.mapToArgString(tpcdsInfluxDBArgs),
                '--runner=SparkRunner',
                '--dataSize=1GB',
                '--sourceType=PARQUET',
                '--dataDirectory=gs://beam-tpcds/datasets/parquet/nonpartitioned',
                '--resultsDirectory=gs://beam-tpcds/results/spark3-rdd/',
                '--tpcParallel=1',
                '--queries=' + tpcdsQueriesArg
              ].join(' '))
        }
        shell('echo "*** RUN TPC-DS USING SPARK 3 DATASET RUNNER ***"')
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':sdks:java:testing:tpcds:run')
          commonJobProperties.setGradleSwitches(delegate)
          switches('-Ptpcds.runner=":runners:spark:3"' +
              ' -Ptpcds.args="' +
              [
                commonJobProperties.mapToArgString(tpcdsBigQueryArgs),
                commonJobProperties.mapToArgString(tpcdsInfluxDBArgs),
                '--runner=SparkStructuredStreamingRunner',
                '--dataSize=1GB',
                '--sourceType=PARQUET',
                '--dataDirectory=gs://beam-tpcds/datasets/parquet/nonpartitioned',
                '--resultsDirectory=gs://beam-tpcds/results/spark3-dataset/',
                '--tpcParallel=1',
                '--queries=' + tpcdsQueriesArg
              ].join(' '))
        }
      }
    }
