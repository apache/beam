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
import CommonJobProperties as common

def jobConfigs = [
  [
    title        : 'SQL BigQueryIO with push-down Batch Performance Test Java',
    triggerPhrase: 'Run SQLBigQueryIO Batch Performance Test Java',
    name      : 'beam_SQLBigQueryIO_Batch_Performance_Test_Java',
    itClass      : 'org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BigQueryIOPushDownIT',
    properties: [
      project               : 'apache-beam-testing',
      tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
      tempRoot              : 'gs://temp-storage-for-perf-tests/loadtests',
      metricsBigQueryDataset: 'beam_performance',
      metricsBigQueryTable  : 'sql_bqio_read_java_batch',
      runner                : "DataflowRunner",
      maxNumWorkers         : '5',
      numWorkers            : '5',
      autoscalingAlgorithm  : 'NONE',
    ]
  ]
]

jobConfigs.forEach { jobConfig -> createPostCommitJob(jobConfig)}

private void createPostCommitJob(jobConfig) {
  job(jobConfig.name) {
    description(jobConfig.description)
    common.setTopLevelMainJobProperties(delegate)
    common.enablePhraseTriggeringFromPullRequest(delegate, jobConfig.title, jobConfig.triggerPhrase)
    common.setAutoJob(delegate, 'H */6 * * *')
    publishers {
      archiveJunit('**/build/test-results/**/*.xml')
    }

    steps {
      gradle {
        rootBuildScriptDir(common.checkoutDir)
        common.setGradleSwitches(delegate)
        switches("--info")
        switches("-DintegrationTestPipelineOptions=\'${common.joinOptionsWithNestedJsonValues(jobConfig.properties)}\'")
        switches("-DintegrationTestRunner=dataflow")
        tasks(":sdks:java:extensions:sql:perf-tests:integrationTest --tests ${jobConfig.itClass}")
      }
    }
  }
}
