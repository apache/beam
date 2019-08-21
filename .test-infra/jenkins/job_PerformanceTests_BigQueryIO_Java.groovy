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
import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def bqioStreamTest = [
        title        : 'BigQueryIO Streaming Performance Test Java 10 GB',
        itClass      : 'org.apache.beam.sdk.bigqueryioperftests.BigQueryIOIT',
        runner       : CommonTestProperties.Runner.DATAFLOW,
        jobProperties: [
                jobName               : 'performance-tests-bqio-java-stream-10gb' + now,
                project               : 'apache-beam-testing',
                tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                tempRoot              : 'gs://temp-storage-for-perf-tests/loadtests',
                writeMethod           : 'STREAMING_INSERTS',
                publishToBigQuery     : true,
                testBigQueryDataset   : 'beam_performance',
                testBigQueryTable     : 'bqio_write_10GB_java',
                metricsBigQueryDataset: 'beam_performance',
                metricsBigQueryTable  : 'bqio_10GB_results_java_stream',
                sourceOptions         : '\'{' +
                        '"num_records": 10485760,' +
                        '"key_size": 1,' +
                        '"value_size": 1024}\'',
                maxNumWorkers         : 5,
                numWorkers            : 5,
                autoscalingAlgorithm  : 'NONE',  // Disable autoscale the worker pool.
        ]
]

def bqioBatchTest = [
        title        : 'BigQueryIO Batch Performance Test Java 10 GB',
        itClass      : 'org.apache.beam.sdk.bigqueryioperftests.BigQueryIOIT',
        runner       : CommonTestProperties.Runner.DATAFLOW,
        jobProperties: [
                jobName               : 'performance-tests-bqio-java-stream-10gb' + now,
                project               : 'apache-beam-testing',
                tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                tempRoot              : 'gs://temp-storage-for-perf-tests/loadtests',
                writeMethod           : 'FILE_LOADS',
                publishToBigQuery     : true,
                testBigQueryDataset   : 'beam_performance',
                testBigQueryTable     : 'bqio_write_10GB_java',
                metricsBigQueryDataset: 'beam_performance',
                metricsBigQueryTable  : 'bqio_10GB_results_java_batch',
                sourceOptions         : '\'{' +
                        '"num_records": 10485760,' +
                        '"key_size": 1,' +
                        '"value_size": 1024}\'',
                maxNumWorkers         : 5,
                numWorkers            : 5,
                autoscalingAlgorithm  : 'NONE',  // Disable autoscale the worker pool.
        ]
]

def executeJob = { scope, testConfig ->
    job(testConfig.title) {
        commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)
        def testTask = ':sdks:java:io:bigquery-io-perf-tests:integrationTest'
        steps {
            gradle {
                rootBuildScriptDir(commonJobProperties.checkoutDir)
                commonJobProperties.setGradleSwitches(delegate)
                switches("--info")
                switches("-DintegrationTestPipelineOptions=\'${commonJobProperties.joinPipelineOptions(testConfig.jobProperties)}\'")
                switches("-DintegrationTestRunner=\'${testConfig.runner}\'")
                tasks("${testTask} --tests ${testConfig.itClass}")
            }
        }

    }
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_BiqQueryIO_Batch_Performance_Test_Java',
        'Run BigQueryIO Batch Performance Test Java',
        'BigQueryIO Batch Performance Test Java',
        this
) {
    executeJob(delegate, bqioBatchTest)
}

CronJobBuilder.cronJob('beam_BiqQueryIO_Batch_Performance_Test_Java', 'H 15 * * *', this) {
    executeJob(delegate, bqioBatchTest)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_BiqQueryIO_Stream_Performance_Test_Java',
        'Run BigQueryIO Streaming Performance Test Java',
        'BigQueryIO Streaming Performance Test Java',
        this
) {
    executeJob(delegate, bqioStreamTest)
}

CronJobBuilder.cronJob('beam_BiqQueryIO_Stream_Performance_Test_Java', 'H 15 * * *', this) {
    executeJob(delegate, bqioStreamTest)
}
