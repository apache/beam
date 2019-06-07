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

def testConfigurations = [
        [
                jobName           : 'beam_PerformanceTests_BigQueryIOStorageReadIT',
                jobDescription    : 'Runs PerfKit tests for BigQueryIOStorageReadIT',
                itClass           : 'org.apache.beam.sdk.io.gcp.bigquery.BigQueryIOStorageReadIT',
                bqTable           : 'beam_performance.bigqueryiostoragereadit_pkb_results',
                prCommitStatusName: 'Java BigQueryIO Storage Read Performance Test',
                prTriggerPhrase   : 'Run Java BigQueryIO Storage Read Performance Test',
                extraPipelineArgs : [
                        inputTable: 'big_query_storage.storage_read_1T',
                        numRecords: '11110839000'
                ]
        ]
]

for (testConfiguration in testConfigurations) {
    create_gcp_performance_test_job(testConfiguration)
}


private void create_gcp_performance_test_job(testConfiguration) {

    // This job runs the GCP IO performance tests on PerfKit benchmarker.
    job(testConfiguration.jobName) {
        description(testConfiguration.jobDescription)

        // Set default Beam job properties.
        commonJobProperties.setTopLevelMainJobProperties(delegate)

        // Allows triggering this build against pull requests.
        commonJobProperties.enablePhraseTriggeringFromPullRequest(
                delegate,
                testConfiguration.prCommitStatusName,
                testConfiguration.prTriggerPhrase)

        // Run job in postcommit every 6 hours, don't trigger every push, and
        // don't email individual contributors.
        commonJobProperties.setAutoJob(
                delegate,
                'H */6 * * *')

        def pipelineOptions = [
                project       : 'apache-beam-testing',
                tempRoot      : 'gs://temp-storage-for-perf-tests',
                filenamePrefix: "gs://temp-storage-for-perf-tests/${testConfiguration.jobName}/\${BUILD_ID}/",
        ]

        if (testConfiguration.containsKey('extraPipelineArgs')) {
            pipelineOptions << testConfiguration.extraPipelineArgs
        }

        def argMap = [
                benchmarks     : 'beam_integration_benchmark',
                // I assume this is in seconds?
                beam_it_timeout: '3600',
                beam_prebuilt  : 'false',
                beam_sdk       : 'java',
                beam_it_module : 'sdks/java/io/google-cloud-platform',
                beam_it_class  : testConfiguration.itClass,
                beam_it_options: commonJobProperties.joinPipelineOptions(pipelineOptions),
                bigquery_table : testConfiguration.bqTable
        ]

        commonJobProperties.buildPerformanceTest(delegate, argMap)
    }
}
