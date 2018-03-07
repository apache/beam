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

def testsConfigurations = [
        [
                jobName           : 'beam_PerformanceTests_TextIOIT',
                jobDescription    : 'Runs PerfKit tests for TextIOIT',
                itClass           : 'org.apache.beam.sdk.io.text.TextIOIT',
                bqTable           : 'beam_performance.textioit_pkb_results',
                prCommitStatusName: 'Java TextIO Performance Test',
                prTriggerPhase    : 'Run Java TextIO Performance Test',
                extraPipelineArgs: [
                        numberOfRecords: '1000000'
                ]

        ],
        [
                jobName            : 'beam_PerformanceTests_Compressed_TextIOIT',
                jobDescription     : 'Runs PerfKit tests for TextIOIT with GZIP compression',
                itClass            : 'org.apache.beam.sdk.io.text.TextIOIT',
                bqTable            : 'beam_performance.compressed_textioit_pkb_results',
                prCommitStatusName : 'Java CompressedTextIO Performance Test',
                prTriggerPhase     : 'Run Java CompressedTextIO Performance Test',
                extraPipelineArgs: [
                        numberOfRecords: '1000000',
                        compressionType: 'GZIP'
                ]
        ],
        [
                jobName           : 'beam_PerformanceTests_AvroIOIT',
                jobDescription    : 'Runs PerfKit tests for AvroIOIT',
                itClass           : 'org.apache.beam.sdk.io.avro.AvroIOIT',
                bqTable           : 'beam_performance.avroioit_pkb_results',
                prCommitStatusName: 'Java AvroIO Performance Test',
                prTriggerPhase    : 'Run Java AvroIO Performance Test',
                extraPipelineArgs: [
                        numberOfRecords: '1000000'
                ]
        ],
        [
                jobName           : 'beam_PerformanceTests_TFRecordIOIT',
                jobDescription    : 'Runs PerfKit tests for beam_PerformanceTests_TFRecordIOIT',
                itClass           : 'org.apache.beam.sdk.io.tfrecord.TFRecordIOIT',
                bqTable           : 'beam_performance.tfrecordioit_pkb_results',
                prCommitStatusName: 'Java TFRecordIO Performance Test',
                prTriggerPhase    : 'Run Java TFRecordIO Performance Test',
                extraPipelineArgs: [
                        numberOfRecords: '1000000'
                ]
        ],
        [
                jobName           : 'beam_PerformanceTests_XmlIOIT',
                jobDescription    : 'Runs PerfKit tests for beam_PerformanceTests_XmlIOIT',
                itClass           : 'org.apache.beam.sdk.io.xml.XmlIOIT',
                bqTable           : 'beam_performance.xmlioit_pkb_results',
                prCommitStatusName: 'Java XmlIOPerformance Test',
                prTriggerPhase    : 'Run Java XmlIO Performance Test',
                extraPipelineArgs: [
                        numberOfRecords: '100000000',
                        charset: 'UTF-8'
                ]
        ]
]

for (testConfiguration in testsConfigurations) {
    create_filebasedio_performance_test_job(testConfiguration)
}


private void create_filebasedio_performance_test_job(testConfiguration) {

    // This job runs the file-based IOs performance tests on PerfKit Benchmarker.
    job(testConfiguration.jobName) {
        description(testConfiguration.jobDescription)

        // Set default Beam job properties.
        common_job_properties.setTopLevelMainJobProperties(delegate)

        // Allows triggering this build against pull requests.
        common_job_properties.enablePhraseTriggeringFromPullRequest(
                delegate,
                testConfiguration.prCommitStatusName,
                testConfiguration.prTriggerPhase)

        // Run job in postcommit every 6 hours, don't trigger every push, and
        // don't email individual committers.
        common_job_properties.setPostCommit(
                delegate,
                '0 */6 * * *',
                false,
                'commits@beam.apache.org',
                false)

        def pipelineArgs = [
                project        : 'apache-beam-testing',
                tempRoot       : 'gs://temp-storage-for-perf-tests',
                filenamePrefix : "gs://temp-storage-for-perf-tests/${testConfiguration.jobName}/\${BUILD_ID}/",
        ]
        if (testConfiguration.containsKey('extraPipelineArgs')) {
            pipelineArgs << testConfiguration.extraPipelineArgs
        }

        def pipelineArgList = []
        pipelineArgs.each({
            key, value -> pipelineArgList.add("\"--$key=$value\"")
        })
        def pipelineArgsJoined = "[" + pipelineArgList.join(',') + "]"

        def argMap = [
                benchmarks               : 'beam_integration_benchmark',
                beam_it_timeout          : '1200',
                beam_it_profile          : 'io-it',
                beam_prebuilt            : 'true',
                beam_sdk                 : 'java',
                beam_it_module           : 'sdks/java/io/file-based-io-tests',
                beam_it_class            : testConfiguration.itClass,
                beam_it_options          : pipelineArgsJoined,
                beam_extra_mvn_properties: '["filesystem=gcs"]',
                bigquery_table           : testConfiguration.bqTable,
        ]
        common_job_properties.buildPerformanceTest(delegate, argMap)
    }
}