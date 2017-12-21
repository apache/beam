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

// This job runs the file-based IOs performance tests on PerfKit Benchmarker.
job('beam_PerformanceTests_FileBasedIO_IT') {
    description('Runs PerfKit tests for file-based IOs.')

    // Set default Beam job properties.
    common_job_properties.setTopLevelMainJobProperties(delegate)

    // Allows triggering this build against pull requests.
    common_job_properties.enablePhraseTriggeringFromPullRequest(
            delegate,
            'Java FileBasedIOs Performance Test',
            'Run Java FileBasedIOs Performance Test')

    // Run job in postcommit every 6 hours, don't trigger every push, and
    // don't email individual committers.
    common_job_properties.setPostCommit(
            delegate,
            '0 */6 * * *',
            false,
            'commits@beam.apache.org',
            false)

    def pipelineArgs = [
            project: 'apache-beam-testing',
            tempRoot: 'gs://temp-storage-for-perf-tests',
            numberOfRecords: '1000000',
            filenamePrefix: 'gs://temp-storage-for-perf-tests/filebased/${BUILD_ID}/TESTIOIT',
    ]
    def pipelineArgList = []
    pipelineArgs.each({
        key, value -> pipelineArgList.add("\"--$key=$value\"")
    })
    def pipelineArgsJoined = "[" + pipelineArgList.join(',') + "]"


    def itClasses = [
            "org.apache.beam.sdk.io.text.TextIOIT",
            "org.apache.beam.sdk.io.avro.AvroIOIT",
            "org.apache.beam.sdk.io.tfrecord.TFRecordIOIT",
    ]

    itClasses.each {
        def argMap = [
            benchmarks: 'beam_integration_benchmark',
            beam_it_profile: 'io-it',
            beam_prebuilt: 'true',
            beam_sdk: 'java',
            beam_it_module: 'sdks/java/io/file-based-io-tests',
            beam_it_class: "${it}",
            beam_it_options: pipelineArgsJoined,
            beam_extra_mvn_properties: '["filesystem=gcs"]',
        ]
        common_job_properties.buildPerformanceTest(delegate, argMap)
    }
}
