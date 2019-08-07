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

def jobConfigurations = [
        [
                jobName           : 'beam_PerformanceTests_TextIOIT',
                jobDescription    : 'Runs performance tests for TextIOIT',
                itClass           : 'org.apache.beam.sdk.io.text.TextIOIT',
                prCommitStatusName: 'Java TextIO Performance Test',
                prTriggerPhase    : 'Run Java TextIO Performance Test',
                pipelineOptions: [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable: 'textioit_results',
                        numberOfRecords: '1000000'
                ]

        ],
        [
                jobName            : 'beam_PerformanceTests_Compressed_TextIOIT',
                jobDescription     : 'Runs performance tests for TextIOIT with GZIP compression',
                itClass            : 'org.apache.beam.sdk.io.text.TextIOIT',
                prCommitStatusName : 'Java CompressedTextIO Performance Test',
                prTriggerPhase     : 'Run Java CompressedTextIO Performance Test',
                pipelineOptions: [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable: 'compressed_textioit_results',
                        numberOfRecords: '1000000',
                        compressionType: 'GZIP'
                ]
        ],
        [
                jobName           : 'beam_PerformanceTests_ManyFiles_TextIOIT',
                jobDescription    : 'Runs performance tests for TextIOIT with many output files',
                itClass           : 'org.apache.beam.sdk.io.text.TextIOIT',
                prCommitStatusName: 'Java ManyFilesTextIO Performance Test',
                prTriggerPhase    : 'Run Java ManyFilesTextIO Performance Test',
                pipelineOptions: [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable: 'many_files_textioit_results',
                        reportGcsPerformanceMetrics: 'true',
                        gcsPerformanceMetrics: 'true',
                        numberOfRecords: '1000000',
                        numberOfShards: '1000'
                ]

        ],
        [
                jobName           : 'beam_PerformanceTests_AvroIOIT',
                jobDescription    : 'Runs performance tests for AvroIOIT',
                itClass           : 'org.apache.beam.sdk.io.avro.AvroIOIT',
                prCommitStatusName: 'Java AvroIO Performance Test',
                prTriggerPhase    : 'Run Java AvroIO Performance Test',
                pipelineOptions: [
                        numberOfRecords: '1000000',
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable: 'avroioit_results',
                ]
        ],
        [
                jobName           : 'beam_PerformanceTests_TFRecordIOIT',
                jobDescription    : 'Runs performance tests for beam_PerformanceTests_TFRecordIOIT',
                itClass           : 'org.apache.beam.sdk.io.tfrecord.TFRecordIOIT',
                prCommitStatusName: 'Java TFRecordIO Performance Test',
                prTriggerPhase    : 'Run Java TFRecordIO Performance Test',
                pipelineOptions: [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable: 'tfrecordioit_results',
                        numberOfRecords: '1000000'
                ]
        ],
        [
                jobName           : 'beam_PerformanceTests_XmlIOIT',
                jobDescription    : 'Runs performance tests for beam_PerformanceTests_XmlIOIT',
                itClass           : 'org.apache.beam.sdk.io.xml.XmlIOIT',
                prCommitStatusName: 'Java XmlIOPerformance Test',
                prTriggerPhase    : 'Run Java XmlIO Performance Test',
                pipelineOptions: [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable: 'xmlioit_results',
                        numberOfRecords: '100000000',
                        charset: 'UTF-8'
                ]
        ],
        [
                jobName           : 'beam_PerformanceTests_ParquetIOIT',
                jobDescription    : 'Runs performance tests for beam_PerformanceTests_ParquetIOIT',
                itClass           : 'org.apache.beam.sdk.io.parquet.ParquetIOIT',
                prCommitStatusName: 'Java ParquetIOPerformance Test',
                prTriggerPhase    : 'Run Java ParquetIO Performance Test',
                pipelineOptions: [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable: 'parquetioit_results',
                        numberOfRecords: '100000000'
                ]
        ]
]

for (jobConfiguration in jobConfigurations) {
    createFileBasedIOITTestJob(jobConfiguration)
}


private void createFileBasedIOITTestJob(jobConfiguration) {

    job(jobConfiguration.jobName) {
        description(jobConfiguration.jobDescription)
        commonJobProperties.setTopLevelMainJobProperties(delegate)
        commonJobProperties.enablePhraseTriggeringFromPullRequest(
                delegate,
                jobConfiguration.prCommitStatusName,
                jobConfiguration.prTriggerPhase)
        commonJobProperties.setAutoJob(
                delegate,
                'H */6 * * *')

        def dataflowSpecificOptions = [
                runner        : 'DataflowRunner',
                project       : 'apache-beam-testing',
                tempRoot      : 'gs://temp-storage-for-perf-tests',
                filenamePrefix: "gs://temp-storage-for-perf-tests/${jobConfiguration.jobName}/\${BUILD_ID}/",
        ]
        Map allPipelineOptions = dataflowSpecificOptions << jobConfiguration.pipelineOptions
        String runner = "dataflow"
        String filesystem = "gcs"
        String testTask = ":sdks:java:io:file-based-io-tests:integrationTest"

        steps {
            gradle {
                rootBuildScriptDir(commonJobProperties.checkoutDir)
                commonJobProperties.setGradleSwitches(delegate)
                switches("--info")
                switches("-DintegrationTestPipelineOptions=\'${commonJobProperties.joinPipelineOptions(allPipelineOptions)}\'")
                switches("-Dfilesystem=\'${filesystem}\'")
                switches("-DintegrationTestRunner=\'${runner}\'")
                tasks("${testTask} --tests ${jobConfiguration.itClass}")
            }
        }
    }
}
