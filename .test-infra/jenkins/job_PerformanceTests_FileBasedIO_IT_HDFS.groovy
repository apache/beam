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

def testsConfigurations = [
        [
                jobName           : 'beam_PerformanceTests_TextIOIT_HDFS',
                jobDescription    : 'Runs PerfKit tests for TextIOIT on HDFS',
                itClass           : 'org.apache.beam.sdk.io.text.TextIOIT',
                bqTable           : 'beam_performance.textioit_hdfs_pkb_results',
                prCommitStatusName: 'Java TextIO Performance Test on HDFS',
                prTriggerPhase    : 'Run Java TextIO Performance Test HDFS',
                extraPipelineArgs: [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable: 'textioit_hdfs_results',
                        numberOfRecords: '1000000'
                ]

        ],
        [
                jobName            : 'beam_PerformanceTests_Compressed_TextIOIT_HDFS',
                jobDescription     : 'Runs PerfKit tests for TextIOIT with GZIP compression on HDFS',
                itClass            : 'org.apache.beam.sdk.io.text.TextIOIT',
                bqTable            : 'beam_performance.compressed_textioit_hdfs_pkb_results',
                prCommitStatusName : 'Java CompressedTextIO Performance Test on HDFS',
                prTriggerPhase     : 'Run Java CompressedTextIO Performance Test HDFS',
                extraPipelineArgs: [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable: 'compressed_textioit_hdfs_results',
                        numberOfRecords: '1000000',
                        compressionType: 'GZIP'
                ]
        ],
        [
                jobName           : 'beam_PerformanceTests_ManyFiles_TextIOIT_HDFS',
                jobDescription    : 'Runs PerfKit tests for TextIOIT with many output files on HDFS',
                itClass           : 'org.apache.beam.sdk.io.text.TextIOIT',
                bqTable           : 'beam_performance.many_files_textioit_hdfs_pkb_results',
                prCommitStatusName: 'Java ManyFilesTextIO Performance Test on HDFS',
                prTriggerPhase    : 'Run Java ManyFilesTextIO Performance Test HDFS',
                extraPipelineArgs: [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable: 'many_files_textioit_hdfs_results',
                        reportGcsPerformanceMetrics: 'true',
                        gcsPerformanceMetrics: 'true',
                        numberOfRecords: '1000000',
                        numberOfShards: '1000'
                ]

        ],
        [
                jobName           : 'beam_PerformanceTests_AvroIOIT_HDFS',
                jobDescription    : 'Runs PerfKit tests for AvroIOIT on HDFS',
                itClass           : 'org.apache.beam.sdk.io.avro.AvroIOIT',
                bqTable           : 'beam_performance.avroioit_hdfs_pkb_results',
                prCommitStatusName: 'Java AvroIO Performance Test on HDFS',
                prTriggerPhase    : 'Run Java AvroIO Performance Test HDFS',
                extraPipelineArgs: [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable: 'avroioit_hdfs_results',
                        numberOfRecords: '1000000'
                ]
        ],
// TODO(BEAM-3945) TFRecord performance test is failing only when running on hdfs.
// We need to fix this before enabling this job on jenkins.
//        [
//                jobName           : 'beam_PerformanceTests_TFRecordIOIT_HDFS',
//                jobDescription    : 'Runs PerfKit tests for beam_PerformanceTests_TFRecordIOIT on HDFS',
//                itClass           : 'org.apache.beam.sdk.io.tfrecord.TFRecordIOIT',
//                bqTable           : 'beam_performance.tfrecordioit_hdfs_pkb_results',
//                prCommitStatusName: 'Java TFRecordIO Performance Test on HDFS',
//                prTriggerPhase    : 'Run Java TFRecordIO Performance Test HDFS',
//                extraPipelineArgs: [
//                        numberOfRecords: '1000000'
//                ]
//        ],
        [
                jobName           : 'beam_PerformanceTests_XmlIOIT_HDFS',
                jobDescription    : 'Runs PerfKit tests for beam_PerformanceTests_XmlIOIT on HDFS',
                itClass           : 'org.apache.beam.sdk.io.xml.XmlIOIT',
                bqTable           : 'beam_performance.xmlioit_hdfs_pkb_results',
                prCommitStatusName: 'Java XmlIOPerformance Test on HDFS',
                prTriggerPhase    : 'Run Java XmlIO Performance Test HDFS',
                extraPipelineArgs: [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable: 'xmlioit_hdfs_results',
                        numberOfRecords: '100000',
                        charset: 'UTF-8'
                ]
        ],
        [
                jobName           : 'beam_PerformanceTests_ParquetIOIT_HDFS',
                jobDescription    : 'Runs PerfKit tests for beam_PerformanceTests_ParquetIOIT on HDFS',
                itClass           : 'org.apache.beam.sdk.io.parquet.ParquetIOIT',
                bqTable           : 'beam_performance.parquetioit_hdfs_pkb_results',
                prCommitStatusName: 'Java ParquetIOPerformance Test on HDFS',
                prTriggerPhase    : 'Run Java ParquetIO Performance Test HDFS',
                extraPipelineArgs: [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable: 'parquetioit_hdfs_results',
                        numberOfRecords: '1000000'
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
        commonJobProperties.setTopLevelMainJobProperties(delegate)

        // Allows triggering this build against pull requests.
        commonJobProperties.enablePhraseTriggeringFromPullRequest(
                delegate,
                testConfiguration.prCommitStatusName,
                testConfiguration.prTriggerPhase)

        // Run job in postcommit every 6 hours, don't trigger every push, and
        // don't email individual committers.
        commonJobProperties.setAutoJob(
                delegate,
                'H */6 * * *')

        def pipelineArgs = [
                project        : 'apache-beam-testing',
                tempRoot       : 'gs://temp-storage-for-perf-tests',
        ]
        if (testConfiguration.containsKey('extraPipelineArgs')) {
            pipelineArgs << testConfiguration.extraPipelineArgs
        }

        def pipelineArgList = []
        pipelineArgs.each({
            key, value -> pipelineArgList.add("\"--$key=$value\"")
        })
        def pipelineArgsJoined = "[" + pipelineArgList.join(',') + "]"

        String namespace = commonJobProperties.getKubernetesNamespace(testConfiguration.jobName)
        String kubeconfig = commonJobProperties.getKubeconfigLocationForNamespace(namespace)

        def argMap = [
                kubeconfig              : kubeconfig,
                benchmarks              : 'beam_integration_benchmark',
                beam_it_timeout         : '1200',
                beam_prebuilt           : 'false',
                beam_sdk                : 'java',
                beam_it_module          : 'sdks/java/io/file-based-io-tests',
                beam_it_class           : testConfiguration.itClass,
                beam_it_options         : pipelineArgsJoined,
                beam_extra_properties   : '["filesystem=hdfs"]',
                bigquery_table          : testConfiguration.bqTable,
                beam_options_config_file: makePathAbsolute('pkb-config.yml'),
                beam_kubernetes_scripts : makePathAbsolute('hdfs-multi-datanode-cluster.yml')
        ]
        commonJobProperties.setupKubernetes(delegate, namespace, kubeconfig)
        commonJobProperties.buildPerformanceTest(delegate, argMap)
        commonJobProperties.cleanupKubernetes(delegate, namespace, kubeconfig)
    }
}

static def makePathAbsolute(String path) {
    return '"$WORKSPACE/src/.test-infra/kubernetes/hadoop/LargeITCluster/' + path + '"'
}
