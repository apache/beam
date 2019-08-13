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
import Kubernetes

def testsConfigurations = [
        [
                jobName           : 'beam_PerformanceTests_TextIOIT_HDFS',
                jobDescription    : 'Runs PerfKit tests for TextIOIT on HDFS',
                itClass           : 'org.apache.beam.sdk.io.text.TextIOIT',
                bqTable           : 'beam_performance.textioit_hdfs_pkb_results',
                prCommitStatusName: 'Java TextIO Performance Test on HDFS',
                prTriggerPhase    : 'Run Java TextIO Performance Test HDFS',
                pipelineOptions: [
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
                pipelineOptions: [
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
                pipelineOptions: [
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
                pipelineOptions: [
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
//                pipelineOptions: [
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
                pipelineOptions: [
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
                pipelineOptions: [
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

        String namespace = commonJobProperties.getKubernetesNamespace(testConfiguration.jobName)
        String kubeconfig = commonJobProperties.getKubeconfigLocationForNamespace(namespace)
        Kubernetes k8s = Kubernetes.create(delegate, kubeconfig, namespace)

        k8s.apply(commonJobProperties.makePathAbsolute("src/.test-infra/kubernetes/hadoop/LargeITCluster/hdfs-multi-datanode-cluster.yml"))
        String hostName = "LOAD_BALANCER_IP"
        k8s.loadBalancerIP("hadoop", hostName)

        Map additionalOptions = [
                runner  : 'DataflowRunner',
                project : 'apache-beam-testing',
                tempRoot: 'gs://temp-storage-for-perf-tests',
                hdfsConfiguration: /[{\\\"fs.defaultFS\\\":\\\"hdfs:$${hostName}:9000\\\",\\\"dfs.replication\\\":1}]/,
                filenamePrefix   : "hdfs://\$${hostName}:9000/TEXTIO_IT_"
        ]

        Map allPipelineOptions = testConfiguration.pipelineOptions << additionalOptions
        String runner = "dataflow"
        String filesystem = "hdfs"
        String testTask = ":sdks:java:io:file-based-io-tests:integrationTest"

        steps {
          gradle {
            rootBuildScriptDir(commonJobProperties.checkoutDir)
            commonJobProperties.setGradleSwitches(delegate)
            switches("--info")
            switches("-DintegrationTestPipelineOptions=\'${commonJobProperties.joinPipelineOptions(allPipelineOptions)}\'")
            switches("-Dfilesystem=\'${filesystem}\'")
            switches("-DintegrationTestRunner=\'${runner}\'")
            tasks("${testTask} --tests ${testConfiguration.itClass}")
          }
        }
    }
}
