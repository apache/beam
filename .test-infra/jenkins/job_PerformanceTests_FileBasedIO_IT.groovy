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

def jobs = [
        [
                name               : 'beam_PerformanceTests_TextIOIT',
                description        : 'Runs performance tests for TextIOIT',
                test               : 'org.apache.beam.sdk.io.text.TextIOIT',
                githubTitle        : 'Java TextIO Performance Test',
                githubTriggerPhrase: 'Run Java TextIO Performance Test',
                pipelineOptions    : [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable  : 'textioit_results',
                        numberOfRecords: '1000000'
                ]

        ],
        [
                name               : 'beam_PerformanceTests_Compressed_TextIOIT',
                description        : 'Runs performance tests for TextIOIT with GZIP compression',
                test               : 'org.apache.beam.sdk.io.text.TextIOIT',
                githubTitle        : 'Java CompressedTextIO Performance Test',
                githubTriggerPhrase: 'Run Java CompressedTextIO Performance Test',
                pipelineOptions    : [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable  : 'compressed_textioit_results',
                        numberOfRecords: '1000000',
                        compressionType: 'GZIP'
                ]
        ],
        [
                name               : 'beam_PerformanceTests_ManyFiles_TextIOIT',
                description        : 'Runs performance tests for TextIOIT with many output files',
                test               : 'org.apache.beam.sdk.io.text.TextIOIT',
                githubTitle        : 'Java ManyFilesTextIO Performance Test',
                githubTriggerPhrase: 'Run Java ManyFilesTextIO Performance Test',
                pipelineOptions    : [
                        bigQueryDataset            : 'beam_performance',
                        bigQueryTable              : 'many_files_textioit_results',
                        reportGcsPerformanceMetrics: 'true',
                        gcsPerformanceMetrics      : 'true',
                        numberOfRecords            : '1000000',
                        numberOfShards             : '1000'
                ]

        ],
        [
                name               : 'beam_PerformanceTests_AvroIOIT',
                description        : 'Runs performance tests for AvroIOIT',
                test               : 'org.apache.beam.sdk.io.avro.AvroIOIT',
                githubTitle        : 'Java AvroIO Performance Test',
                githubTriggerPhrase: 'Run Java AvroIO Performance Test',
                pipelineOptions    : [
                        numberOfRecords: '1000000',
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable  : 'avroioit_results',
                ]
        ],
        [
                name               : 'beam_PerformanceTests_TFRecordIOIT',
                description        : 'Runs performance tests for beam_PerformanceTests_TFRecordIOIT',
                test               : 'org.apache.beam.sdk.io.tfrecord.TFRecordIOIT',
                githubTitle        : 'Java TFRecordIO Performance Test',
                githubTriggerPhrase: 'Run Java TFRecordIO Performance Test',
                pipelineOptions    : [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable  : 'tfrecordioit_results',
                        numberOfRecords: '1000000'
                ]
        ],
        [
                name               : 'beam_PerformanceTests_XmlIOIT',
                description        : 'Runs performance tests for beam_PerformanceTests_XmlIOIT',
                test               : 'org.apache.beam.sdk.io.xml.XmlIOIT',
                githubTitle        : 'Java XmlIOPerformance Test',
                githubTriggerPhrase: 'Run Java XmlIO Performance Test',
                pipelineOptions    : [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable  : 'xmlioit_results',
                        numberOfRecords: '100000000',
                        charset        : 'UTF-8'
                ]
        ],
        [
                name               : 'beam_PerformanceTests_ParquetIOIT',
                description        : 'Runs performance tests for beam_PerformanceTests_ParquetIOIT',
                test               : 'org.apache.beam.sdk.io.parquet.ParquetIOIT',
                githubTitle        : 'Java ParquetIOPerformance Test',
                githubTriggerPhrase: 'Run Java ParquetIO Performance Test',
                pipelineOptions    : [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable  : 'parquetioit_results',
                        numberOfRecords: '100000000'
                ]
        ],
        [
                name               : 'beam_PerformanceTests_TextIOIT_HDFS',
                description        : 'Runs PerfKit tests for TextIOIT on HDFS',
                test               : 'org.apache.beam.sdk.io.text.TextIOIT',
                githubTitle        : 'Java TextIO Performance Test on HDFS',
                githubTriggerPhrase: 'Run Java TextIO Performance Test HDFS',
                pipelineOptions    : [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable  : 'textioit_hdfs_results',
                        numberOfRecords: '1000000'
                ]

        ],
        [
                name               : 'beam_PerformanceTests_Compressed_TextIOIT_HDFS',
                description        : 'Runs PerfKit tests for TextIOIT with GZIP compression on HDFS',
                test               : 'org.apache.beam.sdk.io.text.TextIOIT',
                githubTitle        : 'Java CompressedTextIO Performance Test on HDFS',
                githubTriggerPhrase: 'Run Java CompressedTextIO Performance Test HDFS',
                pipelineOptions    : [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable  : 'compressed_textioit_hdfs_results',
                        numberOfRecords: '1000000',
                        compressionType: 'GZIP'
                ]
        ],
        [
                name               : 'beam_PerformanceTests_ManyFiles_TextIOIT_HDFS',
                description        : 'Runs PerfKit tests for TextIOIT with many output files on HDFS',
                test               : 'org.apache.beam.sdk.io.text.TextIOIT',
                githubTitle        : 'Java ManyFilesTextIO Performance Test on HDFS',
                githubTriggerPhrase: 'Run Java ManyFilesTextIO Performance Test HDFS',
                pipelineOptions    : [
                        bigQueryDataset            : 'beam_performance',
                        bigQueryTable              : 'many_files_textioit_hdfs_results',
                        reportGcsPerformanceMetrics: 'true',
                        gcsPerformanceMetrics      : 'true',
                        numberOfRecords            : '1000000',
                        numberOfShards             : '1000'
                ]

        ],
        [
                name               : 'beam_PerformanceTests_AvroIOIT_HDFS',
                description        : 'Runs PerfKit tests for AvroIOIT on HDFS',
                test               : 'org.apache.beam.sdk.io.avro.AvroIOIT',
                githubTitle        : 'Java AvroIO Performance Test on HDFS',
                githubTriggerPhrase: 'Run Java AvroIO Performance Test HDFS',
                pipelineOptions    : [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable  : 'avroioit_hdfs_results',
                        numberOfRecords: '1000000'
                ]
        ],
        [
                name               : 'beam_PerformanceTests_TFRecordIOIT_HDFS',
                description        : 'Runs PerfKit tests for beam_PerformanceTests_TFRecordIOIT on HDFS',
                test               : 'org.apache.beam.sdk.io.tfrecord.TFRecordIOIT',
                githubTitle        : 'Java TFRecordIO Performance Test on HDFS',
                githubTriggerPhrase: 'Run Java TFRecordIO Performance Test HDFS',
                pipelineOptions    : [
                        numberOfRecords: '1000000'
                ]
        ],
        [
                name               : 'beam_PerformanceTests_XmlIOIT_HDFS',
                description        : 'Runs PerfKit tests for beam_PerformanceTests_XmlIOIT on HDFS',
                test               : 'org.apache.beam.sdk.io.xml.XmlIOIT',
                githubTitle        : 'Java XmlIOPerformance Test on HDFS',
                githubTriggerPhrase: 'Run Java XmlIO Performance Test HDFS',
                pipelineOptions    : [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable  : 'xmlioit_hdfs_results',
                        numberOfRecords: '100000',
                        charset        : 'UTF-8'
                ]
        ],
        [
                name               : 'beam_PerformanceTests_ParquetIOIT_HDFS',
                description        : 'Runs PerfKit tests for beam_PerformanceTests_ParquetIOIT on HDFS',
                test               : 'org.apache.beam.sdk.io.parquet.ParquetIOIT',
                githubTitle        : 'Java ParquetIOPerformance Test on HDFS',
                githubTriggerPhrase: 'Run Java ParquetIO Performance Test HDFS',
                pipelineOptions    : [
                        bigQueryDataset: 'beam_performance',
                        bigQueryTable  : 'parquetioit_hdfs_results',
                        numberOfRecords: '1000000'
                ]
        ]
]

jobs.findAll {
  it.name in [
          'beam_PerformanceTests_TextIOIT',
          'beam_PerformanceTests_Compressed_TextIOIT',
          'beam_PerformanceTests_ManyFiles_TextIOIT',
          'beam_PerformanceTests_AvroIOIT',
          'beam_PerformanceTests_TFRecordIOIT',
          'beam_PerformanceTests_XmlIOIT',
          'beam_PerformanceTests_ParquetIOIT'
  ]
}.forEach { testJob -> createGCSFileBasedIOITTestJob(testJob) }

private void createGCSFileBasedIOITTestJob(testJob) {
  job(testJob.name) {
    description(testJob.description)
    common.setTopLevelMainJobProperties(delegate)
    common.enablePhraseTriggeringFromPullRequest(delegate, testJob.githubTitle, testJob.githubTriggerPhrase)
    common.setAutoJob(delegate, 'H */6 * * *')

    def dataflowSpecificOptions = [
            runner        : 'DataflowRunner',
            project       : 'apache-beam-testing',
            tempRoot      : 'gs://temp-storage-for-perf-tests',
            filenamePrefix: "gs://temp-storage-for-perf-tests/${testJob.name}/\${BUILD_ID}/",
    ]

    Map allPipelineOptions = dataflowSpecificOptions << testJob.pipelineOptions
    String runner = "dataflow"
    String filesystem = "gcs"
    String testTask = ":sdks:java:io:file-based-io-tests:integrationTest"

    steps {
      gradle {
        rootBuildScriptDir(common.checkoutDir)
        common.setGradleSwitches(delegate)
        switches("--info")
        switches("-DintegrationTestPipelineOptions=\'${common.joinPipelineOptions(allPipelineOptions)}\'")
        switches("-Dfilesystem=\'${filesystem}\'")
        switches("-DintegrationTestRunner=\'${runner}\'")
        tasks("${testTask} --tests ${testJob.test}")
      }
    }
  }
}

jobs.findAll {
  it.name in [
          'beam_PerformanceTests_TextIOIT_HDFS',
          'beam_PerformanceTests_Compressed_TextIOIT_HDFS',
          'beam_PerformanceTests_ManyFiles_TextIOIT_HDFS',
          // TODO(BEAM-3945) TFRecord performance test is failing only when running on hdfs.
          // We need to fix this before enabling this job on jenkins.
          //'beam_PerformanceTests_TFRecordIOIT_HDFS',
          'beam_PerformanceTests_AvroIOIT_HDFS',
          'beam_PerformanceTests_XmlIOIT_HDFS',
          'beam_PerformanceTests_ParquetIOIT_HDFS'
  ]
}.forEach { testJob -> createHDFSFileBasedIOITTestJob(testJob) }

private void createHDFSFileBasedIOITTestJob(testJob) {
  job(testJob.name) {
    description(testJob.description)
    common.setTopLevelMainJobProperties(delegate)
    common.enablePhraseTriggeringFromPullRequest(delegate, testJob.githubTitle, testJob.githubTriggerPhrase)
    common.setAutoJob(delegate, 'H */6 * * *')

    String namespace = common.getKubernetesNamespace(testJob.name)
    String kubeconfig = common.getKubeconfigLocationForNamespace(namespace)
    Kubernetes k8s = Kubernetes.create(delegate, kubeconfig, namespace)

    k8s.apply(common.makePathAbsolute("src/.test-infra/kubernetes/hadoop/LargeITCluster/hdfs-multi-datanode-cluster.yml"))
    String hostName = "LOAD_BALANCER_IP"
    k8s.loadBalancerIP("hadoop", hostName)

    Map additionalOptions = [
            runner           : 'DataflowRunner',
            project          : 'apache-beam-testing',
            tempRoot         : 'gs://temp-storage-for-perf-tests',
            hdfsConfiguration: /[{\\\"fs.defaultFS\\\":\\\"hdfs:$${hostName}:9000\\\",\\\"dfs.replication\\\":1}]/,
            filenamePrefix   : "hdfs://\$${hostName}:9000/TEXTIO_IT_"
    ]

    Map allPipelineOptions = testJob.pipelineOptions << additionalOptions
    String runner = "dataflow"
    String filesystem = "hdfs"
    String testTask = ":sdks:java:io:file-based-io-tests:integrationTest"

    steps {
      gradle {
        rootBuildScriptDir(common.checkoutDir)
        common.setGradleSwitches(delegate)
        switches("--info")
        switches("-DintegrationTestPipelineOptions=\'${common.joinPipelineOptions(allPipelineOptions)}\'")
        switches("-Dfilesystem=\'${filesystem}\'")
        switches("-DintegrationTestRunner=\'${runner}\'")
        tasks("${testTask} --tests ${testJob.test}")
      }
    }
  }
}
