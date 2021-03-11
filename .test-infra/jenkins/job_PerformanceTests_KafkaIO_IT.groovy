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
import Kubernetes
import InfluxDBCredentialsHelper

String jobName = "beam_PerformanceTests_Kafka_IO"

job(jobName) {
  common.setTopLevelMainJobProperties(delegate)
  common.setAutoJob(delegate, 'H */6 * * *')
  common.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Java KafkaIO Performance Test',
      'Run Java KafkaIO Performance Test')
  InfluxDBCredentialsHelper.useCredentials(delegate)

  String namespace = common.getKubernetesNamespace(jobName)
  String kubeconfig = common.getKubeconfigLocationForNamespace(namespace)
  Kubernetes k8s = Kubernetes.create(delegate, kubeconfig, namespace)
  k8s.apply(common.makePathAbsolute("src/.test-infra/kubernetes/kafka-cluster"))

  (0..2).each { k8s.loadBalancerIP("outside-$it", "KAFKA_BROKER_$it") }

  Map pipelineOptions = [
    tempRoot                     : 'gs://temp-storage-for-perf-tests',
    project                      : 'apache-beam-testing',
    runner                       : 'DataflowRunner',
    sourceOptions                : """
                                     {
                                       "numRecords": "100000000",
                                       "keySizeBytes": "1",
                                       "valueSizeBytes": "90"
                                     }
                                   """.trim().replaceAll("\\s", ""),
    bigQueryDataset              : 'beam_performance',
    bigQueryTable                : 'kafkaioit_results',
    influxMeasurement            : 'kafkaioit_results',
    influxDatabase               : InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influxHost                   : InfluxDBCredentialsHelper.InfluxDBHostUrl,
    kafkaBootstrapServerAddresses: "\$KAFKA_BROKER_0:32400,\$KAFKA_BROKER_1:32401,\$KAFKA_BROKER_2:32402",
    kafkaTopic                   : 'beam',
    readTimeout                  : '900',
    numWorkers                   : '5',
    autoscalingAlgorithm         : 'NONE'
  ]

  // We are using a smaller number of records for streaming test since streaming read is much slower
  // than batch read.
  Map dataflowRunnerV2SdfWrapperPipelineOptions = pipelineOptions + [
    sourceOptions                : """
                                     {
                                       "numRecords": "100000",
                                       "keySizeBytes": "1",
                                       "valueSizeBytes": "90"
                                     }
                                  """.trim().replaceAll("\\s", ""),
    kafkaTopic                   : 'beam-runnerv2',
    bigQueryTable                : 'kafkaioit_results_sdf_wrapper',
    influxMeasurement            : 'kafkaioit_results_sdf_wrapper',
    // TODO(BEAM-11779) remove shuffle_mode=appliance with runner v2 once issue is resolved.
    experiments                  : 'beam_fn_api,use_runner_v2,shuffle_mode=appliance,use_unified_worker',
  ]

  Map dataflowRunnerV2SdfPipelineOptions = pipelineOptions + [
    sourceOptions                : """
                                     {
                                       "numRecords": "100000",
                                       "keySizeBytes": "1",
                                       "valueSizeBytes": "90"
                                     }
                                   """.trim().replaceAll("\\s", ""),
    kafkaTopic                   : 'beam-sdf',
    bigQueryTable                : 'kafkaioit_results_runner_v2',
    influxMeasurement            : 'kafkaioit_results_runner_v2',
    // TODO(BEAM-11779) remove shuffle_mode=appliance with runner v2 once issue is resolved.
    experiments                  : 'beam_fn_api,use_runner_v2,shuffle_mode=appliance,use_unified_worker',
  ]

  steps {
    gradle {
      rootBuildScriptDir(common.checkoutDir)
      common.setGradleSwitches(delegate)
      switches("--info")
      switches("-DintegrationTestPipelineOptions=\'${common.joinOptionsWithNestedJsonValues(pipelineOptions)}\'")
      switches("-DintegrationTestRunner=dataflow")
      tasks(":sdks:java:io:kafka:integrationTest --tests org.apache.beam.sdk.io.kafka.KafkaIOIT.testKafkaIOReadsAndWritesCorrectlyInBatch")
    }
    gradle {
      rootBuildScriptDir(common.checkoutDir)
      common.setGradleSwitches(delegate)
      switches("--info")
      switches("-DintegrationTestPipelineOptions=\'${common.joinOptionsWithNestedJsonValues(dataflowRunnerV2SdfWrapperPipelineOptions)}\'")
      switches("-DintegrationTestRunner=dataflow")
      tasks(":sdks:java:io:kafka:integrationTest --tests org.apache.beam.sdk.io.kafka.KafkaIOIT.testKafkaIOReadsAndWritesCorrectlyInStreaming")
    }
    gradle {
      rootBuildScriptDir(common.checkoutDir)
      common.setGradleSwitches(delegate)
      switches("--info")
      switches("-DintegrationTestPipelineOptions=\'${common.joinOptionsWithNestedJsonValues(dataflowRunnerV2SdfPipelineOptions)}\'")
      switches("-DintegrationTestRunner=dataflow")
      tasks(":sdks:java:io:kafka:integrationTest --tests org.apache.beam.sdk.io.kafka.KafkaIOIT.testKafkaIOReadsAndWritesCorrectlyInStreaming")
    }
  }
}
