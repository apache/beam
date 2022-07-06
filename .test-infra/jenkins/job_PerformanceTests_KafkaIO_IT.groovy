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
String HIGH_RANGE_PORT = "32767"

/**
 * This job runs the Kafka IO performance tests.
 It runs on a kafka cluster that is build by applying the folder .test-infra/kubernetes/kafka-cluster,
 in an existing kubernetes cluster (DEFAULT_CLUSTER in Kubernetes.groovy).
 The services created to run this test are:
 Pods: 3 kafka pods, 3 zookeeper pods, 1 kafka-config pod which run a job that creates topics.
 Services: 1 bootstrap, 1 broker, 3 outside, 1 zookeeper
 Job: job.batch/kafka-config-eff079ec
 When the performance tests finish all resources are cleaned up by a postBuild step in Kubernetes.groovy
 */
job(jobName) {
  common.setTopLevelMainJobProperties(delegate, 'master', 120)
  common.setAutoJob(delegate, 'H H/6 * * *')
  // [Issue#21824] Disable trigger
  //  common.enablePhraseTriggeringFromPullRequest(
  //      delegate,
  //      'Java KafkaIO Performance Test',
  //      'Run Java KafkaIO Performance Test')
  InfluxDBCredentialsHelper.useCredentials(delegate)

  String namespace = common.getKubernetesNamespace(jobName)
  String kubeconfig = common.getKubeconfigLocationForNamespace(namespace)
  Kubernetes k8s = Kubernetes.create(delegate, kubeconfig, namespace)

  String kafkaDir = common.makePathAbsolute("src/.test-infra/kubernetes/kafka-cluster")
  String kafkaTopicJob = "job.batch/kafka-config-eff079ec"

  /**
   * Specifies steps to avoid port collisions when the Kafka outside services (1,2,3) are created.
   Function k8s.availablePort finds unused ports in the Kubernetes cluster in a range from 32400
   to 32767 by querying used ports, those ports are stored in env vars like KAFKA_SERVICE_PORT_${service},
   which are used to replace default ports for outside-${service}.yml files, before the apply command.
   */
  steps {
    String[] configuredPorts = ["32400", "32401", "32402"]
    (0..2).each { service ->
      k8s.availablePort(service == 0 ? configuredPorts[service] : "\$KAFKA_SERVICE_PORT_${service-1}",
          HIGH_RANGE_PORT, "KAFKA_SERVICE_PORT_$service")
      shell("sed -i -e s/${configuredPorts[service]}/\$KAFKA_SERVICE_PORT_$service/ \
                  ${kafkaDir}/04-outside-services/outside-${service}.yml")
    }
  }
  k8s.apply(kafkaDir)
  (0..2).each { k8s.loadBalancerIP("outside-$it", "KAFKA_BROKER_$it") }
  k8s.waitForJob(kafkaTopicJob,"40m")

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
    kafkaBootstrapServerAddresses: "\$KAFKA_BROKER_0:\$KAFKA_SERVICE_PORT_0,\$KAFKA_BROKER_1:\$KAFKA_SERVICE_PORT_1," +
    "\$KAFKA_BROKER_2:\$KAFKA_SERVICE_PORT_2", //KAFKA_BROKER_ represents IP and KAFKA_SERVICE_ port of outside services
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
    // TODO(https://github.com/apache/beam/issues/20806) remove shuffle_mode=appliance with runner v2 once issue is resolved.
    experiments                  : 'use_runner_v2,shuffle_mode=appliance,use_unified_worker',
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
    // TODO(https://github.com/apache/beam/issues/20806) remove shuffle_mode=appliance with runner v2 once issue is resolved.
    experiments                  : 'use_runner_v2,shuffle_mode=appliance,use_unified_worker',
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
