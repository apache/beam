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

String jobName = "beam_PerformanceTests_SparkReceiver_IO"

/**
 * This job runs the SparkReceiver IO performance tests.
 It runs on a RabbitMQ cluster that is build by applying the folder .test-infra/kubernetes/rabbit,
 in an existing kubernetes cluster (DEFAULT_CLUSTER in Kubernetes.groovy).
 The services created to run this test are:
 Pods: 1 RabbitMq pods.
 Services: 1 broker
 When the performance tests finish all resources are cleaned up by a postBuild step in Kubernetes.groovy
 */
job(jobName) {
  common.setTopLevelMainJobProperties(delegate, 'master', 120)
  common.setAutoJob(delegate, 'H H/12 * * *')
  common.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Java SparkReceiverIO Performance Test',
      'Run Java SparkReceiverIO Performance Test')
  InfluxDBCredentialsHelper.useCredentials(delegate)

  String namespace = common.getKubernetesNamespace(jobName)
  String kubeconfig = common.getKubeconfigLocationForNamespace(namespace)
  Kubernetes k8s = Kubernetes.create(delegate, kubeconfig, namespace)

  k8s.apply(common.makePathAbsolute("src/.test-infra/kubernetes/rabbit/rabbitmq.yaml"))
  String rabbitMqHostName = "LOAD_BALANCER_IP"
  k8s.loadBalancerIP("rabbitmq", rabbitMqHostName)

  Map pipelineOptions = [
    tempRoot                      : 'gs://temp-storage-for-perf-tests',
    project                       : 'apache-beam-testing',
    runner                        : 'DataflowRunner',
    sourceOptions                 : """
                                     {
                                       "numRecords": "5000000",
                                       "keySizeBytes": "1",
                                       "valueSizeBytes": "90"
                                     }
                                   """.trim().replaceAll("\\s", ""),
    bigQueryDataset               : 'beam_performance',
    bigQueryTable                 : 'sparkreceiverioit_results',
    influxMeasurement             : 'sparkreceiverioit_results',
    influxDatabase                : InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influxHost                    : InfluxDBCredentialsHelper.InfluxDBHostUrl,
    rabbitMqBootstrapServerAddress: "amqp://guest:guest@\$${rabbitMqHostName}:5672",
    streamName                    : 'rabbitMqTestStream',
    readTimeout                   : '1800',
    numWorkers                    : '1',
    autoscalingAlgorithm          : 'NONE'
  ]

  steps {
    gradle {
      rootBuildScriptDir(common.checkoutDir)
      common.setGradleSwitches(delegate)
      switches("--info")
      switches("-DintegrationTestPipelineOptions=\'${common.joinOptionsWithNestedJsonValues(pipelineOptions)}\'")
      switches("-DintegrationTestRunner=dataflow")
      tasks(":sdks:java:io:sparkreceiver:2:integrationTest --tests org.apache.beam.sdk.io.sparkreceiver.SparkReceiverIOIT")
    }
  }
}
