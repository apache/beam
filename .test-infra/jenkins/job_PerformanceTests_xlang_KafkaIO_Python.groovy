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
import LoadTestsBuilder as loadTestsBuilder
import InfluxDBCredentialsHelper

def jobs = [
  [
    name               : 'beam_PerformanceTests_xlang_KafkaIO_Python',
    description        : 'Runs performance tests for xlang Python KafkaIO',
    test               : 'apache_beam.io.external.xlang_kafkaio_perf_test',
    githubTitle        : 'Python xlang KafkaIO Performance Test',
    githubTriggerPhrase: 'Run Python xlang KafkaIO Performance Test',
    pipelineOptions    : [
      publish_to_big_query : true,
      metrics_dataset      : 'beam_performance',
      metrics_table        : 'python_kafkaio_results',
      influx_measurement   : 'python_kafkaio_results',
      test_class           : 'KafkaIOPerfTest',
      input_options        : """'{
                               "num_records": 100000000,
                               "key_size": 10,
                               "value_size": 90,
                               "algorithm": "lcg"
                             }'""".trim().replaceAll("\\s", ""),
      kafka_topic          : 'beam',
      read_timeout         : '1500',
      num_workers          : '5',
      autoscaling_algorithm: 'NONE'
    ]
  ]
]

jobs.findAll {
  it.name in [
    // all tests that enabled
    'beam_PerformanceTests_xlang_KafkaIO_Python',
  ]
}.forEach { testJob -> createKafkaIOTestJob(testJob) }

private void createKafkaIOTestJob(testJob) {
  job(testJob.name) {
    description(testJob.description)
    common.setTopLevelMainJobProperties(delegate)
    common.enablePhraseTriggeringFromPullRequest(delegate, testJob.githubTitle, testJob.githubTriggerPhrase)
    common.setAutoJob(delegate, 'H H * * *')
    InfluxDBCredentialsHelper.useCredentials(delegate)

    // Setup kafka k8s pods
    String namespace = common.getKubernetesNamespace(testJob.name)
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
      String HIGH_RANGE_PORT = "32767"
      (0..2).each { service ->
        k8s.availablePort(service == 0 ? configuredPorts[service] : "\$KAFKA_SERVICE_PORT_${service-1}",
            HIGH_RANGE_PORT, "KAFKA_SERVICE_PORT_$service")
        shell("sed -i -e s/${configuredPorts[service]}/\$KAFKA_SERVICE_PORT_$service/ \
                    ${kafkaDir}/04-outside-services/outside-${service}.yml")
      }
      gradle {
        rootBuildScriptDir(common.checkoutDir)
        tasks(':sdks:java:io:expansion-service:shadowJar')
      }
    }
    k8s.apply(kafkaDir)
    (0..2).each { k8s.loadBalancerIP("outside-$it", "KAFKA_BROKER_$it") }
    k8s.waitForJob(kafkaTopicJob,"40m")

    additionalPipelineArgs = [
      influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
      influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
      bootstrap_servers: "\$KAFKA_BROKER_0:\$KAFKA_SERVICE_PORT_0,\$KAFKA_BROKER_1:\$KAFKA_SERVICE_PORT_1," +
      "\$KAFKA_BROKER_2:\$KAFKA_SERVICE_PORT_2", //KAFKA_BROKER_ represents IP and KAFKA_SERVICE_ port of outside services
    ]
    testJob.pipelineOptions.putAll(additionalPipelineArgs)

    def dataflowSpecificOptions = [
      runner          : 'DataflowRunner',
      project         : 'apache-beam-testing',
      region          : 'us-central1',
      temp_location   : 'gs://temp-storage-for-perf-tests/',
      filename_prefix : "gs://temp-storage-for-perf-tests/${testJob.name}/\${BUILD_ID}/",
      sdk_harness_container_image_overrides: '.*java.*,gcr.io/apache-beam-testing/beam-sdk/beam_java8_sdk:latest'
    ]

    Map allPipelineOptions = dataflowSpecificOptions << testJob.pipelineOptions

    loadTestsBuilder.loadTest(
        delegate,
        testJob.name,
        CommonTestProperties.Runner.DATAFLOW,
        CommonTestProperties.SDK.PYTHON,
        allPipelineOptions,
        testJob.test)
  }
}
