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
import CommonTestProperties
import Kubernetes
import InfluxDBCredentialsHelper
import LoadTestsBuilder

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

String jobName = "beam_PerformanceTests_Debezium"

job(jobName) {
  common.setTopLevelMainJobProperties(delegate)
  common.setAutoJob(delegate, 'H H/12 * * *')
  common.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Python Debezium Performance Test',
      'Run Python Debezium Performance Test')
  InfluxDBCredentialsHelper.useCredentials(delegate)

  String namespace = common.getKubernetesNamespace(jobName)
  String kubeconfig = common.getKubeconfigLocationForNamespace(namespace)
  String postgresHostName = "LOAD_BALANCER_IP"

  Kubernetes k8s = Kubernetes.create(delegate, kubeconfig, namespace)
  k8s.apply(common.makePathAbsolute("src/.test-infra/kubernetes/postgres/postgres-service-for-debezium.yml"))
  k8s.loadBalancerIP("postgres-for-debezium", postgresHostName)

  def testConfig = [
    runner: CommonTestProperties.Runner.DATAFLOW,
    kubernetes_host : "\$LOAD_BALANCER_IP",
    kubernetes_port : "5432",
    postgres_user: 'postgres',
    postgres_password: 'uuinkks',
    input_options  : '\'{' +
    '"num_records": 20000000 }\'',
    project: 'apache-beam-testing',
    job_name: 'debezium-performance-test-20min' + now,
    temp_location: 'gs://temp-storage-for-perf-tests/loadtests',
    region: 'us-west1'
  ]

  steps {
    gradle {
      rootBuildScriptDir(common.checkoutDir)
      switches("-Prunner=${testConfig.runner.getDependencyBySDK(CommonTestProperties.SDK.PYTHON)}")
      switches("-PloadTest.mainClass=apache_beam.testing.load_tests.debezium_performance_test")
      switches("-PloadTest.args=\"${LoadTestsBuilder.parseOptions(testConfig)}\"")
      tasks(":sdks:python:apache_beam:testing:load_tests:run")
    }
  }
}

