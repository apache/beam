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
  Kubernetes k8s = Kubernetes.create(delegate, kubeconfig, namespace)
  k8s.apply(common.makePathAbsolute("src/.test-infra/kubernetes/postgres/postgres-service-for-debezium.yml"))
  String postgresHostName = "LOAD_BALANCER_IP"
  k8s.loadBalancerIP("postgres-for-dev", postgresHostName)

  (0..2).each { k8s.loadBalancerIP("outside-$it", "Postgres_IP_$it") }

  Map pipelineOptions = [
    tempRoot             : 'gs://temp-storage-for-perf-tests',
    project              : 'apache-beam-testing',
    runner               : 'DataflowRunner',
    postgresServerName   : "\$${postgresHostName}",
    postgresIP           : "\$Postgres_IP_0"
  ]

  steps {
    gradle {
      rootBuildScriptDir(common.checkoutDir)
      common.setGradleSwitches(delegate)
      switches("--info")
      switches("-DintegrationTestPipelineOptions=\'${common.joinPipelineOptions(pipelineOptions)}\'")
      switches("-DintegrationTestRunner=dataflow")
      tasks("apache_beam.testing.load_tests.debezium_performance")
    }
  }
}

