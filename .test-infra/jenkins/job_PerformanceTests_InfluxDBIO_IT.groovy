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

String jobName = "beam_PerformanceTests_InfluxDBIO_IT"

job(jobName) {
  common.setTopLevelMainJobProperties(delegate)
  common.enablePhraseTriggeringFromPullRequest(
          delegate,
          'Java InfluxDBIO Performance Test',
          'Run Java InfluxDBIO Performance Test')

  String namespace = common.getKubernetesNamespace(jobName)
  String kubeconfigPath = common.getKubeconfigLocationForNamespace(namespace)
  Kubernetes k8s = Kubernetes.create(delegate, kubeconfigPath, namespace)

  k8s.apply(common.makePathAbsolute("src/.test-infra/kubernetes/influxdb/influxdb.yml"))
  String influxDBHostName = "LOAD_BALANCER_IP"
  k8s.loadBalancerIP("influxdb-load-balancer-service", influxDBHostName)
  Map pipelineOptions = [
          influxDBURL     : "http://\$${influxDBHostName}:8086",
          influxDBUserName : "superadmin",
          influxDBPassword : "supersecretpassword",
          databaseName : "db1"
  ]

  steps {
    gradle {
      rootBuildScriptDir(common.checkoutDir)
      common.setGradleSwitches(delegate)
      switches("--info")
      switches("-DintegrationTestPipelineOptions=\'${common.joinPipelineOptions(pipelineOptions)}\'")
      switches("-DintegrationTestRunner=direct")
      tasks(":sdks:java:io:influxdb:integrationTest --tests org.apache.beam.sdk.io.influxdb.InfluxDBIOIT")
    }
  }
}
