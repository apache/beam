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

String jobName = "beam_PerformanceTests_SingleStoreIO"

void waitForPodWithLabel(job, Kubernetes k8s, String label) {
  job.steps {
    shell("${k8s.KUBERNETES_DIR}/singlestore/wait-for-pod-with-label.sh ${label} 600")
  }
}

void waitFor(job, Kubernetes k8s, String resource) {
  job.steps {
    shell("${k8s.KUBERNETES_DIR}/singlestore/wait-for.sh ${resource} 600")
  }
}

job(jobName) {
  common.setTopLevelMainJobProperties(delegate)
  common.setAutoJob(delegate,'H H/12 * * *')
  common.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Java SingleStoreIO Performance Test',
      'Run Java SingleStoreIO Performance Test')
  InfluxDBCredentialsHelper.useCredentials(delegate)


  String namespace = common.getKubernetesNamespace(jobName)
  String kubeconfigPath = common.getKubeconfigLocationForNamespace(namespace)
  Kubernetes k8s = Kubernetes.create(delegate, kubeconfigPath, namespace)

  k8s.apply(common.makePathAbsolute("src/.test-infra/kubernetes/singlestore/sdb-rbac.yaml"))
  k8s.apply(common.makePathAbsolute("src/.test-infra/kubernetes/singlestore/sdb-cluster-crd.yaml"))
  k8s.apply(common.makePathAbsolute("src/.test-infra/kubernetes/singlestore/sdb-operator.yaml"))
  waitForPodWithLabel(delegate, k8s, "sdb-operator")

  k8s.apply(common.makePathAbsolute("src/.test-infra/kubernetes/singlestore/sdb-cluster.yaml"))
  waitFor(delegate, k8s, "memsqlclusters.memsql.com")

  String singlestoreHostName = "LOAD_BALANCER_IP"
  k8s.loadBalancerIP("svc-sdb-cluster-ddl", singlestoreHostName)

  Map pipelineOptions = [
    tempRoot                  : 'gs://temp-storage-for-perf-tests',
    project                   : 'apache-beam-testing',
    runner                    : 'DataflowRunner',
    singleStoreServerName     : "\$${singlestoreHostName}",
    singleStoreUsername : "admin",
    singleStorePassword : "secretpass",
    singleStorePort: "3306",
    numberOfRecords: "5000000",
    influxMeasurement    : 'singlestoreioit_results',
    influxDatabase       : InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influxHost           : InfluxDBCredentialsHelper.InfluxDBHostUrl
  ]

  steps {
    gradle {
      rootBuildScriptDir(common.checkoutDir)
      common.setGradleSwitches(delegate)
      switches("--info")
      switches("-DintegrationTestPipelineOptions=\'${common.joinPipelineOptions(pipelineOptions)}\'")
      switches("-DintegrationTestRunner=dataflow")
      tasks(":sdks:java:io:singlestore:integrationTest --tests org.apache.beam.sdk.io.singlestore.SingleStoreIOPerformanceIT")
    }
  }
}
