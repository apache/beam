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

String jobName = "beam_python_mongoio_load_test"

job(jobName) {
  common.setTopLevelMainJobProperties(delegate)
  common.setAutoJob(delegate, 'H H/6 * * *')
  // [Issue#21824] Disable trigger
  //  common.enablePhraseTriggeringFromPullRequest(
  //      delegate,
  //      'Python MongoDBIO Load Test',
  //      'Run Python MongoDBIO Load Test')

  String namespace = common.getKubernetesNamespace(jobName)
  String kubeconfigPath = common.getKubeconfigLocationForNamespace(namespace)
  Kubernetes k8s = Kubernetes.create(delegate, kubeconfigPath, namespace)

  k8s.apply(common.makePathAbsolute("src/.test-infra/kubernetes/mongodb/load-balancer/mongo.yml"))
  String mongoHostName = "LOAD_BALANCER_IP"
  k8s.loadBalancerIP("mongo-load-balancer-service", mongoHostName)

  Map pipelineOptions = [
    temp_location: 'gs://temp-storage-for-perf-tests/loadtests',
    project      : 'apache-beam-testing',
    region       : 'us-central1',
    mongo_uri    : "mongodb://\$${mongoHostName}:27017",
    num_documents: '1000000',
    batch_size   : '10000',
    runner       : 'DataflowRunner',
    num_workers  : '5'
  ]

  steps {
    gradle {
      rootBuildScriptDir(common.checkoutDir)
      common.setGradleSwitches(delegate)
      switches("-Popts=\'${common.mapToArgString(pipelineOptions)}\'")
      tasks(":sdks:python:test-suites:dataflow:mongodbioIT")
    }
  }
}
