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

String jobName = "beam_PerformanceTests_MongoDBIO_IT"

job(jobName) {
  common.setTopLevelMainJobProperties(delegate)
  common.setAutoJob(delegate,'H */6 * * *')
  common.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Java MongoDBIO Performance Test',
      'Run Java MongoDBIO Performance Test')
  InfluxDBCredentialsHelper.useCredentials(delegate)

  String namespace = common.getKubernetesNamespace(jobName)
  String kubeconfigPath = common.getKubeconfigLocationForNamespace(namespace)
  Kubernetes k8s = Kubernetes.create(delegate, kubeconfigPath, namespace)

  k8s.apply(common.makePathAbsolute("src/.test-infra/kubernetes/mongodb/load-balancer/mongo.yml"))
  String mongoHostName = "LOAD_BALANCER_IP"
  k8s.loadBalancerIP("mongo-load-balancer-service", mongoHostName)

  Map pipelineOptions = [
    tempRoot            : 'gs://temp-storage-for-perf-tests',
    project             : 'apache-beam-testing',
    numberOfRecords     : '10000000',
    bigQueryDataset     : 'beam_performance',
    bigQueryTable       : 'mongodbioit_results',
    influxMeasurement   : 'mongodbioit_results',
    influxDatabase      : InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influxHost          : InfluxDBCredentialsHelper.InfluxDBHostUrl,
    mongoDBDatabaseName : 'beam',
    mongoDBHostName     : "\$${mongoHostName}",
    mongoDBPort         : 27017,
    runner              : 'DataflowRunner',
    autoscalingAlgorithm: 'NONE',
    numWorkers          : '5'
  ]

  steps {
    gradle {
      rootBuildScriptDir(common.checkoutDir)
      common.setGradleSwitches(delegate)
      switches("--info")
      switches("-DintegrationTestPipelineOptions=\'${common.joinPipelineOptions(pipelineOptions)}\'")
      switches("-DintegrationTestRunner=dataflow")
      tasks(":sdks:java:io:mongodb:integrationTest --tests org.apache.beam.sdk.io.mongodb.MongoDBIOIT")
    }
  }
}
