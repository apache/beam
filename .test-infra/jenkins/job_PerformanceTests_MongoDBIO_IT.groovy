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

String kubernetesDir = '"$WORKSPACE/src/.test-infra/kubernetes"'
String kubernetesScript = "${kubernetesDir}/kubernetes.sh"
String jobName = "beam_PerformanceTests_MongoDBIO_IT"

Map pipelineOptions = [
        tempRoot       : 'gs://temp-storage-for-perf-tests',
        project        : 'apache-beam-testing',
        numberOfRecords: '10000000',
        bigQueryDataset: 'beam_performance',
        bigQueryTable  : 'mongodbioit_results',
        mongoDBDatabaseName: 'beam',
        mongoDBHostName: "\$LOAD_BALANCER_IP",
        mongoDBPort: 27017,
        runner: 'DataflowRunner'
]
String namespace = common.getKubernetesNamespace(jobName)
String kubeconfigPath = common.getKubeconfigLocationForNamespace(namespace)

job(jobName) {
  common.setTopLevelMainJobProperties(delegate)
  common.setAutoJob(delegate,'H */6 * * *')
  common.enablePhraseTriggeringFromPullRequest(
          delegate,
          'Java MongoDBIO Performance Test',
          'Run Java MongoDBIO Performance Test')

  steps {
    shell("cp /home/jenkins/.kube/config ${kubeconfigPath}")

    environmentVariables {
      env('KUBECONFIG', kubeconfigPath)
      env('KUBERNETES_NAMESPACE', namespace)
    }

    shell("${kubernetesScript} createNamespace ${namespace}")
    shell("${kubernetesScript} apply ${kubernetesDir}/mongodb/load-balancer/mongo.yml")

    String variableName = "LOAD_BALANCER_IP"
    String command = "${kubernetesScript} loadBalancerIP mongo-load-balancer-service"
    shell("set -eo pipefail; eval ${command} | sed 's/^/${variableName}=/' > job.properties")
    environmentVariables {
      propertiesFile('job.properties')
    }

    gradle {
      rootBuildScriptDir(common.checkoutDir)
      common.setGradleSwitches(delegate)
      switches("--info")
      switches("-DintegrationTestPipelineOptions=\'${common.joinPipelineOptions(pipelineOptions)}\'")
      switches("-DintegrationTestRunner=dataflow")
      tasks(":sdks:java:io:mongodb:integrationTest --tests org.apache.beam.sdk.io.mongodb.MongoDBIOIT")
    }
  }

  publishers {
    postBuildScripts {
      steps {
        shell("${kubernetesScript} deleteNamespace ${namespace}")
        shell("rm ${kubeconfigPath}")
      }
      onlyIfBuildSucceeds(false)
      onlyIfBuildFails(false)
    }
  }
}
