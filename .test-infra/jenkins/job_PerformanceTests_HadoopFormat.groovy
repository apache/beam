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
import CommonJobProperties as commonJobProperties
import Kubernetes

String jobName = "beam_PerformanceTests_HadoopFormat"

job(jobName) {
    // Set default Beam job properties.
    commonJobProperties.setTopLevelMainJobProperties(delegate)

    // Run job in postcommit every 6 hours, don't trigger every push, and
    // don't email individual committers.
    commonJobProperties.setAutoJob(
            delegate,
            'H */6 * * *')

    commonJobProperties.enablePhraseTriggeringFromPullRequest(
            delegate,
            'Java HadoopFormatIO Performance Test',
            'Run Java HadoopFormatIO Performance Test')

    String namespace = commonJobProperties.getKubernetesNamespace(jobName)
    String kubeconfig = commonJobProperties.getKubeconfigLocationForNamespace(namespace)
    Kubernetes k8s = Kubernetes.create(delegate, kubeconfig, namespace)

    k8s.apply(commonJobProperties.makePathAbsolute("src/.test-infra/kubernetes/postgres/postgres-service-for-local-dev.yml"))
    String postgresHostName = "LOAD_BALANCER_IP"
    k8s.loadBalancerIP("postgres-for-dev", postgresHostName)

    Map pipelineOptions = [
            tempRoot            : 'gs://temp-storage-for-perf-tests',
            project             : 'apache-beam-testing',
            runner              : 'DataflowRunner',
            numberOfRecords     : '600000',
            bigQueryDataset     : 'beam_performance',
            bigQueryTable       : 'hadoopformatioit_results',
            postgresUsername    : 'postgres',
            postgresPassword    : 'uuinkks',
            postgresDatabaseName: 'postgres',
            postgresServerName  : "\$${postgresHostName}",
            postgresSsl         : false,
            postgresPort        : '5432'
    ]

    steps {
        gradle {
            rootBuildScriptDir(commonJobProperties.checkoutDir)
            commonJobProperties.setGradleSwitches(delegate)
            switches("--info")
            switches("-DintegrationTestPipelineOptions=\'${commonJobProperties.joinPipelineOptions(pipelineOptions)}\'")
            switches("-DintegrationTestRunner=dataflow")
            tasks(":sdks:java:io:hadoop-format:integrationTest --tests org.apache.beam.sdk.io.hadoop.format.HadoopFormatIOIT")
        }
    }
}

