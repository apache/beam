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

String jobName = "beam_PerformanceTests_MongoDBIO_IT"

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
            'Java MongoDBIO Performance Test',
            'Run Java MongoDBIO Performance Test')

    def pipelineOptions = [
            tempRoot       : 'gs://temp-storage-for-perf-tests',
            project        : 'apache-beam-testing',
            numberOfRecords: '10000000'
    ]

    String namespace = commonJobProperties.getKubernetesNamespace(jobName)
    String kubeconfig = commonJobProperties.getKubeconfigLocationForNamespace(namespace)

    def testArgs = [
            kubeconfig              : kubeconfig,
            beam_it_timeout         : '1800',
            benchmarks              : 'beam_integration_benchmark',
            beam_prebuilt           : 'false',
            beam_sdk                : 'java',
            beam_it_module          : 'sdks/java/io/mongodb',
            beam_it_class           : 'org.apache.beam.sdk.io.mongodb.MongoDBIOIT',
            beam_it_options         : commonJobProperties.joinPipelineOptions(pipelineOptions),
            beam_kubernetes_scripts : commonJobProperties.makePathAbsolute('src/.test-infra/kubernetes/mongodb/load-balancer/mongo.yml'),
            beam_options_config_file: commonJobProperties.makePathAbsolute('src/.test-infra/kubernetes/mongodb/load-balancer/pkb-config.yml'),
            bigquery_table          : 'beam_performance.mongodbioit_pkb_results'
    ]

    commonJobProperties.setupKubernetes(delegate, namespace, kubeconfig)
    commonJobProperties.buildPerformanceTest(delegate, testArgs)
    commonJobProperties.cleanupKubernetes(delegate, namespace, kubeconfig)
}
