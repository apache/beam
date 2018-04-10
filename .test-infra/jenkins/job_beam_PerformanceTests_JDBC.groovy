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

import common_job_properties

job('beam_PerformanceTests_JDBC') {
    // Set default Beam job properties.
    common_job_properties.setTopLevelMainJobProperties(delegate)

    // Run job in postcommit every 6 hours, don't trigger every push, and
    // don't email individual committers.
    common_job_properties.setPostCommit(
            delegate,
            '0 */6 * * *',
            false,
            'commits@beam.apache.org',
            false)

    common_job_properties.enablePhraseTriggeringFromPullRequest(
            delegate,
            'Java JdbcIO Performance Test',
            'Run Java JdbcIO Performance Test')

    def pipelineOptions = [
            tempRoot       : 'gs://temp-storage-for-perf-tests',
            project        : 'apache-beam-testing',
            postgresPort   : '5432',
            numberOfRecords: '5000000'
    ]

    String namespace = common_job_properties.getKubernetesNamespace('jdbcioit')
    String kubeconfig = common_job_properties.getKubeconfigLocationForNamespace(namespace)

    def testArgs = [
            kubeconfig              : kubeconfig,
            beam_it_timeout         : '1800',
            benchmarks              : 'beam_integration_benchmark',
            beam_it_profile         : 'io-it',
            beam_prebuilt           : 'true',
            beam_sdk                : 'java',
            beam_it_module          : 'sdks/java/io/jdbc',
            beam_it_class           : 'org.apache.beam.sdk.io.jdbc.JdbcIOIT',
            beam_it_options         : common_job_properties.joinPipelineOptions(pipelineOptions),
            beam_kubernetes_scripts : common_job_properties.makePathAbsolute('src/.test-infra/kubernetes/postgres/postgres-service-for-local-dev.yml'),
            beam_options_config_file: common_job_properties.makePathAbsolute('src/.test-infra/kubernetes/postgres/pkb-config-local.yml'),
            bigquery_table          : 'beam_performance.jdbcioit_pkb_results'
    ]

    common_job_properties.setupKubernetes(delegate, namespace, kubeconfig)
    common_job_properties.buildPerformanceTest(delegate, testArgs)
    common_job_properties.cleanupKubernetes(delegate, namespace, kubeconfig)
}

