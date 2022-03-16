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

import Kubernetes
import CommonJobProperties as common
import CommonTestProperties
import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder
import CronJobBuilder
import InfluxDBCredentialsHelper


def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def loadTestConfigurations = { mode, datasetName ->
  [
    [
      title          : 'Debezium  Python Load test: write and read data for 20 minutes',
      test           : 'apache_beam.testing.load_tests.debeziumIO_test',
      runner         : CommonTestProperties.Runner.DATAFLOW,
      pipelineOptions: [
        project              : 'apache-beam-testing',
        region               : 'us-central1',
        job_name             : "load-tests-python-dataflow-${mode}-debezium-IO-${now}",
        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
        publish_to_big_query : true,
        metrics_dataset      : datasetName,
        metrics_table        : "python_dataflow_${mode}_debezium_IO",
        influx_measurement   : "python_${mode}_debezium_IO",
        iterations           : 1,
        postgresUsername     : 'postgres',
        postgresPassword     : 'uuinkks',
        postgresDatabaseName : 'postgres',
        postgresServerName   : "\$${postgresHostName}",
        postgresSsl          : false,
        postgresPort         : '5432',
        num_workers          : 5,
        autoscaling_algorithm: 'NONE'
      ]
    ],    
  ]
  .each { test -> test.pipelineOptions.putAll(additionalPipelineArgs) }
  .each { test -> (mode != 'streaming') ?: addStreamingOptions(test) }
}

def addStreamingOptions(test) {
  // Use highmem workers to prevent out of memory issues.
  test.pipelineOptions << [streaming: null,
    worker_machine_type: 'n1-highmem-4'
  ]
}

def loadTestJob = { scope, triggeringContext, mode ->
  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  loadTestsBuilder.loadTests(scope, CommonTestProperties.SDK.PYTHON,
      loadTestConfigurations(mode, datasetName), 'Debezium', mode)
}

CronJobBuilder.cronJob('beam_LoadTests_Python_Debezium_IO_batch', 'H 16 * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  String namespace = common.getKubernetesNamespace("load-tests-python-dataflow-debezium-IO")
  String kubeconfig = common.getKubeconfigLocationForNamespace(namespace)
  Kubernetes k8s = Kubernetes.create(delegate, kubeconfig, namespace)

  k8s.apply(common.makePathAbsolute("src/.test-infra/kubernetes/postgres/postgres-service-for-local-dev.yml"))
  String postgresHostName = "LOAD_BALANCER_IP"
  k8s.loadBalancerIP("postgres-for-dev", postgresHostName)
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'batch')
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Python_Debezium_IO_batch',
    'Run Load Tests Python Debezium IO Batch',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'batch')
    }

CronJobBuilder.cronJob('beam_LoadTests_Python_Debezium_IO_streaming', 'H 16 * * *', this) {
  additionalPipelineArgs = [
    influx_db_name: InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    influx_hostname: InfluxDBCredentialsHelper.InfluxDBHostUrl,
  ]
  String namespace = common.getKubernetesNamespace("load-tests-python-dataflow-debezium-IO")
  String kubeconfig = common.getKubeconfigLocationForNamespace(namespace)
  Kubernetes k8s = Kubernetes.create(delegate, kubeconfig, namespace)

  k8s.apply(common.makePathAbsolute("src/.test-infra/kubernetes/postgres/postgres-service-for-local-dev.yml"))
  String postgresHostName = "LOAD_BALANCER_IP"
  k8s.loadBalancerIP("postgres-for-dev", postgresHostName)
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT, 'streaming')
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
    'beam_LoadTests_Python_Debezium_IO_streaming',
    'Run Load Tests Python Debezium IO Streaming',
    this
    ) {
      additionalPipelineArgs = [:]
      loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR, 'streaming')
    }