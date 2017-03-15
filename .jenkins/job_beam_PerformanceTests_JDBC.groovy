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

// This job runs the Beam performance tests on PerfKit Benchmarker.
job('beam_PerformanceTests_JDBC'){
    // Set default Beam job properties.
    common_job_properties.setTopLevelMainJobProperties(delegate)

    // Run job in postcommit every 6 hours and don't trigger every push.
    common_job_properties.setPostCommit(delegate, '0 */6 * * *', false)

    common_job_properties.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Dataflow Runner JDBC Performance Tests',
      'Run Dataflow JDBC Performance Tests')

    steps {
        // Clones appropriate perfkit branch
        shell('git clone -b apache --single-branch https://github.com/jasonkuster/PerfKitBenchmarker.git')
        shell('pip install --user -r PerfKitBenchmarker/requirements.txt')
        shell('python PerfKitBenchmarker/pkb.py --project=apache-beam-testing --benchmarks=beam_integration_benchmark --dpb_it_class=org.apache.beam.sdk.io.jdbc.JdbcIOIT --dpb_it_args=--tempRoot=gs://temp-storage-for-end-to-end-tests,--project=apache-beam-testing,--postgresServerName=10.36.0.11,--postgresUsername=postgres,--postgresDatabaseName=postgres,--postgresPassword=uuinkks,--postgresSsl=false --dpb_log_level=INFO --dpb_maven_binary=/home/jenkins/tools/maven/latest/bin/mvn --dpb_beam_it_module=sdks/java/io/jdbc --dpb_beam_it_profile=io-it --bigquery_table=beam_performance.pkb_results --official=true')
        shell('rm -rf PerfKitBenchmarker')
    }
}
