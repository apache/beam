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

    // Run job in postcommit every 6 hours, don't trigger every push, and
    // don't email individual committers.
    common_job_properties.setPostCommit(
        delegate,
        '0 */6 * * *',
        false,
        'commits@beam.apache.org',
        false)

    common_job_properties.buildPerfKit(delegate)

    clean_install_args = [
            '-B',
            '-e',
            "-Pdataflow-runner",
            'clean',
            'install',
            "-pl runners/google-cloud-dataflow-java",
            '-DskipTests'
              ]

    io_it_suite_args = [
            '-B',
            '-e',
            '-pl sdks/java/io/jdbc',
            '-Dio-it-suite',
            '-DpkbLocation="$WORKSPACE/PerfKitBenchmarker/pkb.py"',
            '-DintegrationTestPipelineOptions=\'[ "--project=apache-beam-testing", "--tempRoot=gs://temp-storage-for-end-to-end-tests" ]\''
    ]

    steps {
        maven {
            goals(clean_install_args.join(' '))
        }
        maven {
            goals(io_it_suite_args.join(' '))
        }
    }
}
