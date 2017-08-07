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

    // Allows triggering this build against pull requests.
    common_job_properties.enablePhraseTriggeringFromPullRequest(
            delegate,
            'Run JDBC Performance Test')

    clean_install_command = [
            '/home/jenkins/tools/maven/latest/bin/mvn',
            '-B',
            '-e',
            "-Pdataflow-runner",
            'clean',
            'install',
            "-pl runners/google-cloud-dataflow-java",
            '-DskipTests'
    ]

    io_it_suite_command = [
            '/home/jenkins/tools/maven/latest/bin/mvn',
            '-B',
            '-e',
            'verify',
            '-pl sdks/java/io/jdbc',
            '-Dio-it-suite',
            '-DpkbLocation="$WORKSPACE/PerfKitBenchmarker/pkb.py"',
            '-DmvnBinary=/home/jenkins/tools/maven/latest/bin/mvn',
            '-DintegrationTestPipelineOptions=\'[ "--project=apache-beam-testing", "--tempRoot=gs://temp-storage-for-end-to-end-tests" ]\''
    ]

    steps {
        shell(clean_install_command.join(' '))
        shell(io_it_suite_command.join(' '))
    }
}
