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

def testConfiguration = [
                jobName           : 'beam_PerformanceTests_Analysis',
                jobDescription    : 'Runs python script that is verifying results in bq',
                prCommitStatusName: 'Performance Tests Analysis',
                prTriggerPhase    : 'Run Performance Tests Analysis',
                bqTables: [
                        "beam_performance.textioit_pkb_results",
                        "beam_performance.compressed_textioit_pkb_results",
                        "beam_performance.many_files_textioit_pkb_results",
                        "beam_performance.avroioit_pkb_results",
                        "beam_performance.tfrecordioit_pkb_results",
                        "beam_performance.xmlioit_pkb_results",
                        "beam_performance.textioit_hdfs_pkb_results",
                        "beam_performance.compressed_textioit_hdfs_pkb_results",
                        "beam_performance.many_files_textioit_hdfs_pkb_results",
                        "beam_performance.avroioit_hdfs_pkb_results",
                        "beam_performance.xmlioit_hdfs_pkb_results",
                        "beam_performance.hadoopformatioit_pkb_results",
                        "beam_performance.mongodbioit_pkb_results",
                        "beam_performance.jdbcioit_pkb_results"
                ]
        ]

// This job runs the performance tests analysis job and produces daily report.
job(testConfiguration.jobName) {
    description(testConfiguration.jobDescription)

    // Set default Beam job properties.
    commonJobProperties.setTopLevelMainJobProperties(delegate)

    // Allows triggering this build against pull requests.
    commonJobProperties.enablePhraseTriggeringFromPullRequest(
            delegate,
            testConfiguration.prCommitStatusName,
            testConfiguration.prTriggerPhase)

    // Run job in postcommit every 24 hours, don't trigger every push, and
    // don't email individual committers.
    commonJobProperties.setAutoJob(
            delegate,
            '30 */24 * * *')

    wrappers{
        credentialsBinding {
            string("SLACK_WEBHOOK_URL", "beam-slack-webhook-url")
        }
    }

    steps {
        // Clean up environment after other python using tools.
        shell('rm -rf PerfKitBenchmarker')
        shell('rm -rf .env')

        // create new VirtualEnv, inherit already existing packages. Explicitly
        // pin to python2.7 here otherwise python3 is used by default.
        shell('virtualenv .env --python=python2.7 --system-site-packages')

        // update setuptools and pip
        shell('.env/bin/pip install --upgrade setuptools pip')

        // Install job requirements for analysis script.
        shell('.env/bin/pip install requests google.cloud.bigquery mock google.cloud.bigtable google.cloud')

        // Launch verification tests before executing script.
        shell('.env/bin/python ' + commonJobProperties.checkoutDir + '/.test-infra/jenkins/verify_performance_test_results_test.py')

        // Launch performance tests analysis.
        shell('.env/bin/python ' + commonJobProperties.checkoutDir + '/.test-infra/jenkins/verify_performance_test_results.py --bqtable \"'+ testConfiguration.bqTables + '\" ' + '--metric=\"run_time\" ' + '--mode=report --send_notification')
    }
}
