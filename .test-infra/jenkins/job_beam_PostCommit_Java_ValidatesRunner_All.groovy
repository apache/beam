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

// Entries for all standard ValidatesRunner tests. Maps from name of runner to list of options to
// run the ValidatesRunner tests.
validatesRunnerTests = [
        'Apex': [
                '--projects runners/apex',
                '--activate-profiles validates-runner-tests',
                '--activate-profiles local-validates-runner-tests',
        ],
        'Dataflow': [
                '--projects runners/google-cloud-dataflow-java',
                '-DforkCount=0',
                '-DvalidatesRunnerPipelineOptions=\'[ "--runner=TestDataflowRunner", "--project=apache-beam-testing", "--tempRoot=gs://temp-storage-for-validates-runner-tests/" ]\'',
        ],
        'Flink': [
                '--projects runners/flink',
                '--activate-profiles validates-runner-tests',
                '--activate-profiles local-validates-runner-tests',
        ],
        'Spark': [
                '--projects runners/spark',
                '--activate-profiles validates-runner-tests',
                '--activate-profiles local-validates-runner-tests',
                '-Dspark.ui.enabled=false',
        ],
]

// These options are applied to every list of goals.
commonJobOptions = [
        'clean',
        'verify',
        '--also-make',
        '--batch-mode',
        '--errors',
]

// Create a mavenJob for each <K, V> pair in validatesRunnerTests.
validatesRunnerTests.each({
    name, args -> mavenJob("beam_PostCommit_ValidatesRunner_${name}") { genValidatesRunner(delegate, name, args) }
})

// This method calls the common_job_properties methods necessary for a ValidatesRunner suite.
def genValidatesRunner(def context, String name, ArrayList<String> args) {
    // Set common parameters.
    common_job_properties.setTopLevelMainJobProperties(context)

    // Set maven parameters.
    common_job_properties.setMavenConfig(context)

    // Sets that this is a PostCommit job.
    common_job_properties.setPostCommit(context)

    // Allows triggering this build against pull requests.
    common_job_properties.enablePhraseTriggeringFromPullRequest(
            delegate,
            "${name} Runner ValidatesRunner Tests",
            "Run ${name} ValidatesRunner")

    // Build list of Maven goals from commonJobOptions and the runner-specific args.
    context.goals("${(commonJobOptions + args).join(' ')}")
}
