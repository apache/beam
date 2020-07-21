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
import PostcommitJobBuilder
import CronJobBuilder
import LoadTestsBuilder

def chicagoTaxiJob = { scope ->
    scope.description('Runs the Chicago Taxi Example on the Dataflow runner.')

    // Set common parameters.
    commonJobProperties.setTopLevelMainJobProperties(scope)

    def pipelineOptions = [
        num_workers          : 5,
        autoscaling_algorithm: 'NONE',
    ]

    // Gradle goals for this job.
    scope.steps {
        gradle {
            rootBuildScriptDir(commonJobProperties.checkoutDir)
            commonJobProperties.setGradleSwitches(delegate)
            tasks(':sdks:python:test-suites:dataflow:py2:chicagoTaxiExample')
            switches('-PgcsRoot=gs://temp-storage-for-perf-tests/chicago-taxi')
            switches("-PpipelineOptions=\"${LoadTestsBuilder.parseOptions(pipelineOptions)}\"")
        }
    }
}

PostcommitJobBuilder.postCommitJob(
    'beam_PostCommit_Python_Chicago_Taxi_Dataflow',
    'Run Chicago Taxi on Dataflow',
    'Chicago Taxi Example on Dataflow ("Run Chicago Taxi on Dataflow")',
    this
) {
    chicagoTaxiJob(delegate)
}

CronJobBuilder.cronJob(
    'beam_PostCommit_Python_Chicago_Taxi_Dataflow',
    'H 14 * * *',
    this
) {
    chicagoTaxiJob(delegate)
}
