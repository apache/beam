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
import CommonTestProperties



// This job runs the Chicao Taxi Example script on Dataflow
PostcommitJobBuilder.postCommitJob('beam_PostCommit_Python_Chicago_Taxi_Example_Dataflow',
        'Run Chicago Taxi Example on Dataflow', 'Google Cloud Dataflow Runner Chicago Taxi Example', this) {

    chicagoTaxiExampleJob(delegate)
}

CronJobBuilder.cronJob('beam_PostCommit_Python_Chicago_Taxi_Example_Dataflow', 'H 12 * * *', this) {
    chicagoTaxiExampleJob(delegate)
}

def chicagoTaxiExampleJob = { scope ->
    description('Runs the Chicago Taxi Example on the Dataflow runner.')
    // Publish all test results to Jenkins
    publishers {
        archiveJunit('**/build/test-results/**/*.xml')
    }

    // Gradle goals for this job.
    steps {
        gradle {
            rootBuildScriptDir(commonJobProperties.checkoutDir)
            tasks(':sdks:python:dataflowChicagoTaxiExample')
            switches('-PgcsRoot=gs://')
            switches('-Prunner=DataflowRunner')
        }
    }
}