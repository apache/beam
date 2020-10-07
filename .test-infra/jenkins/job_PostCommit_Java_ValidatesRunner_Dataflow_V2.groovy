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


// This job runs the suite of ValidatesRunner tests against the Dataflow
// runner V2.
PostcommitJobBuilder.postCommitJob('beam_PostCommit_Java_ValidatesRunner_Dataflow_V2',
    'Run Dataflow_V2 ValidatesRunner', 'Google Cloud Dataflow Runner V2 ValidatesRunner Tests', this) {

      description('Runs the ValidatesRunner suite on the Dataflow runner V2.')

      commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 270)

      // Publish all test results to Jenkins
      publishers {
        archiveJunit('**/build/test-results/**/*.xml')
      }

      // Gradle goals for this job.
      steps {
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':runners:google-cloud-dataflow-java:validatesRunnerV2Test')
          // Increase parallel worker threads above processor limit since most time is
          // spent waiting on Dataflow jobs. ValidatesRunner tests on Dataflow are slow
          // because each one launches a Dataflow job with about 3 mins of overhead.
          // 3 x num_cores strikes a good balance between maxing out parallelism without
          // overloading the machines.
          commonJobProperties.setGradleSwitches(delegate, 3 * Runtime.runtime.availableProcessors())
        }
      }
    }
