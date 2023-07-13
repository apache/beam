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

import PostcommitJobBuilder
import CommonJobProperties as commonJobProperties
import java.time.LocalDateTime

PostcommitJobBuilder.postCommitJob('beam_PostCommit_Java_Examples_Dataflow_V2',
    'Run Java Examples on Dataflow Runner V2', 'Google Cloud Dataflow Runner V2 Examples', this) {

      description('Runs the Java Examples suite on Dataflow runner V2.')

      commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 180)

      publishers {
        archiveJunit('**/build/test-results/**/*.xml')
      }

      // Generates a unique tag for the container as the current time.
      def now = LocalDateTime.now()
      def unique_tag = '${now.date}${now.hour}${now.minute}${now.second}'
      steps {
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':runners:google-cloud-dataflow-java:examplesJavaRunnerV2IntegrationTest')
          // Increase parallel worker threads above processor limit since most time is
          // spent waiting on Dataflow jobs. ValidatesRunner tests on Dataflow are slow
          // because each one launches a Dataflow job with about 3 mins of overhead.
          // 3 x num_cores strikes a good balance between maxing out parallelism without
          // overloading the machines.
          commonJobProperties.setGradleSwitches(delegate, 3 * Runtime.runtime.availableProcessors())
          switches "-Pcontainer-architecture-list=arm64,amd64"
          switches '-Ppush-containers'
          switches "-Pdocker-repository-root=us.gcr.io/apache-beam-testing/java-postcommit-it"
        }
      }
    }
