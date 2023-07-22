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
import CommonTestProperties.Runner
import CommonTestProperties.SDK
import CommonTestProperties.TriggeringContext
import NexmarkBuilder as Nexmark
import NoPhraseTriggeringPostCommitBuilder
import PhraseTriggeringPostCommitBuilder

// This job runs the suite of Nexmark tests against the Dataflow runner V2.
NoPhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_Dataflow_V2_Java11',
    'Dataflow Runner V2 Java 11 Nexmark Tests', this) {

      description('Runs the Nexmark suite on the Dataflow runner V2 on Java 11.')

      commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240)

      commonJob(delegate, TriggeringContext.POST_COMMIT)
    }

PhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_DataflowV2_Java11',
    'Run Dataflow Runner V2 Java 11 Nexmark Tests', 'Dataflow Runner V2 Java 11 Nexmark Tests', this) {

      description('Runs the Nexmark suite on the Dataflow runner V2 on Java 11 against a Pull Request, on demand.')

      commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240)

      commonJob(delegate, TriggeringContext.PR)
    }

private void commonJob(delegate, TriggeringContext triggeringContext) {
  def final JOB_SPECIFIC_OPTIONS = [
    'influxTags' : '{\\\"runnerVersion\\\":\\\"V2\\\",\\\"javaVersion\\\":\\\"11\\\"}',
    'exportSummaryToBigQuery' : false,
    'region' : 'us-central1',
    'suite' : 'STRESS',
    'numWorkers' : 4,
    'maxNumWorkers' : 4,
    'autoscalingAlgorithm' : 'NONE',
    'nexmarkParallel' : 16,
    'enforceEncodability' : true,
    'enforceImmutability' : true
  ]

  def final JOB_SPECIFIC_SWITCHES = [
    '-Pnexmark.runner.version="V2"'
  ]

  Nexmark.nonQueryLanguageJobs(delegate, Runner.DATAFLOW, SDK.JAVA, JOB_SPECIFIC_OPTIONS, triggeringContext, JOB_SPECIFIC_SWITCHES, Nexmark.JAVA_11_RUNTIME_VERSION)
}
