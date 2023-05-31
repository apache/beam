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
import CommonTestProperties
import CommonTestProperties.Runner
import CommonTestProperties.SDK
import CommonTestProperties.TriggeringContext
import NexmarkBuilder as Nexmark
import NoPhraseTriggeringPostCommitBuilder
import PhraseTriggeringPostCommitBuilder

// This job runs the suite of ValidatesRunner tests against the Flink runner.
NoPhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_Flink',
    'Flink Runner Nexmark Tests', this) {

      description('Runs the Nexmark suite on the Flink runner.')

      commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240, true, 'beam-perf', true, 40)

      commonJob(delegate, TriggeringContext.POST_COMMIT)
    }

PhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_Flink',
    'Run Flink Runner Nexmark Tests', 'Flink Runner Nexmark Tests', this) {

      description('Runs the Nexmark suite on the Flink runner against a Pull Request, on demand.')

      commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240, true, 'beam', true, 40)

      commonJob(delegate, TriggeringContext.PR)
    }

private void commonJob(delegate, TriggeringContext triggeringContext) {
  def final JOB_SPECIFIC_OPTIONS = [
    'suite' : 'SMOKE',
    'streamTimeout' : 60,
  ]

  Nexmark.standardJob(delegate, Runner.FLINK, SDK.JAVA, JOB_SPECIFIC_OPTIONS, triggeringContext)
}
