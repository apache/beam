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
import CommonTestProperties.TriggeringContext
import NexmarkBigqueryProperties
import NexmarkBuilder as Nexmark
import NoPhraseTriggeringPostCommitBuilder
import PhraseTriggeringPostCommitBuilder

def final JOB_SPECIFIC_OPTIONS = [
        'suite' : 'SMOKE',
        'streamTimeout' : 60
]

// This job runs the suite of Nexmark tests against the Gearpump runner.
NoPhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_Gearpump',
        'Gearpump Runner Nexmark Tests', this) {
  description('Runs the Nexmark suite on the Gearpump runner.')

  // Set common parameters.
  commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240)

  // Gradle goals for this job.
  Nexmark.streamingOnlyJob(delegate, Runner.GEARPUMP, JOB_SPECIFIC_OPTIONS, 
    Nexmark.TriggeringContext.POST_COMMIT)
}

PhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_Gearpump', 
  'Run Gearpump Runner Nexmark Tests', 'Gearpump Runner Nexmark Tests', this) {
  description('Runs the Nexmark suite on the Gearpump runner against a Pull Request, on demand.')

  // Set common parameters.
  commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240)

  Nexmark.streamingOnlyJob(delegate, Runner.GEARPUMP, JOB_SPECIFIC_OPTIONS, Nexmark.TriggeringContext.PR)
}