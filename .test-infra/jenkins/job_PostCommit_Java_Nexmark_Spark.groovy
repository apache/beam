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
import CommonTestProperties.TriggeringContext
import NexmarkBuilder as Nexmark
import NoPhraseTriggeringPostCommitBuilder
import PhraseTriggeringPostCommitBuilder

def final JOB_SPECIFIC_OPTIONS = [
    'suite'        : 'SMOKE',
    'streamTimeout': 60
]

NoPhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_Spark',
        'Spark Runner Nexmark Tests', this) {
  description('Runs the Nexmark suite on the Spark runner.')
  commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240, true, 'beam-perf')
  Nexmark.batchOnlyJob(delegate, JOB_SPECIFIC_OPTIONS, TriggeringContext.PR)
}

PhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_Spark',
        'Run Spark Runner Nexmark Tests', 'Spark Runner Nexmark Tests', this) {
  description('Runs the Nexmark suite on the Spark runner against a Pull Request, on demand.')
  commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240)
  Nexmark.batchOnlyJob(delegate, JOB_SPECIFIC_OPTIONS, TriggeringContext.PR)
}
