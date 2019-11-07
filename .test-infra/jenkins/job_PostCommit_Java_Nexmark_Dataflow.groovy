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
import NexmarkJob
import NoPhraseTriggeringPostCommitBuilder
import PhraseTriggeringPostCommitBuilder

Runner runner = Runner.DATAFLOW
SDK sdk = SDK.JAVA

Map jobSpecificOptions = [
    'suite'               : 'STRESS',
    'numWorkers'          : 4,
    'maxNumWorkers'       : 4,
    'autoscalingAlgorithm': 'NONE',
    'nexmarkParallel'     : 16,
    'enforceEncodability' : true,
    'enforceImmutability' : true,
    'runner'              : runner.option
]

NoPhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_Dataflow',
    'Dataflow Runner Nexmark Tests', this) {
  description('Runs the Nexmark suite on the Dataflow runner.')
  commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240)

  new NexmarkJob(delegate, runner, sdk, TriggeringContext.POST_COMMIT).standardJob(jobSpecificOptions)
}

PhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_Nexmark_Dataflow',
    'Run Dataflow Runner Nexmark Tests', 'Dataflow Runner Nexmark Tests', this) {
  description('Runs the Nexmark suite on the Dataflow runner against a Pull Request, on demand.')
  commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240)

  new NexmarkJob(delegate, runner, sdk, TriggeringContext.PR).standardJob(jobSpecificOptions)
}
