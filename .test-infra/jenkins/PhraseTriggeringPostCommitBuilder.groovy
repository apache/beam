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

/**
 * This class is to be used for defining postcommit jobs that are phrase-triggered only.
 *
 * Purpose of this class is to define common strategies and reporting/building parameters
 * for pre- and post- commit test jobs and unify them across the project.
 */
class PhraseTriggeringPostCommitBuilder extends PostcommitJobBuilder {
  static void postCommitJob(nameBase,
      triggerPhrase,
      githubUiHint,
      scope,
      jobDefinition = {}) {

    // [Issue#21824] Disable trigger
    //    new PostcommitJobBuilder(scope, jobDefinition).defineGhprbTriggeredJob(
    //        nameBase + "_PR", triggerPhrase, githubUiHint, false)
  }
}
