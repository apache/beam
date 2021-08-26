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

// This creates the Python nightly snapshot build. Into gs://beam-python-nightly-snapshots.
job('beam_Release_Python_NightlySnapshot') {
  description('Publish a nightly snapshot for Python SDK.')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  commonJobProperties.setTopLevelMainJobProperties(delegate)

  // This is a post-commit job that runs once per day, not for every push.
  commonJobProperties.setAutoJob(
      delegate,
      '0 7 * * *',
      'builds@beam.apache.org')

  // Allows triggering this build against pull requests.
  commonJobProperties.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Create Python SDK Nightly Snapshot',
      'Run Python Publish')

  steps {
    // Cleanup Python directory.
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':sdks:python:clean')
      commonJobProperties.setGradleSwitches(delegate)
    }
    // Build snapshot.
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':sdks:python:buildSnapshot')
      commonJobProperties.setGradleSwitches(delegate)
    }
    // Publish snapshot to a public accessible GCS directory.
    shell('cd ' + commonJobProperties.checkoutDir +
        ' && bash sdks/python/scripts/run_snapshot_publish.sh')
  }
}
