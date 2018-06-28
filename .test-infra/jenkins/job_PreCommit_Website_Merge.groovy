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
import WebsiteShared as websiteShared

// TODO(BEAM-4505): This job is for the apache/beam-site repository and
// should be removed once website sources are migrated to apache/beam.

// Defines a job.
job('beam_PreCommit_Website_Merge') {
  description('Runs website tests for mergebot.')

  // Set common parameters.
  commonJobProperties.setTopLevelWebsiteJobProperties(delegate, 'mergebot')

  triggers {
    githubPush()
  }

  steps {
    // Run the following shell script as a build step.
    shell """
        ${websiteShared.install_ruby_and_gems_bash}
        # Build the new site and test it.
        rm -fr ./content/
        bundle exec rake test
    """.stripIndent().trim()
  }
}
