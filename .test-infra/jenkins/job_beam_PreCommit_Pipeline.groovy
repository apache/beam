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

// import common_job_properties

// This is the Java precommit which runs a maven install, and the current set
// of precommit tests.
pipelineJob('beam_PreCommit_Pipeline') {
  description('Top-level precommit job. Controls sub-jobs and publishes status to GitHub.')

  // Execute concurrent builds if necessary.
  concurrentBuild()
  properties {
    githubProjectUrl('https://github.com/beam-testing/beam/')
  }
  parameters {
    // This is a recommended setup if you want to run the job manually. The
    // ${sha1} parameter needs to be provided, and defaults to the main branch.
    stringParam(
        'sha1',
        'pipeline_test',
        'Commit id or refname (eg: origin/pr/9/head) you want to build.')
  }
  // Set common parameters.
  //common_job_properties.setTopLevelMainJobProperties(
  //  delegate,
  //  'master',
  //  120)

  // Sets that this is a PreCommit job.
  // common_job_properties.setPreCommit(delegate, 'Jenkins PR Verification')

  definition {
    cpsScm {
      scm {
        git {
          remote {
            github('jasonkuster/beam')
            refspec('+refs/heads/*:refs/remotes/origin/* ' +
                    '+refs/pull/${ghprbPullId}/*:refs/remotes/origin/pr/${ghprbPullId}/*')
          }
          branch('${sha1}')
        }
      }
      scriptPath('.test-infra/jenkins/precommit_pipeline.groovy')
    }
  }
}
