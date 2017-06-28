
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

import common_job_properties

// This is the Java precommit which runs a maven install, and the current set
// of precommit tests.
mavenJob('beam_PreCommit_Pipeline') {
  description('PreCommit Pipeline Job. Owns overall lifecycle of PreCommit tests.')

  properties {
    githubProjectUrl('https://github.com/apache/' + repositoryName + '/')
  }

  parameters {
    stringParam(
      'sha1',
      'master',
      'Commit id or refname (e.g. origin/pr/9/head) you want to build.')
  }
  
  wrappers {
    timeout {
      absolute(120)
      abortBuild()
    }
  }  
  
  // Restrict this project to run only on Jenkins executors as specified
  label('beam')

  // Execute concurrent builds if necessary.
  concurrentBuild()


  triggers {
    githubPullRequest {
      admins(['asfbot'])
      useGitHubHooks()
      orgWhitelist(['apache'])
      allowMembersOfWhitelistedOrgsAsAdmin()
      permitAll()
      displayBuildErrorsOnDownstreamBuilds()
      extensions {
        commitStatus {
          context("Jenkins: PreCommit Pipeline")
        }
        buildStatus {
          completedStatus('SUCCESS', '--none--')
          completedStatus('FAILURE', '--none--')
          completedStatus('ERROR', '--none--')
        }
      }
    }
  }

  definition {
    cpsScm {
      // Source code management.
      scm {
        git {
          remote {
            github("apache/beam")
            refspec('+refs/heads/*:refs/remotes/origin/* ' +
                    '+refs/pull/*:refs/remotes/origin/pr/*')
          }
          branch('${sha1}')
          extensions {
            cleanAfterCheckout()
          }
        }
      }
      scriptPath('.test-infra/jenkins/Precommit_Pipeline.groovy')
    }
  }
}
