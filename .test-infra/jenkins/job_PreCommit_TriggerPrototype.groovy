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

// Prototype job for testing triggering
job('beam_PreCommit_TriggerPrototype') {
  description('Runs a generic gradle command based on pull request triggering.')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  properties {
    githubProjectUrl('https://github.com/apache/beam/')
  }

  // Set JDK version.
  jdk('JDK 1.8 (latest)')

  // Restrict this project to run only on Jenkins executors as specified
  label('beam')

  // Discard old builds. Build records are only kept up to this number of days.
  logRotator {
    daysToKeep(1)
  }

  // Source code management.
  scm {
    git {
      remote {
        // Double quotes here mean ${repositoryName} is interpolated.
        github("apache/beam")
        // Single quotes here mean that ${ghprbPullId} is not interpolated and instead passed
        // through to Jenkins where it refers to the environment variable.
        refspec('+refs/heads/*:refs/remotes/origin/* ' +
                '+refs/pull/${ghprbPullId}/*:refs/remotes/origin/pr/${ghprbPullId}/*')
      }
      branch('${sha1}')
      extensions {
        cleanAfterCheckout()
        relativeTargetDirectory('src')
      }
    }
  }

  parameters {
    // This is a recommended setup if you want to run the job manually. The
    // ${sha1} parameter needs to be provided, and defaults to the main branch.
    stringParam(
        'sha1',
        'master',
        'Commit id or refname (eg: origin/pr/9/head) you want to build.')
  }

  wrappers {
    // Abort the build if it's stuck for more minutes than specified.
    timeout {
      absolute(90)
      abortBuild()
    }

  }


  // Sets that this is a PreCommit job.
  triggers {
    githubPullRequest {
      admins(['asfbot'])
      useGitHubHooks()
      orgWhitelist(['apache'])
      allowMembersOfWhitelistedOrgsAsAdmin()
      permitAll()
      triggerPhrase('abracadabra')
      userWhitelist('swegner@google.com')
      includedRegions([
        'examples/.*',
        // '^model/'
      ].join('\n'))

      extensions {
        commitStatus {
          // This is the name that will show up in the GitHub pull request UI
          // for this Jenkins project. It has a limit of 255 characters.
          context('TriggerPrototype')
        }

        // Comment messages after build completes.
        buildStatus {
          completedStatus('SUCCESS', '--none--')
          completedStatus('FAILURE', '--none--')
          completedStatus('ERROR', '--none--')
        }
      }
    }
  }

  steps {
    gradle {
      rootBuildScriptDir('src')
      tasks('tasks')
    }
  }
}
