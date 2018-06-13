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

// Defines the seed job, which creates or updates all other Jenkins projects.
job('beam_SeedJob_TriggerPrototype') {
  description('Regenerates TriggerPrototype job.')

  properties {
    githubProjectUrl('https://github.com/apache/beam/')
  }

  // Restrict to only run on Jenkins executors labeled 'beam'
  label('beam')

  logRotator {
    daysToKeep(14)
  }

  scm {
    git {
      remote {
        github('apache/beam')

        // ${ghprbPullId} is not interpolated by groovy, but passed through to Jenkins where it
        // refers to the environment variable
        refspec(['+refs/heads/*:refs/remotes/origin/*',
                 '+refs/pull/${ghprbPullId}/*:refs/remotes/origin/pr/${ghprbPullId}/*']
                .join(' '))

        // The variable ${sha1} is not interpolated by groovy, but a parameter of the Jenkins job
        branch('${sha1}')

        extensions {
          cleanAfterCheckout()
        }
      }
    }
  }

  parameters {
    // Setup for running this job from a pull request
    stringParam(
        'sha1',
        'master',
        'Commit id or refname (eg: origin/pr/4001/head) you want to build against.')
  }

  wrappers {
    timeout {
      absolute(60)
      abortBuild()
    }
  }

  triggers {
    githubPullRequest {
      admins(['asfbot'])
      useGitHubHooks()
      orgWhitelist(['apache'])
      allowMembersOfWhitelistedOrgsAsAdmin()
      permitAll()

      // Also run when manually kicked on a pull request
      triggerPhrase('Run Trigger Seed')
      onlyTriggerPhrase()

      extensions {
        commitStatus {
          context("Trigger Seed Job")
        }

        buildStatus {
          completedStatus('SUCCESS', '--none--')
          completedStatus('FAILURE', '--none--')
          completedStatus('ERROR', '--none--')
        }
      }
    }
  }

  steps {
    dsl {
      // A list or a glob of other groovy files to process.
      external('.test-infra/jenkins/job_PreCommit_TriggerPrototype.groovy')

      // Don't disable or delete jobs not generated from this Seed.
      removeAction('IGNORE')
    }
  }
}
