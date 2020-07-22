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
job('beam_SeedJob_Standalone') {
  description('Automatically configures all Apache Beam Jenkins projects based' +
      ' on Jenkins DSL groovy files checked into the code repository.')

  properties {
    githubProjectUrl('https://github.com/apache/beam/')
  }

  // Restrict to only run on Jenkins executors labeled 'beam'
  label('beam')

  logRotator {
    daysToKeep(30)
  }

  scm {
    git {
      remote {
        github('apache/beam')

        // ${ghprbPullId} is not interpolated by groovy, but passed through to Jenkins where it
        // refers to the environment variable
        refspec([
          '+refs/heads/*:refs/remotes/origin/*',
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
    // Run once per day
    cron('0 */5 * * *')

    githubPullRequest {
      admins(['asfbot'])
      useGitHubHooks()
      orgWhitelist(['apache'])
      allowMembersOfWhitelistedOrgsAsAdmin()

      // Also run when manually kicked on a pull request
      triggerPhrase('Run Standalone Seed Job')
      onlyTriggerPhrase()

      extensions {
        commitStatus {
          context("Jenkins: Standalone Seed Job")
        }

        buildStatus {
          completedStatus('SUCCESS', '--none--')
          completedStatus('FAILURE', '--none--')
          completedStatus('ERROR', '--none--')
        }
      }
    }
  }

  publishers {
    mailer('builds@beam.apache.org', false, true)
  }

  steps {
    dsl {
      // A list or a glob of other groovy files to process.
      external('.test-infra/jenkins/job_*.groovy')

      // If a job is removed from the script, disable it (rather than deleting).
      removeAction('DISABLE')
    }
  }
}
