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

// Contains functions that help build Jenkins projects. Functions typically set
// common properties that are shared among all Jenkins projects.
class common_job_properties {

  // Sets common top-level job properties.
  static def setTopLevelJobProperties(def context,
                                      def default_branch = 'master',
                                      def default_timeout = 100) {

    // GitHub project.
    context.properties {
      githubProjectUrl('https://github.com/apache/incubator-beam/')
    }

    // Set JDK version.
    context.jdk('JDK 1.8 (latest)')

    // Restrict this project to run only on Jenkins executors dedicated to the
    // Apache Beam project.
    context.label('beam')

    // Discard old builds. Build records are only kept up to this number of days.
    context.logRotator {
      daysToKeep(14)
    }

    // Source code management.
    context.scm {
      git {
        remote {
          url('https://github.com/apache/incubator-beam.git')
          refspec('+refs/heads/*:refs/remotes/origin/* ' +
                  '+refs/pull/*:refs/remotes/origin/pr/*')
        }
        branch('${sha1}')
        extensions {
          cleanAfterCheckout()
          pruneBranches()
        }
      }
    }

    context.parameters {
      // This is a recommended setup if you want to run the job manually. The
      // ${sha1} parameter needs to be provided, and defaults to the main branch.
      stringParam(
          'sha1',
          default_branch,
          'Commit id or refname (eg: origin/pr/9/head) you want to build.')
    }

    context.wrappers {
      // Abort the build if it's stuck for more minutes than specified.
      timeout {
        absolute(default_timeout)
        abortBuild()
      }

      // Set SPARK_LOCAL_IP for spark tests.
      environmentVariables {
        env('SPARK_LOCAL_IP', '127.0.0.1')
      }
    }
  }

  // Sets the pull request build trigger.
  static def setPullRequestBuildTrigger(def context,
                                        def commitStatusContext,
                                        def successComment = '--none--') {
    context.triggers {
      githubPullRequest {
        admins(['asfbot'])
        useGitHubHooks()
        orgWhitelist(['apache'])
        allowMembersOfWhitelistedOrgsAsAdmin()
        permitAll()

        extensions {
          commitStatus {
            // This is the name that will show up in the GitHub pull request UI
            // for this Jenkins project.
            delegate.context(commitStatusContext)
          }

          /*
            This section is disabled, because of jenkinsci/ghprb-plugin#417 issue.
            For the time being, an equivalent configure section below is added.

          // Comment messages after build completes.
          buildStatus {
            completedStatus('SUCCESS', successComment)
            completedStatus('FAILURE', '--none--')
            completedStatus('ERROR', '--none--')
          }
          */
        }
      }
    }

    // Comment messages after build completes.
    context.configure {
      def messages = it / triggers / 'org.jenkinsci.plugins.ghprb.GhprbTrigger' / extensions / 'org.jenkinsci.plugins.ghprb.extensions.comments.GhprbBuildStatus' / messages
      messages << 'org.jenkinsci.plugins.ghprb.extensions.comments.GhprbBuildResultMessage' {
        message(successComment)
        result('SUCCESS')
      }
      messages << 'org.jenkinsci.plugins.ghprb.extensions.comments.GhprbBuildResultMessage' {
        message('--none--')
        result('ERROR')
      }
      messages << 'org.jenkinsci.plugins.ghprb.extensions.comments.GhprbBuildResultMessage' {
        message('--none--')
        result('FAILURE')
      }
    }
  }

  // Sets common config for Maven jobs.
  static def setMavenConfig(def context) {
    context.mavenInstallation('Maven 3.3.3')
    context.mavenOpts('-Dorg.slf4j.simpleLogger.showDateTime=true')
    context.mavenOpts('-Dorg.slf4j.simpleLogger.dateTimeFormat=yyyy-MM-dd\\\'T\\\'HH:mm:ss.SSS')
    context.rootPOM('pom.xml')
    // Use a repository local to the workspace for better isolation of jobs.
    context.localRepository(LocalRepositoryLocation.LOCAL_TO_WORKSPACE)
    // Disable archiving the built artifacts by default, as this is slow and flaky.
    // We can usually recreate them easily, and we can also opt-in individual jobs
    // to artifact archiving.
    context.archivingDisabled(true)
  }

  // Sets common config for PreCommit jobs.
  static def setPreCommit(def context, comment) {
    // Set pull request build trigger.
    setPullRequestBuildTrigger(context, comment)
  }

  // Sets common config for PostCommit jobs.
  static def setPostCommit(def context,
                           def build_schedule = '0 */6 * * *',
                           def scm_schedule = '* * * * *',
                           def notify_address = 'commits@beam.incubator.apache.org') {
    // Set build triggers
    context.triggers {
      // By default runs every 6 hours.
      cron(build_schedule)
      // Also polls SCM every minute.
      scm(scm_schedule)
    }

    context.publishers {
      // Notify an email address for each failed build (defaults to commits@).
      mailer(notify_address, false, true)
    }
  }
}
