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
// Code in this directory should conform to the Groovy style guide.
//  http://groovy-lang.org/style-guide.html
class common_job_properties {

  // Sets common top-level job properties for website repository jobs.
  static void setTopLevelWebsiteJobProperties(context) {
    setTopLevelJobProperties(
            context,
            'beam-site',
            'asf-site',
            'beam',
            30)
  }

  // Sets common top-level job properties for main repository jobs.
  static void setTopLevelMainJobProperties(context,
                                           String branch = 'master',
                                           int timeout = 100,
                                           String jenkinsExecutorLabel = 'beam') {
    setTopLevelJobProperties(
            context,
            'beam',
            branch,
            jenkinsExecutorLabel,
            timeout)
  }

  // Sets common top-level job properties. Accessed through one of the above
  // methods to protect jobs from internal details of param defaults.
  private static void setTopLevelJobProperties(context,
                                               String repositoryName,
                                               String defaultBranch,
                                               String jenkinsExecutorLabel,
                                               int defaultTimeout) {

    // GitHub project.
    context.properties {
      githubProjectUrl('https://github.com/apache/' + repositoryName + '/')
    }

    // Set JDK version.
    context.jdk('JDK 1.8 (latest)')

    // Restrict this project to run only on Jenkins executors as specified
    context.label(jenkinsExecutorLabel)

    // Discard old builds. Build records are only kept up to this number of days.
    context.logRotator {
      daysToKeep(14)
    }

    // Source code management.
    context.scm {
      git {
        remote {
          url('https://github.com/apache/' + repositoryName + '.git')
          refspec('+refs/heads/*:refs/remotes/origin/* ' +
                  '+refs/pull/${ghprbPullId}/*:refs/remotes/origin/pr/${ghprbPullId}/*')
        }
        branch('${sha1}')
        extensions {
          cleanAfterCheckout()
        }
      }
    }

    context.parameters {
      // This is a recommended setup if you want to run the job manually. The
      // ${sha1} parameter needs to be provided, and defaults to the main branch.
      stringParam(
          'sha1',
          defaultBranch,
          'Commit id or refname (eg: origin/pr/9/head) you want to build.')
    }

    context.wrappers {
      // Abort the build if it's stuck for more minutes than specified.
      timeout {
        absolute(defaultTimeout)
        abortBuild()
      }

      // Set SPARK_LOCAL_IP for spark tests.
      environmentVariables {
        env('SPARK_LOCAL_IP', '127.0.0.1')
      }
      credentialsBinding {
        string("COVERALLS_REPO_TOKEN", "beam-coveralls-token")
      }
    }
  }

  // Sets the pull request build trigger. Accessed through precommit methods
  // below to insulate callers from internal parameter defaults.
  private static void setPullRequestBuildTrigger(context,
                                                 String commitStatusContext,
                                                 String successComment = '--none--',
                                                 String prTriggerPhrase = '') {
    context.triggers {
      githubPullRequest {
        admins(['asfbot'])
        useGitHubHooks()
        orgWhitelist(['apache'])
        allowMembersOfWhitelistedOrgsAsAdmin()
        permitAll()
        // prTriggerPhrase is the argument which gets set when we want to allow
        // post-commit builds to run against pending pull requests. This block
        // overrides the default trigger phrase with the new one. Setting this
        // will disable automatic invocation of this build; the phrase will be
        // required to start it.
        if (prTriggerPhrase) {
          triggerPhrase(prTriggerPhrase)
          onlyTriggerPhrase()
        }

        extensions {
          commitStatus {
            // This is the name that will show up in the GitHub pull request UI
            // for this Jenkins project.
            delegate.context("Jenkins: " + commitStatusContext)
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
  static void setMavenConfig(context, mavenInstallation='Maven 3.3.3') {
    context.mavenInstallation(mavenInstallation)
    context.mavenOpts('-Dorg.slf4j.simpleLogger.showDateTime=true')
    context.mavenOpts('-Dorg.slf4j.simpleLogger.dateTimeFormat=yyyy-MM-dd\\\'T\\\'HH:mm:ss.SSS')
    // The -XX:+TieredCompilation -XX:TieredStopAtLevel=1 JVM options enable
    // tiered compilation to make the JVM startup times faster during the tests.
    context.mavenOpts('-XX:+TieredCompilation')
    context.mavenOpts('-XX:TieredStopAtLevel=1')
    context.rootPOM('pom.xml')
    // Use a repository local to the workspace for better isolation of jobs.
    context.localRepository(LocalRepositoryLocation.LOCAL_TO_WORKSPACE)
    // Disable archiving the built artifacts by default, as this is slow and flaky.
    // We can usually recreate them easily, and we can also opt-in individual jobs
    // to artifact archiving.
    if (context.metaClass.respondsTo(context, 'archivingDisabled', boolean)) {
      context.archivingDisabled(true)
    }
  }

  // Sets common config for PreCommit jobs.
  static void setPreCommit(context,
                           String commitStatusName,
                           String successComment = '--none--') {
    // Set pull request build trigger.
    setPullRequestBuildTrigger(context, commitStatusName, successComment)
  }

  // Enable triggering postcommit runs against pull requests. Users can comment the trigger phrase
  // specified in the postcommit job and have the job run against their PR to run
  // tests not in the presubmit suite for additional confidence.
  static void enablePhraseTriggeringFromPullRequest(context,
                                                    String commitStatusName,
                                                    String prTriggerPhrase) {
    setPullRequestBuildTrigger(
      context,
      commitStatusName,
      '--none--',
      prTriggerPhrase)
  }

  // Sets common config for PostCommit jobs.
  static void setPostCommit(context,
                            String buildSchedule = '0 */6 * * *',
                            boolean triggerEveryPush = true,
                            String notifyAddress = 'commits@beam.apache.org',
                            boolean emailIndividuals = true) {
    // Set build triggers
    context.triggers {
      // By default runs every 6 hours.
      cron(buildSchedule)
      if (triggerEveryPush) {
        githubPush()
      }
    }

    context.publishers {
      // Notify an email address for each failed build (defaults to commits@).
      mailer(notifyAddress, false, emailIndividuals)
    }
  }

  // Configures the argument list for performance tests, adding the standard
  // performance test job arguments.
  private static def genPerformanceArgs(def argMap) {
    def standard_args = [
      project: 'apache-beam-testing',
      dpb_log_level: 'INFO',
      maven_binary: '/home/jenkins/tools/maven/latest/bin/mvn',
      bigquery_table: 'beam_performance.pkb_results',
      // Publishes results with official tag, for use in dashboards.
      official: 'true'
    ]
    // Note: in case of key collision, keys present in ArgMap win.
    def joined_args = standard_args.plus(argMap)
    def argList = []
    joined_args.each({
        // FYI: Replacement only works with double quotes.
        key, value -> argList.add("--$key=$value")
    })
    return argList.join(' ')
  }

  // Adds the standard performance test job steps.
  static def buildPerformanceTest(def context, def argMap) {
    def pkbArgs = genPerformanceArgs(argMap)
    context.steps {
        // Clean up environment.
        shell('rm -rf PerfKitBenchmarker')
        // Clone appropriate perfkit branch
        shell('git clone https://github.com/GoogleCloudPlatform/PerfKitBenchmarker.git')
        // Install basic job requirements.
        shell('pip install --user -r PerfKitBenchmarker/requirements.txt')
        // Install job requirements for Python SDK.
        shell('pip install --user -e sdks/python/[gcp,test]')
        // Launch performance test.
        shell("python PerfKitBenchmarker/pkb.py $pkbArgs")
    }
  }
}
