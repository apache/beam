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
class CommonJobProperties {

  static String checkoutDir = 'src'

  // Sets common top-level job properties for main repository jobs.
  static void setTopLevelMainJobProperties(def context,
                                           String defaultBranch = 'master',
                                           int defaultTimeout = 100,
                                           boolean allowRemotePoll = true,
                                           String jenkinsExecutorLabel =  'beam') {
    // GitHub project.
    context.properties {
      githubProjectUrl('https://github.com/apache/beam/')
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
          github("apache/beam")
          // Single quotes here mean that ${ghprbPullId} is not interpolated and instead passed
          // through to Jenkins where it refers to the environment variable.
          refspec('+refs/heads/*:refs/remotes/origin/* ' +
                  '+refs/pull/${ghprbPullId}/*:refs/remotes/origin/pr/${ghprbPullId}/*')
        }
        branch('${sha1}')
        extensions {
          cleanAfterCheckout()
          relativeTargetDirectory(checkoutDir)
          if (!allowRemotePoll) {
            disableRemotePoll()
          }
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
        string("SLACK_WEBHOOK_URL", "beam-slack-webhook-url")
      }
      timestamps()
    }
  }

  // Sets the pull request build trigger. Accessed through precommit methods
  // below to insulate callers from internal parameter defaults.
  static void setPullRequestBuildTrigger(context,
                                         String commitStatusContext,
                                         String prTriggerPhrase = '',
                                         boolean onlyTriggerPhraseToggle = true,
                                         List<String> triggerPathPatterns = []) {
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
        }
        if (onlyTriggerPhraseToggle) {
          onlyTriggerPhrase()
        }
        if (!triggerPathPatterns.isEmpty()) {
          includedRegions(triggerPathPatterns.join('\n'))
        }

        extensions {
          commitStatus {
            // This is the name that will show up in the GitHub pull request UI
            // for this Jenkins project. It has a limit of 255 characters.
            delegate.context commitStatusContext.take(255)
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
  }

  // Default maxWorkers is 12 to avoid jvm oom as in [BEAM-4847].
  static void setGradleSwitches(context, maxWorkers = 12) {
    def defaultSwitches = [
      // Continue the build even if there is a failure to show as many potential failures as possible.
      '--continue',
    ]

    for (String gradle_switch : defaultSwitches) {
      context.switches(gradle_switch)
    }
    context.switches("--max-workers=${maxWorkers}")

    // Ensure that parallel workers don't exceed total available memory.

    // For [BEAM-4847], hardcode Xms and Xmx to reasonable values (2g/4g).
    context.switches("-Dorg.gradle.jvmargs=-Xms2g")
    context.switches("-Dorg.gradle.jvmargs=-Xmx4g")
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
      prTriggerPhrase,
      true)
  }

  // Sets this as a cron job, running on a schedule.
  static void setCronJob(context, String buildSchedule) {
    context.triggers {
      cron(buildSchedule)
    }
  }

  // Sets common config for jobs which run on a schedule; optionally on push
  static void setAutoJob(context,
                         String buildSchedule = '0 */6 * * *',
                         notifyAddress = 'builds@beam.apache.org',
                         triggerOnCommit = false,
                         emailIndividuals = false) {

    // Set build triggers
    context.triggers {
      // By default runs every 6 hours.
      cron(buildSchedule)

      if (triggerOnCommit){
        githubPush()
      }
    }



    context.publishers {
      // Notify an email address for each failed build (defaults to builds@).
      mailer(
          notifyAddress,
          /* _do_ notify every unstable build */ false,
          /* do not email individuals */ false)
      if (emailIndividuals){
        extendedEmail {
          triggers {
            firstFailure {
              sendTo {
                firstFailingBuildSuspects()
              }
            }
          }
        }
      }
    }
  }

  static def mapToArgString(LinkedHashMap<String, String> inputArgs) {
    List argList = []
    inputArgs.each({
        // FYI: Replacement only works with double quotes.
      key, value -> argList.add("--$key=$value")
    })
    return argList.join(' ')
  }

  // Configures the argument list for performance tests, adding the standard
  // performance test job arguments.
  private static def genPerformanceArgs(def argMap) {
    LinkedHashMap<String, String> standardArgs = [
      project: 'apache-beam-testing',
      dpb_log_level: 'INFO',
      bigquery_table: 'beam_performance.pkb_results',
      k8s_get_retry_count: 36, // wait up to 6 minutes for K8s LoadBalancer
      k8s_get_wait_interval: 10,
      temp_dir: '$WORKSPACE',
      // Use source cloned by Jenkins and not clone it second time (redundantly).
      beam_location: '$WORKSPACE/src',
      // Publishes results with official tag, for use in dashboards.
      official: 'true'
    ]
    // Note: in case of key collision, keys present in ArgMap win.
    LinkedHashMap<String, String> joinedArgs = standardArgs.plus(argMap)
    return mapToArgString(joinedArgs)
  }

  static def setupKubernetes(def context, def namespace, def kubeconfigLocation) {
    context.steps {
      shell('gcloud container clusters get-credentials io-datastores --zone=us-central1-a --verbosity=debug')
      shell("cp /home/jenkins/.kube/config ${kubeconfigLocation}")

      shell("kubectl --kubeconfig=${kubeconfigLocation} create namespace ${namespace}")
      shell("kubectl --kubeconfig=${kubeconfigLocation} config set-context \$(kubectl config current-context) --namespace=${namespace}")
    }
  }

  static def cleanupKubernetes(def context, def namespace, def kubeconfigLocation) {
    context.steps {
      shell("kubectl --kubeconfig=${kubeconfigLocation} delete namespace ${namespace}")
      shell("rm ${kubeconfigLocation}")
    }
  }

  // Namespace must contain lower case alphanumeric characters or '-'
  static String getKubernetesNamespace(def jobName) {
    jobName = jobName.replaceAll("_", "-").toLowerCase()
    return "${jobName}-\${BUILD_ID}"
  }

  static String getKubeconfigLocationForNamespace(def namespace) {
    return '"$WORKSPACE/' + "config-${namespace}" + '"'
  }

  // Adds the standard performance test job steps.
  static def buildPerformanceTest(def context, def argMap) {
    def pkbArgs = genPerformanceArgs(argMap)

    // Absolute path of project root and virtualenv path of Perfkit.
    def perfkit_root = makePathAbsolute("PerfKitBenchmarker")
    def perfkit_env = makePathAbsolute("env/.perfkit_env")

    context.steps {
        // Clean up environment.
        shell("rm -rf ${perfkit_root}")
        shell("rm -rf ${perfkit_env}")

        // create new VirtualEnv for Perfkit framework. Explicitly pin to python2.7
        // here otherwise python3 is used by default.
        shell("virtualenv ${perfkit_env} --python=python2.7")

        // update setuptools and pip
        shell("${perfkit_env}/bin/pip install --upgrade setuptools pip")

        // Clone appropriate perfkit branch
        shell("git clone https://github.com/GoogleCloudPlatform/PerfKitBenchmarker.git ${perfkit_root}")

        // Install Perfkit benchmark requirements.
        shell("${perfkit_env}/bin/pip install -r ${perfkit_root}/requirements.txt")

        // Launch performance test.
        shell("${perfkit_env}/bin/python ${perfkit_root}/pkb.py ${pkbArgs}")
    }
  }

  /**
   * Transforms pipeline options to a string of format like below:
   * ["--pipelineOption=123", "--pipelineOption2=abc", ...]
   *
   * @param pipelineOptions A map of pipeline options.
   */
  static String joinPipelineOptions(Map pipelineOptions) {
    List<String> pipelineArgList = []
    pipelineOptions.each({
      key, value -> pipelineArgList.add("\"--$key=$value\"")
    })
    return "[" + pipelineArgList.join(',') + "]"
  }


  /**
   * Returns absolute path to beam project's files.
   * @param path A relative path to project resource.
   */
  static String makePathAbsolute(String path) {
    return '"$WORKSPACE/' + path + '"'
  }
}
