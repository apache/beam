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

/** This class defines PrecommitJobBuilder.build() helper for defining pre-comit jobs. */
class PrecommitJobBuilder {
  /** scope 'this' parameter from top-level script; used for binding Job DSL methods. */
  Object scope

  /** Base name for each post-commit suite job, i.e. 'Go'. */
  String nameBase

  /**  The Gradle task to execute. */
  String gradleTask

  /** If defined, set of additional switches to pass to Gradle. */
  List<String> gradleSwitches = []

  /** Overall job timeout. */
  int timeoutMins = 120

  /** If defined, set of path expressions used to trigger the job on commit. */
  List<String> triggerPathPatterns = []

  /** If defined, set of path expressions to not trigger the job on commit. */
  List<String> excludePathPatterns = []

  /** Whether to trigger on new PR commits. Useful to set to false when testing new jobs. */
  boolean commitTriggering = true

  /**
   * Define a set of pre-commit jobs.
   *
   * @param additionalCustomization Job DSL closure with additional customization to apply to the job.
   */
  void build(Closure additionalCustomization = {}) {
    defineCronJob additionalCustomization
    if (commitTriggering) {
      defineCommitJob additionalCustomization
    }
    definePhraseJob additionalCustomization
  }

  /** Create a pre-commit job which runs on a daily schedule. */
  private void defineCronJob(Closure additionalCustomization) {
    def job = createBaseJob 'Cron'
    job.with {
      description buildDescription('on a daily schedule.')
      commonJobProperties.setAutoJob delegate
    }
    job.with additionalCustomization
  }

  /** Create a pre-commit job which runs on every commit to a PR. */
  private void defineCommitJob(Closure additionalCustomization) {
    def job = createBaseJob 'Commit', true
    def defaultPathTriggers = [
      '^build.gradle$',
      '^buildSrc/.*$',
      '^gradle/.*$',
      '^gradle.properties$',
      '^gradlew$',
      '^gradle.bat$',
      '^settings.gradle$'
    ]
    if (triggerPathPatterns) {
      triggerPathPatterns.addAll defaultPathTriggers
    }
    job.with {
      description buildDescription('for each commit push.')
      concurrentBuild()
      commonJobProperties.setPullRequestBuildTrigger(delegate,
          githubUiHint(),
          '',
          false,
          true,
          triggerPathPatterns,
          excludePathPatterns)
    }
    job.with additionalCustomization
  }

  private void definePhraseJob(Closure additionalCustomization) {
    def job = createBaseJob 'Phrase'
    job.with {
      description buildDescription("on trigger phrase '${buildTriggerPhrase()}'.")
      concurrentBuild()
      commonJobProperties.setPullRequestBuildTrigger delegate, githubUiHint(), buildTriggerPhrase()
    }
    job.with additionalCustomization
  }

  private Object createBaseJob(nameSuffix, usesRegionFilter = false) {
    def allowRemotePoll = !usesRegionFilter
    return scope.job("beam_PreCommit_${nameBase}_${nameSuffix}") {
      commonJobProperties.setTopLevelMainJobProperties(delegate,
          'master',
          timeoutMins,
          allowRemotePoll) // needed for included regions PR triggering; see [JENKINS-23606]
      steps {
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(gradleTask)
          gradleSwitches.each { switches(it) }
          commonJobProperties.setGradleSwitches(delegate)
        }
      }
    }
  }

  /** The magic phrase used to trigger the job when posted as a PR comment. */
  private String buildTriggerPhrase() {
    return "Run ${nameBase} PreCommit"
  }

  /** A human-readable description which will be used as the base of all suite jobs. */
  private buildDescription(String triggerDescription) {
    return "Runs ${nameBase} PreCommit tests ${triggerDescription}"
  }

  private String githubUiHint() {
    "${nameBase} (\"${buildTriggerPhrase()}\")"
  }
}
