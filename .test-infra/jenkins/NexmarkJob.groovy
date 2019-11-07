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

import javaposse.jobdsl.dsl.Job
import CommonJobProperties as commonJobProperties
import CommonTestProperties.Runner
import CommonTestProperties.SDK
import CommonTestProperties.TriggeringContext

class NexmarkJob {

  private static final String JAVA_11_HOME = '/usr/lib/jvm/java-11-openjdk-amd64'
  private static final String JAVA_8_HOME = '/usr/lib/jvm/java-8-openjdk-amd64'

  private static Map<String, Object> DEFAULT_OPTIONS = [
      'bigQueryTable'          : 'nexmark',
      'project'                : 'apache-beam-testing',
      'resourceNameMode'       : 'QUERY_RUNNER_AND_MODE',
      'exportSummaryToBigQuery': true,
      'tempLocation'           : 'gs://temp-storage-for-perf-tests/nexmark',
      'manageResources'        : false,
      'monitorJobs'            : true
  ]

  private Job job

  private Runner runner

  private SDK sdk

  private TriggeringContext triggeringContext

  private String javaRuntimeVersion = "1.8"

  NexmarkJob(Job job, Runner runner, SDK sdk, TriggeringContext context) {
    this.job = job
    this.runner = runner
    this.sdk = sdk
    this.triggeringContext = context
  }

  void standardJob(Map<String, Object> jobSpecificOptions) {
    Map<String, Object> options = fullOptions(jobSpecificOptions)

    options.put('streaming', false)
    suite("NEXMARK IN BATCH MODE USING ${runner} RUNNER", options)

    options.put('streaming', true)
    suite("NEXMARK IN STREAMING MODE USING ${runner} RUNNER", options)

    options.put('queryLanguage', 'sql')

    options.put('streaming', false)
    suite("NEXMARK IN SQL BATCH MODE USING ${runner} RUNNER", options)

    options.put('streaming', true)
    suite("NEXMARK IN SQL STREAMING MODE USING ${runner} RUNNER", options)
  }

  void batchOnlyJob(Map<String, Object> jobSpecificOptions) {
    Map<String, Object> options = fullOptions(jobSpecificOptions)
    options.put('streaming', false)

    suite("NEXMARK IN BATCH MODE USING ${runner} RUNNER", options)

    options.put('queryLanguage', 'sql')
    suite("NEXMARK IN SQL BATCH MODE USING ${runner} RUNNER", options)
  }

  private static Map<String, Object> fullOptions(Map<String, Object> jobSpecificOptions) {
    def options = (DEFAULT_OPTIONS + jobSpecificOptions)
    options.put('bigQueryDataset', TriggeringContext.PR ? "nexmark_PRs" : "nexmark")
    options
  }

  void suite(String title, Map<String, Object> options) {
    if (javaRuntimeVersion == "11") {
      setupSuiteForJava11(title, options)
    } else {
      setupSuite(title, options)
    }
  }

  // TODO: To be removed once we achieve full Java 11 support
  private void setupSuiteForJava11(String title, Map<String, Object> options) {
    job.steps {
      shell("echo *** RUN ${title} ***")

      // Build with Java 8
      gradle {
        rootBuildScriptDir(commonJobProperties.checkoutDir)
        tasks(':sdks:java:testing:nexmark:jar')
        commonJobProperties.setGradleSwitches(delegate)
        switches("-Pnexmark.runner=${runner.getDepenedencyBySDK(sdk)}")
        switches("-xtest")
        switches("-Dorg.gradle.java.home=${JAVA_8_HOME}")
      }

      // Run with Java 11
      gradle {
        rootBuildScriptDir(commonJobProperties.checkoutDir)
        tasks(':sdks:java:testing:nexmark:run')
        commonJobProperties.setGradleSwitches(delegate)
        switches("-Pnexmark.runner=${runner.getDepenedencyBySDK(sdk)}")
        switches("-Pnexmark.args=\"${parseOptions(options)}\"")
        switches('-x shadowJar')
        switches('-x shadowTestJar')
        switches('-x compileJava')
        switches('-x compileTestJava')
        switches("-Djava.specification.version=${javaRuntimeVersion}")
        switches("-Dorg.gradle.java.home=${JAVA_11_HOME}")
      }
    }
  }

  private void setupSuite(String title, Map<String, Object> options) {
    job.steps {
      shell("echo *** RUN ${title} ***")
      gradle {
        rootBuildScriptDir(commonJobProperties.checkoutDir)
        tasks(':sdks:java:testing:nexmark:run')
        commonJobProperties.setGradleSwitches(delegate)
        switches("-Pnexmark.runner=${runner.getDepenedencyBySDK(sdk)}")
        switches("-Pnexmark.args=\"${parseOptions(options)}\"")
      }
    }
  }

  private static String parseOptions(Map<String, Object> options) {
    options.collect { "--${it.key}=${it.value.toString()}" }.join(' ')
  }
}