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
import CommonTestProperties.Runner
import CommonTestProperties.SDK
import CommonTestProperties.TriggeringContext
import InfluxDBCredentialsHelper
import NexmarkDatabaseProperties

// Class for building NEXMark jobs and suites.
class NexmarkBuilder {

  private static Map<String, Object> defaultOptions = [
    'manageResources': false,
    'monitorJobs'    : true,
  ] << NexmarkDatabaseProperties.nexmarkBigQueryArgs << NexmarkDatabaseProperties.nexmarkInfluxDBArgs

  static void standardJob(context, Runner runner, SDK sdk, Map<String, Object> jobSpecificOptions, TriggeringContext triggeringContext) {
    Map<String, Object> options = getFullOptions(jobSpecificOptions, runner, triggeringContext)

    options.put('streaming', false)
    suite(context, "NEXMARK IN BATCH MODE USING ${runner} RUNNER", runner, sdk, options)

    options.put('streaming', true)
    suite(context, "NEXMARK IN STREAMING MODE USING ${runner} RUNNER", runner, sdk, options)

    options.put('queryLanguage', 'sql')

    options.put('streaming', false)
    suite(context, "NEXMARK IN SQL BATCH MODE USING ${runner} RUNNER", runner, sdk, options)

    options.put('streaming', true)
    suite(context, "NEXMARK IN SQL STREAMING MODE USING ${runner} RUNNER", runner, sdk, options)

    options.put('queryLanguage', 'zetasql')

    options.put('streaming', false)
    suite(context, "NEXMARK IN ZETASQL BATCH MODE USING ${runner} RUNNER", runner, sdk, options)

    options.put('streaming', true)
    suite(context, "NEXMARK IN ZETASQL STREAMING MODE USING ${runner} RUNNER", runner, sdk, options)
  }

  static void batchOnlyJob(context, Runner runner, SDK sdk, Map<String, Object> jobSpecificOptions, TriggeringContext triggeringContext) {
    Map<String, Object> options = getFullOptions(jobSpecificOptions, runner, triggeringContext)

    options.put('streaming', false)
    suite(context, "NEXMARK IN BATCH MODE USING ${runner} RUNNER", runner, sdk, options)

    options.put('queryLanguage', 'sql')
    suite(context, "NEXMARK IN SQL BATCH MODE USING ${runner} RUNNER", runner, sdk, options)

    options.put('queryLanguage', 'zetasql')
    suite(context, "NEXMARK IN ZETASQL BATCH MODE USING ${runner} RUNNER", runner, sdk, options)
  }

  private
  static Map<String, Object> getFullOptions(Map<String, Object> jobSpecificOptions, Runner runner, TriggeringContext triggeringContext) {
    Map<String, Object> options = defaultOptions + jobSpecificOptions

    options.put('runner', runner.option)
    options.put('bigQueryDataset', determineStorageName(triggeringContext))
    options.put('baseInfluxMeasurement', determineStorageName(triggeringContext))
    options
  }


  static void suite(context, String title, Runner runner, SDK sdk, Map<String, Object> options) {
    InfluxDBCredentialsHelper.useCredentials(context)
    context.steps {
      shell("echo \"*** RUN ${title} ***\"")
      gradle {
        rootBuildScriptDir(commonJobProperties.checkoutDir)
        tasks(':sdks:java:testing:nexmark:run')
        commonJobProperties.setGradleSwitches(delegate)
        switches("-Pnexmark.runner=${runner.getDependencyBySDK(sdk)}")
        switches("-Pnexmark.args=\"${parseOptions(options)}\"")
      }
    }
  }

  private static String parseOptions(Map<String, Object> options) {
    options.collect { "--${it.key}=${it.value.toString()}" }.join(' ')
  }

  private static String determineStorageName(TriggeringContext triggeringContext) {
    triggeringContext == TriggeringContext.PR ? "nexmark_PRs" : "nexmark"
  }
}
