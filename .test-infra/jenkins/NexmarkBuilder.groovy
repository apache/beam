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
  final static String DEFAULT_JAVA_RUNTIME_VERSION = "1.8";
  final static String JAVA_11_RUNTIME_VERSION = "11";
  final static String JAVA_17_RUNTIME_VERSION = "17";

  private static Map<String, Object> defaultOptions = [
    'manageResources': false,
    'monitorJobs'    : true,
  ] << NexmarkDatabaseProperties.nexmarkBigQueryArgs << NexmarkDatabaseProperties.nexmarkInfluxDBArgs

  static void standardJob(context, Runner runner, SDK sdk, Map<String, Object> jobSpecificOptions, TriggeringContext triggeringContext) {
    standardJob(context, runner, sdk, jobSpecificOptions, triggeringContext, null, DEFAULT_JAVA_RUNTIME_VERSION);
  }

  static void standardJob(context, Runner runner, SDK sdk, Map<String, Object> jobSpecificOptions, TriggeringContext triggeringContext, List<String> jobSpecificSwitches, String javaRuntimeVersion) {
    Map<String, Object> options = getFullOptions(jobSpecificOptions, runner, triggeringContext)

    options.put('streaming', false)
    suite(context, "NEXMARK IN BATCH MODE USING ${runner} RUNNER", runner, sdk, options, jobSpecificSwitches, javaRuntimeVersion)

    options.put('streaming', true)
    suite(context, "NEXMARK IN STREAMING MODE USING ${runner} RUNNER", runner, sdk, options, jobSpecificSwitches, javaRuntimeVersion)

    options.put('queryLanguage', 'sql')

    options.put('streaming', false)
    suite(context, "NEXMARK IN SQL BATCH MODE USING ${runner} RUNNER", runner, sdk, options, jobSpecificSwitches, javaRuntimeVersion)

    options.put('streaming', true)
    suite(context, "NEXMARK IN SQL STREAMING MODE USING ${runner} RUNNER", runner, sdk, options, jobSpecificSwitches, javaRuntimeVersion)

    options.put('queryLanguage', 'zetasql')

    options.put('streaming', false)
    suite(context, "NEXMARK IN ZETASQL BATCH MODE USING ${runner} RUNNER", runner, sdk, options, jobSpecificSwitches, javaRuntimeVersion)

    options.put('streaming', true)
    suite(context, "NEXMARK IN ZETASQL STREAMING MODE USING ${runner} RUNNER", runner, sdk, options, jobSpecificSwitches, javaRuntimeVersion)
  }

  static void nonQueryLanguageJobs(context, Runner runner, SDK sdk, Map<String, Object> jobSpecificOptions, TriggeringContext triggeringContext, List<String> jobSpecificSwitches, String javaRuntimeVersion) {
    Map<String, Object> options = getFullOptions(jobSpecificOptions, runner, triggeringContext)

    options.put('streaming', false)
    suite(context, "NEXMARK IN BATCH MODE USING ${runner} RUNNER", runner, sdk, options, jobSpecificSwitches, javaRuntimeVersion)

    options.put('streaming', true)
    suite(context, "NEXMARK IN STREAMING MODE USING ${runner} RUNNER", runner, sdk, options, jobSpecificSwitches, javaRuntimeVersion)
  }

  static void batchOnlyJob(context, Runner runner, SDK sdk, Map<String, Object> jobSpecificOptions, TriggeringContext triggeringContext) {
    Map<String, Object> options = getFullOptions(jobSpecificOptions, runner, triggeringContext)

    options.put('streaming', false)
    suite(context, "NEXMARK IN BATCH MODE USING ${runner} RUNNER", runner, sdk, options, null, DEFAULT_JAVA_RUNTIME_VERSION)

    options.put('queryLanguage', 'sql')
    suite(context, "NEXMARK IN SQL BATCH MODE USING ${runner} RUNNER", runner, sdk, options, null, DEFAULT_JAVA_RUNTIME_VERSION)

    options.put('queryLanguage', 'zetasql')
    suite(context, "NEXMARK IN ZETASQL BATCH MODE USING ${runner} RUNNER", runner, sdk, options, null, DEFAULT_JAVA_RUNTIME_VERSION)
  }

  static void standardPythonJob(context, Runner runner, SDK sdk, Map<String, Object> jobSpecificOptions, TriggeringContext triggeringContext) {
    Map<String, Object> options = getFullOptions(jobSpecificOptions, runner, triggeringContext)

    pythonSuite(context, "NEXMARK PYTHON IN BATCH MODE USING ${runner} RUNNER", runner, sdk, options)
  }


  private
  static Map<String, Object> getFullOptions(Map<String, Object> jobSpecificOptions, Runner runner, TriggeringContext triggeringContext) {
    Map<String, Object> options = defaultOptions + jobSpecificOptions

    options.put('runner', runner.option)
    options.put('bigQueryDataset', determineStorageName(triggeringContext))
    options.put('baseInfluxMeasurement', determineStorageName(triggeringContext))
    options
  }


  static void suite(context, String title, Runner runner, SDK sdk, Map<String, Object> options, List<String> jobSpecificSwitches, String javaRuntimeVersion) {

    if (javaRuntimeVersion == JAVA_11_RUNTIME_VERSION) {
      java11Suite(context, title, runner, sdk, options, jobSpecificSwitches)
    } else if (javaRuntimeVersion == JAVA_17_RUNTIME_VERSION) {
      java17Suite(context, title, runner, sdk, options, jobSpecificSwitches)
    } else if(javaRuntimeVersion == DEFAULT_JAVA_RUNTIME_VERSION){
      java8Suite(context, title, runner, sdk, options, jobSpecificSwitches)
    }
  }

  static void java8Suite(context, String title, Runner runner, SDK sdk, Map<String, Object> options, List<String> jobSpecificSwitches) {
    InfluxDBCredentialsHelper.useCredentials(context)
    context.steps {
      shell("echo \"*** RUN ${title} with Java 8 ***\"")
      gradle {
        rootBuildScriptDir(commonJobProperties.checkoutDir)
        tasks(':sdks:java:testing:nexmark:run')
        commonJobProperties.setGradleSwitches(delegate)
        switches("-Pnexmark.runner=${runner.getDependencyBySDK(sdk)}")
        switches("-Pnexmark.args=\"${parseOptions(options)}\"")
        if (jobSpecificSwitches != null) {
          jobSpecificSwitches.each {
            switches(it)
          }
        }
      }
    }
  }

  static void java11Suite(context, String title, Runner runner, SDK sdk, Map<String, Object> options, List<String> jobSpecificSwitches) {
    InfluxDBCredentialsHelper.useCredentials(context)
    context.steps {
      shell("echo \"*** RUN ${title} with Java 11***\"")

      // Run with Java 11
      gradle {
        rootBuildScriptDir(commonJobProperties.checkoutDir)
        tasks(':sdks:java:testing:nexmark:run')
        commonJobProperties.setGradleSwitches(delegate)
        switches("-PtestJavaVersion=11")
        switches("-Pjava11Home=${commonJobProperties.JAVA_11_HOME}")
        switches("-Pnexmark.runner=${runner.getDependencyBySDK(sdk)}")
        switches("-Pnexmark.args=\"${parseOptions(options)}\"")
        if (jobSpecificSwitches != null) {
          jobSpecificSwitches.each {
            switches(it)
          }
        }
      }
    }
  }

  static void java17Suite(context, String title, Runner runner, SDK sdk, Map<String, Object> options, List<String> jobSpecificSwitches) {
    InfluxDBCredentialsHelper.useCredentials(context)
    context.steps {
      shell("echo \"*** RUN ${title} with Java 17***\"")

      // Run with Java 17
      gradle {
        rootBuildScriptDir(commonJobProperties.checkoutDir)
        tasks(':sdks:java:testing:nexmark:run')
        commonJobProperties.setGradleSwitches(delegate)
        switches("-PtestJavaVersion=17")
        switches("-Pjava17Home=${commonJobProperties.JAVA_17_HOME}")
        switches("-Pnexmark.runner=${runner.getDependencyBySDK(sdk)}")
        switches("-Pnexmark.args=\"${parseOptions(options)}\"")
        if (jobSpecificSwitches != null) {
          jobSpecificSwitches.each {
            switches(it)
          }
        }
      }
    }
  }

  static void pythonSuite(context, String title, Runner runner, SDK sdk, Map<String, Object> options) {
    InfluxDBCredentialsHelper.useCredentials(context)

    for (int i = 0; i <= 12; i ++) {
      if (
      // https://github.com/apache/beam/issues/24678
      i == 1 ||
      // https://github.com/apache/beam/issues/24679
      i == 4 || i == 6 || i == 9 ||
      // https://github.com/apache/beam/issues/24680
      i == 12) {
        continue
      }
      pythonTest(context, title, i, runner, sdk, options)
    }
  }

  static void pythonTest(context, String title, int query, Runner runner, SDK sdk, Map<String, Object> options) {
    context.steps {
      shell("echo \"*** GENERATE events for ${title} query ${query} with Python***\"")

      options.put('query', query)

      // Matches defaults in NexmarkSuite.java
      if (query == 4 || query == 6 || query == 9) {
        options.put('numEvents', 10000)
      } else {
        options.put('numEvents', 100000)
      }

      String eventFile = options.get('tempLocation') + "/eventFiles/\${BUILD_TAG}/query${query}-"
      options.remove('input')
      options.put('generateEventFilePathPrefix', eventFile)

      gradle {
        rootBuildScriptDir(commonJobProperties.checkoutDir)
        tasks(':sdks:java:testing:nexmark:run')
        commonJobProperties.setGradleSwitches(delegate)
        switches("-Pnexmark.runner=:runners:direct-java")
        switches("-Pnexmark.args=\"${parseOptions(options)}\"")
      }

      shell("echo \"*** RUN ${title} query ${query} with Python***\"")

      options.remove('generateEventFilePathPrefix')
      options.put('input', eventFile + "\\*")

      gradle {
        rootBuildScriptDir(commonJobProperties.checkoutDir)
        tasks(':sdks:python:apache_beam:testing:benchmarks:nexmark:run')
        commonJobProperties.setGradleSwitches(delegate)
        switches("-Pnexmark.args=\"${parseOptionsPython(options)}\"")
      }
    }
  }

  private static String parseOptions(Map<String, Object> options) {
    options.collect { "--${it.key}=${it.value.toString()}" }.join(' ')
  }

  private static String parseOptionsPython(Map<String, Object> options) {
    options.collect {
      String key = it.key.toString().replaceAll("([a-z])([A-Z]+)", "\$1_\$2").toLowerCase()
      if (it.value == false) {
        return ""
      }
      if (it.value == true) {
        return "--${key}"
      }
      return "--${key}=${it.value}"
    }.join(' ')
  }

  private static String determineStorageName(TriggeringContext triggeringContext) {
    triggeringContext == TriggeringContext.PR ? "nexmark_PRs" : "nexmark"
  }
}
