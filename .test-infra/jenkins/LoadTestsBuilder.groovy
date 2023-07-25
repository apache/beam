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
import static PythonTestProperties.LOAD_TEST_PYTHON_VERSION

class LoadTestsBuilder {
  final static String DOCKER_CONTAINER_REGISTRY = 'gcr.io/apache-beam-testing/beam-sdk'
  final static String GO_SDK_CONTAINER = "${DOCKER_CONTAINER_REGISTRY}/beam_go_sdk:latest"
  final static String DOCKER_BEAM_SDK_IMAGE = "beam_python${LOAD_TEST_PYTHON_VERSION}_sdk:latest"
  final static String DOCKER_BEAM_JOBSERVER = 'gcr.io/apache-beam-testing/beam_portability'

  static void loadTests(scope, CommonTestProperties.SDK sdk, List testConfigurations, String test, String mode,
      List<String> jobSpecificSwitches = null) {
    scope.description("Runs ${sdk.toString().toLowerCase().capitalize()} ${test} load tests in ${mode} mode")

    commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 720)

    for (testConfiguration in testConfigurations) {
      loadTest(scope, testConfiguration.title, testConfiguration.runner, sdk, testConfiguration.pipelineOptions,
          testConfiguration.test, jobSpecificSwitches)
    }
  }


  static void loadTest(context, String title, Runner runner, SDK sdk, Map<String, ?> options,
      String mainClass, List<String> jobSpecificSwitches = null, String requirementsTxtFile = null,
      String pythonVersion = null) {
    options.put('runner', runner.option)
    InfluxDBCredentialsHelper.useCredentials(context)

    context.steps {
      shell("echo \"*** ${title} ***\"")
      gradle {
        rootBuildScriptDir(commonJobProperties.checkoutDir)
        setGradleTask(delegate, runner, sdk, options, mainClass,
            jobSpecificSwitches, requirementsTxtFile, pythonVersion)
        commonJobProperties.setGradleSwitches(delegate)
      }
    }
  }

  static String parseOptions(Map<String, ?> options) {
    options.collect { entry ->

      if (entry.key.matches(".*\\s.*")) {
        throw new IllegalArgumentException("""
          Encountered invalid option name '${entry.key}'. Names must not
          contain whitespace.
          """)
      }

      // Flags are indicated by null values
      if (entry.value == null) {
        "--${entry.key}"
      } else if (entry.value.toString().matches(".*\\s.*") &&
      !entry.value.toString().matches("'[^']*'")) {
        throw new IllegalArgumentException("""
          Option '${entry.key}' has an invalid value, '${entry.value}'. Values
          must not contain whitespace, or they must be wrapped in singe quotes.
          """)
      } else {
        "--${entry.key}=$entry.value".replace('\"', '\\\"').replace('\'', '\\\'')
      }
    }.join(' ')
  }

  static String getBigQueryDataset(String baseName, TriggeringContext triggeringContext) {
    if (triggeringContext == TriggeringContext.PR) {
      return baseName + '_PRs'
    } else {
      return baseName
    }
  }

  private static void setGradleTask(context, Runner runner, SDK sdk, Map<String, ?> options,
      String mainClass, List<String> jobSpecificSwitches, String requirementsTxtFile = null,
      String pythonVersion = null) {
    context.tasks(getGradleTaskName(sdk))
    context.switches("-PloadTest.mainClass=\"${mainClass}\"")
    context.switches("-Prunner=${runner.getDependencyBySDK(sdk)}")
    context.switches("-PloadTest.args=\"${parseOptions(options)}\"")
    if (requirementsTxtFile != null){
      context.switches("-PloadTest.requirementsTxtFile=\"${requirementsTxtFile}\"")
    }
    if (jobSpecificSwitches != null) {
      jobSpecificSwitches.each {
        context.switches(it)
      }
    }

    if (sdk == SDK.PYTHON) {
      if (pythonVersion == null) {
        context.switches("-PpythonVersion=${LOAD_TEST_PYTHON_VERSION}")
      }
      else {
        context.switches("-PpythonVersion=${pythonVersion}")
      }
    }
  }

  private static String getGradleTaskName(SDK sdk) {
    switch (sdk) {
      case SDK.JAVA:
        return ':sdks:java:testing:load-tests:run'
      case SDK.PYTHON:
        return ':sdks:python:apache_beam:testing:load_tests:run'
      case SDK.GO:
        return ':sdks:go:test:load:run'
      default:
        throw new RuntimeException("No task name defined for SDK: $SDK")
    }
  }
}



