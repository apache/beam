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

class LoadTestsBuilder {
  final static String DOCKER_CONTAINER_REGISTRY = 'gcr.io/apache-beam-testing/beam_portability'

  static void loadTests(scope, CommonTestProperties.SDK sdk, List testConfigurations, String test, String mode){
    scope.description("Runs ${sdk.toString().toLowerCase().capitalize()} ${test} load tests in ${mode} mode")

    commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

    for (testConfiguration in testConfigurations) {
      loadTest(scope, testConfiguration.title, testConfiguration.runner, sdk, testConfiguration.pipelineOptions,
          testConfiguration.test, testConfiguration.withDataflowWorkerJar ?: false)
    }
  }


  static void loadTest(context, String title, Runner runner, SDK sdk, Map<String, ?> options,
      String mainClass, Boolean withDataflowWorkerJar = false) {
    options.put('runner', runner.option)
    InfluxDBCredentialsHelper.useCredentials(context)

    context.steps {
      shell("echo \"*** ${title} ***\"")
      gradle {
        rootBuildScriptDir(commonJobProperties.checkoutDir)
        setGradleTask(delegate, runner, sdk, options, mainClass, withDataflowWorkerJar)
        commonJobProperties.setGradleSwitches(delegate)
      }
    }
  }

  static String parseOptions(Map<String, ?> options) {
    options.collect { entry ->
      // Flags are indicated by null values
      if (entry.value == null) {
        "--${entry.key}"
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
      String mainClass, Boolean withDataflowWorkerJar) {
    context.tasks(getGradleTaskName(sdk))
    context.switches("-PloadTest.mainClass=\"${mainClass}\"")
    context.switches("-Prunner=${runner.getDependencyBySDK(sdk)}")
    context.switches("-PwithDataflowWorkerJar=\"${withDataflowWorkerJar}\"")
    context.switches("-PloadTest.args=\"${parseOptions(options)}\"")


    if (sdk == SDK.PYTHON_37) {
      context.switches("-PpythonVersion=3.7")
    }
  }

  private static String getGradleTaskName(SDK sdk) {
    if (sdk == SDK.JAVA) {
      return ':sdks:java:testing:load-tests:run'
    } else if (sdk == SDK.PYTHON || sdk == SDK.PYTHON_37) {
      return ':sdks:python:apache_beam:testing:load_tests:run'
    } else {
      throw new RuntimeException("No task name defined for SDK: $SDK")
    }
  }
}



