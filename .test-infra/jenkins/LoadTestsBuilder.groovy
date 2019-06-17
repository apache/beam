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

class LoadTestsBuilder {
  static void loadTests(scope, CommonTestProperties.SDK sdk, List testConfigurations, TriggeringContext triggeringContext, String test, String mode){
    scope.description("Runs ${sdk.toString().toLowerCase().capitalize()} ${test} load tests in ${mode} mode")

    commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

    for (testConfiguration in testConfigurations) {
        loadTest(scope, testConfiguration.title, testConfiguration.runner, sdk, testConfiguration.jobProperties, testConfiguration.itClass, triggeringContext)
    }
  }


  static void loadTest(context, String title, Runner runner, SDK sdk, Map<String, ?> options, String mainClass, TriggeringContext triggeringContext) {
    options.put('runner', runner.option)

    String datasetKey = 'bigQueryDataset'
    String datasetValue = options.get(datasetKey)

    if (datasetValue) {
      options.put(datasetKey, setContextualDatasetName(datasetValue, triggeringContext))
    }

    context.steps {
      shell("echo *** ${title} ***")
      gradle {
        rootBuildScriptDir(commonJobProperties.checkoutDir)
        tasks(getGradleTaskName(sdk))
        commonJobProperties.setGradleSwitches(delegate)
        switches("-PloadTest.mainClass=\"${mainClass}\"")
        switches("-Prunner=${runner.getDepenedencyBySDK(sdk)}")
        switches("-PloadTest.args=\"${parseOptions(options)}\"")
      }
    }
  }

  private static String getGradleTaskName(SDK sdk) {
    if (sdk == SDK.JAVA) {
      return ':beam:sdks:java:load-tests:run'
    } else if (sdk == SDK.PYTHON) {
      return ':sdks:python:apache_beam:testing:load-tests:run'
    } else {
      throw new RuntimeException("No task name defined for SDK: $SDK")
    }
  }

  private static String parseOptions(Map<String, ?> options) {
    options.collect {
      "--${it.key}=$it.value".replace('\"', '\\\"').replace('\'', '\\\'')
    }.join(' ')
  }

  private static String setContextualDatasetName(String baseName, TriggeringContext triggeringContext) {
    if (triggeringContext == TriggeringContext.PR) {
      return baseName + '_PRs'
    } else {
      return baseName
    }
  }
}



