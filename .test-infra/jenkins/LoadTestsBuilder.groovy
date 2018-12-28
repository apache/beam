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

// Class for building Load Tests jobs and suites
class LoadTestsBuilder {

    private static Map<String, Object> defaultOptions = [
            project             :'apache-beam-testing',
            tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
    ]

    static void buildTest(context, String title, Runner runner, Map<String, Object> jobSpecificOptions, String mainClass) {
        Map<String, Object> options = jobSpecificOptions + defaultOptions
        options.put('runner', runner.option)

        suite(context, title, runner, options, mainClass)
    }

    static void suite(context, String title, Runner runner, Map<String, Object> options, String mainClass) {
        context.steps {
            shell("echo *** ${title} ***")
            gradle {
                rootBuildScriptDir(commonJobProperties.checkoutDir)
                tasks(':beam-sdks-java-load-tests:run')
                commonJobProperties.setGradleSwitches(delegate)
                switches("-PloadTest.mainClass=\"${mainClass}\"")
                switches("-Prunner=${runner.dependency}")
                switches("-PloadTest.args=\"${parseOptions(options)}\"")
            }
        }
    }

    private static String parseOptions(Map<String, Object> options) {
        options.collect { "--${it.key}=${it.value.toString()}".replace('\"', '\\"') }.join(' ')
    }
}