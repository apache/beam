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
import PrecommitJobBuilder

PrecommitJobBuilder builder = new PrecommitJobBuilder(
    scope: this,
    nameBase: 'Portable_Python',
    gradleTask: ':clean',   // Do nothing here. Add test configs below.
    triggerPathPatterns: [
      '^model/.*$',
      '^runners/core-construction-java/.*$',
      '^runners/core-java/.*$',
      '^runners/extensions-java/.*$',
      '^runners/flink/.*$',
      '^runners/java-fn-execution/.*$',
      '^runners/reference/.*$',
      '^sdks/python/.*$',
      '^release/.*$',
    ]
    )

builder.build {
  // Due to BEAM-7993, run multiple Python version of portable precommit
  // tests in parallel could lead python3 container crash. We manually
  // config gradle steps here to run tests in sequential.
  steps {
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':sdks:python:test-suites:portable:py2:preCommitPy2')
      commonJobProperties.setGradleSwitches(delegate)
    }
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':sdks:python:test-suites:portable:py35:preCommitPy35')
      commonJobProperties.setGradleSwitches(delegate)
    }
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':sdks:python:test-suites:portable:py36:preCommitPy36')
      commonJobProperties.setGradleSwitches(delegate)
    }
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':sdks:python:test-suites:portable:py37:preCommitPy37')
      commonJobProperties.setGradleSwitches(delegate)
    }
  }
}
