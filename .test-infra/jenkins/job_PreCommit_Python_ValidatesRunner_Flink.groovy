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

import PrecommitJobBuilder

// This job runs the suite of Python ValidatesRunner tests against the Flink runner on Python 2.
PrecommitJobBuilder builder = new PrecommitJobBuilder(
    scope: this,
    nameBase: 'Python2_PVR_Flink',
    gradleTask: ':sdks:python:test-suites:portable:py2:flinkValidatesRunner',
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
      // Test regressions of cross-language KafkaIO test
      '^sdks/java/io/kafka/.*$',
    ]
    )
builder.build {
  previousNames('beam_PreCommit_Python_PVR_Flink')
}
