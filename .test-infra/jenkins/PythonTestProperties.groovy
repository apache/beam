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

class PythonTestProperties {
  // Indicates all supported Python versions.
  // This must be sorted in ascending order.
  final static List<String> ALL_SUPPORTED_VERSIONS = [
    '3.9',
    '3.10',
    '3.11',
    '3.12'
  ]
  final static List<String> SUPPORTED_CONTAINER_TASKS = ALL_SUPPORTED_VERSIONS.collect {
    "py${it.replace('.', '')}"
  }
  final static String LOWEST_SUPPORTED = ALL_SUPPORTED_VERSIONS[0]
  final static String HIGHEST_SUPPORTED = ALL_SUPPORTED_VERSIONS[-1]
  final static List<String> ESSENTIAL_VERSIONS = [
    LOWEST_SUPPORTED,
    HIGHEST_SUPPORTED
  ]
  final static List<String> CROSS_LANGUAGE_VALIDATES_RUNNER_PYTHON_VERSIONS = ESSENTIAL_VERSIONS
  final static List<String> CROSS_LANGUAGE_VALIDATES_RUNNER_DATAFLOW_USING_SQL_PYTHON_VERSIONS = [HIGHEST_SUPPORTED]
  final static List<String> VALIDATES_CONTAINER_DATAFLOW_PYTHON_VERSIONS = ALL_SUPPORTED_VERSIONS
  final static String LOAD_TEST_PYTHON_VERSION = '3.9'
  final static String RUN_INFERENCE_TEST_PYTHON_VERSION = '3.9'
  final static String CHICAGO_TAXI_EXAMPLE_FLINK_PYTHON_VERSION = '3.9'
  // Use for various shell scripts triggered by Jenkins.
  // Gradle scripts should use project.ext.pythonVersion defined by PythonNature/BeamModulePlugin.
  final static String DEFAULT_INTERPRETER = 'python3.9'
}
