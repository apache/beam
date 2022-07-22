#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Defines constants and helper methods used by the dependency_report_generator

import os

class ReportGeneratorConfig:

  # Jenkins Working Space
  WORKING_SPACE = os.environ['WORKSPACE']

  # Constants for dependency prioritization
  GCLOUD_PROJECT_ID     = 'apache-beam-testing'
  DATASET_ID            = 'beam_dependency_states'
  PYTHON_DEP_TABLE_ID   = 'python_dependency_states'
  JAVA_DEP_TABLE_ID     = 'java_dependency_states'
  PYTHON_DEP_RAW_REPORT = WORKING_SPACE + '/src/build/dependencyUpdates/python_dependency_report.txt'
  JAVA_DEP_RAW_REPORT   = WORKING_SPACE + '/src/build/dependencyUpdates/report.txt'
  FINAL_REPORT          = WORKING_SPACE + '/src/build/dependencyUpdates/beam-dependency-check-report.html'
  MAX_STALE_DAYS = 360
  MAX_MINOR_VERSION_DIFF = 3
  PYPI_URL = "https://pypi.org/project/"
  MAVEN_CENTRAL_URL = "https://mvnrepository.com/artifact"

  # Dependency Owners
  JAVA_DEP_OWNERS       = WORKING_SPACE + '/src/ownership/JAVA_DEPENDENCY_OWNERS.yaml'
  PYTHON_DEP_OWNERS     = WORKING_SPACE + '/src/ownership/PYTHON_DEPENDENCY_OWNERS.yaml'


  @classmethod
  def get_bigquery_table_id(cls, sdk_type):
    if sdk_type.lower() == 'java':
      return cls.JAVA_DEP_TABLE_ID
    elif sdk_type.lower() == 'python':
      return cls.PYTHON_DEP_TABLE_ID
    else:
      raise UndefinedSDKTypeException("""Undefined SDK Type: {0}. 
        Could not find the BigQuery table for {1} dependencies.""".format(sdk_type, sdk_type))


  @classmethod
  def get_raw_report(cls, sdk_type):
    if sdk_type.lower() == 'java':
      return cls.JAVA_DEP_RAW_REPORT
    elif sdk_type.lower() == 'python':
      return cls.PYTHON_DEP_RAW_REPORT
    else:
      raise UndefinedSDKTypeException("""Undefined SDK Type: {0}. 
        Could not find the dependency reports for the {1} SDK.""".format(sdk_type, sdk_type))


  @classmethod
  def get_owners_file(cls, sdk_type):
    if sdk_type.lower() == 'java':
      return cls.JAVA_DEP_OWNERS
    elif sdk_type.lower() == 'python':
      return cls.PYTHON_DEP_OWNERS
    else:
      raise UndefinedSDKTypeException("""Undefined SDK Type: {0}. 
        Could not find the Owners file for the {1} SDK.""".format(sdk_type, sdk_type))


class UndefinedSDKTypeException(Exception):
  """Indicates an error has occurred in while reading constants."""

  def __init__(self, msg):
    super(UndefinedSDKTypeException, self).__init__(msg)
