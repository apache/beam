#!/usr/bin/env python
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

from dependency_check.report_generator_config import ReportGeneratorConfig

def compare_dependency_versions(curr_ver, latest_ver):
  """
  Compare the current using version and the latest version.
  Return true if a major version change was found, or 3 minor versions that the current version is behind.
  Args:
    curr_ver
    latest_ver
  Return:
    boolean
  """
  if curr_ver is None or latest_ver is None:
    return True
  else:
    curr_ver_splitted = curr_ver.split('.')
    latest_ver_splitted = latest_ver.split('.')
    curr_major_ver = curr_ver_splitted[0]
    latest_major_ver = latest_ver_splitted[0]
    # compare major versions
    if curr_major_ver != latest_major_ver:
      return True
    # compare minor versions
    else:
      curr_minor_ver = curr_ver_splitted[1] if len(curr_ver_splitted) > 1 else None
      latest_minor_ver = latest_ver_splitted[1] if len(latest_ver_splitted) > 1 else None
      if curr_minor_ver is not None and latest_minor_ver is not None:
        if (not curr_minor_ver.isdigit() or not latest_minor_ver.isdigit()) and curr_minor_ver != latest_minor_ver:
          return True
        elif int(curr_minor_ver) + ReportGeneratorConfig.MAX_MINOR_VERSION_DIFF <= int(latest_minor_ver):
          return True
    # TODO: Comparing patch versions if needed.
  return False
