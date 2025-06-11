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

import os
import sys


def get_relative_path(path):
  """Returns the path of filename relative to the first directory in sys.path
  contained in filename. Returns the unchanged filename if it is not in any
  sys.path directory.

  Args:
    path: The path to the file.
  """
  for dir_path in sys.path:
    # The path for /aaa/bbb/c.py is relative to /aaa/bbb and not /aaa/bb.
    if not dir_path.endswith(os.path.sep):
      dir_path += os.path.sep
    if path.startswith(dir_path):
      return os.path.relpath(path, dir_path)
  return path


def get_normalized_path(path):
  """Returns a normalized path. This function is intended to be overridden."""
  # Use relative paths to make pickling lambdas deterministic for google3
  # This is needed only for code running inside Google on borg.
  if '/borglet/' in path:
    return get_relative_path(path)

  return path
