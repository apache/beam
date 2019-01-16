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

"""Cross-platform utilities for creating subprocesses.

For internal use only; no backwards-compatibility guarantees.
"""

from __future__ import absolute_import

import platform
import subprocess

# On Windows, we need to use shell=True when creating subprocesses for binary
# paths to be resolved correctly.
force_shell = platform.system() == 'Windows'

# We mimic the interface of the standard Python subprocess module.
PIPE = subprocess.PIPE
STDOUT = subprocess.STDOUT
CalledProcessError = subprocess.CalledProcessError


def call(*args, **kwargs):
  if force_shell:
    kwargs['shell'] = True
  return subprocess.call(*args, **kwargs)


def check_call(*args, **kwargs):
  if force_shell:
    kwargs['shell'] = True
  return subprocess.check_call(*args, **kwargs)


def check_output(*args, **kwargs):
  if force_shell:
    kwargs['shell'] = True
  return subprocess.check_output(*args, **kwargs)


def Popen(*args, **kwargs):  # pylint: disable=invalid-name
  if force_shell:
    kwargs['shell'] = True
  return subprocess.Popen(*args, **kwargs)
