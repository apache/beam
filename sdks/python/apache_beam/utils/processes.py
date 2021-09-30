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

# pytype: skip-file

import platform
import subprocess
import traceback
from typing import TYPE_CHECKING

# On Windows, we need to use shell=True when creating subprocesses for binary
# paths to be resolved correctly.
force_shell = platform.system() == 'Windows'

# We mimic the interface of the standard Python subprocess module.
PIPE = subprocess.PIPE
STDOUT = subprocess.STDOUT
CalledProcessError = subprocess.CalledProcessError

if TYPE_CHECKING:
  call = subprocess.call
  check_call = subprocess.check_call
  check_output = subprocess.check_output
  Popen = subprocess.Popen

else:

  def call(*args, **kwargs):
    if force_shell:
      kwargs['shell'] = True
    try:
      out = subprocess.call(*args, **kwargs)
    except OSError:
      raise RuntimeError("Executable {} not found".format(args[0]))
    except subprocess.CalledProcessError as error:
      if isinstance(args, tuple) and (args[0][2] == "pip"):
        raise RuntimeError( \
          "Full traceback: {}\n Pip install failed for package: {} \
          \n Output from execution of subprocess: {}" \
          .format(traceback.format_exc(), args[0][6], error. output))
      else:
        raise RuntimeError("Full trace: {}\
           \n Output of the failed child process: {} " \
          .format(traceback.format_exc(), error.output))
    return out

  def check_call(*args, **kwargs):
    if force_shell:
      kwargs['shell'] = True
    try:
      out = subprocess.check_call(*args, **kwargs)
    except OSError:
      raise RuntimeError("Executable {} not found".format(args[0]))
    except subprocess.CalledProcessError as error:
      if isinstance(args, tuple) and (args[0][2] == "pip"):
        raise RuntimeError( \
          "Full traceback: {} \n Pip install failed for package: {} \
          \n Output from execution of subprocess: {}" \
          .format(traceback.format_exc(), args[0][6], error.output))
      else:
        raise RuntimeError("Full trace: {} \
          \n Output of the failed child process: {}" \
          .format(traceback.format_exc(), error.output))
    return out

  def check_output(*args, **kwargs):
    if force_shell:
      kwargs['shell'] = True
    try:
      out = subprocess.check_output(*args, **kwargs)
    except OSError:
      raise RuntimeError("Executable {} not found".format(args[0]))
    except subprocess.CalledProcessError as error:
      if isinstance(args, tuple) and (args[0][2] == "pip"):
        raise RuntimeError( \
          "Full traceback: {} \n Pip install failed for package: {} \
          \n Output from execution of subprocess: {}" \
          .format(traceback.format_exc(), args[0][6], error.output))
      else:
        raise RuntimeError("Full trace: {}, \
           output of the failed child process {} "\
          .format(traceback.format_exc(), error.output))
    return out

  def Popen(*args, **kwargs):
    if force_shell:
      kwargs['shell'] = True
    return subprocess.Popen(*args, **kwargs)
