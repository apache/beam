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

"""Common utility module shared by runners.

For internal use only; no backwards-compatibility guarantees.
"""
from __future__ import absolute_import


def is_interactive():
  """Determines if current code execution is in interactive environment.

  Returns:
    is_in_ipython: (bool) tells if current code is executed within an ipython
        session.
    is_in_notebook: (bool) tells if current code is executed from an ipython
        notebook.

  If is_in_notebook is True, then is_in_ipython must also be True.
  """
  is_in_ipython = False
  is_in_notebook = False
  # Check if the runtime is within an interactive environment, i.e., ipython.
  try:
    from IPython import get_ipython  # pylint: disable=import-error
    if get_ipython():
      is_in_ipython = True
      if 'IPKernelApp' in get_ipython().config:
        is_in_notebook = True
  except ImportError:
    pass  # If dependencies are not available, then not interactive for sure.
  return is_in_ipython, is_in_notebook
