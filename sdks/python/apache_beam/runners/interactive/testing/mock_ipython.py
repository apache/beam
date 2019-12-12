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

# Mocked object returned by invoking get_ipython() in an ipython environment.
_mocked_get_ipython = None


def mock_get_ipython():
  """Mock an ipython environment w/o setting up real ipython kernel.

  Each entering of get_ipython() invocation will have the prompt increased by
  one. Grouping arbitrary python code into separate cells using `with` clause.

  Examples::

    # Usage, before each test function, append:
    @patch('IPython.get_ipython', mock_get_ipython)

    # Group lines of code into a cell:
    with mock_get_ipython():
      # arbitrary python code
      # ...
      # arbitrary python code

    # Next cell with prompt increased by one:
    with mock_get_ipython():  # Auto-incremental
      # arbitrary python code
      # ...
      # arbitrary python code
  """

  class MockedGetIpython(object):

    def __init__(self):
      self._execution_count = 0

    @property
    def execution_count(self):
      """Execution count always starts from 1 and is constant within a cell."""
      return self._execution_count

    def __enter__(self):
      """Marks entering of a cell/prompt."""
      self._execution_count = self._execution_count + 1

    def __exit__(self, exc_type, exc_value, traceback):
      """Marks exiting of a cell/prompt."""
      pass

  global _mocked_get_ipython
  if not _mocked_get_ipython:
    _mocked_get_ipython = MockedGetIpython()
  return _mocked_get_ipython
