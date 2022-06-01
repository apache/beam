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

"""Python Callable utilities.

For internal use only; no backwards-compatibility guarantees.
"""


class PythonCallableWithSource(object):
  """Represents a Python callable object with source codes before evaluated.

  Proxy object to Store a callable object with its string form (source code).
  The string form is used when the object is encoded and transferred to foreign
  SDKs (non-Python SDKs).
  """
  def __init__(self, source):
    # type: (str) -> None
    self._source = source
    self._callable = eval(source)  # pylint: disable=eval-used

  def get_source(self):
    # type: () -> str
    return self._source

  def __call__(self, *args, **kwargs):
    return self._callable(*args, **kwargs)
