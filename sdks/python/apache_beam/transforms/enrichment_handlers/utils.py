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
from enum import Enum

__all__ = [
    'ExceptionLevel',
]


class ExceptionLevel(Enum):
  """Options to set the severity of exceptions.

  You can set the exception level option to either
  log a warning, or raise an exception, or do nothing when an empty row
  is fetched from the external service.

  Members:
    - RAISE: Raise the exception.
    - WARN: Log a warning for exception without raising it.
    - QUIET: Neither log nor raise the exception.
  """
  RAISE = 0
  WARN = 1
  QUIET = 2
