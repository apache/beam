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

"""Library of supported test cases.

All tests should conform to the NotebookTestCase abstract class.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
from abc import ABC
from abc import abstractmethod


class NotebookTestCase(ABC):
  @abstractmethod
  def __init__(self, setup):
    """Construct a NotebookTestCase.

    Args:
      setup: arbitrary JSON-serializable object specified by test spec
    """
    pass

  # should raise exception on failure
  @abstractmethod
  def check(self, cell):
    """Check correctness against single Jupyter cell.

    Args:
      cell: JSON representation of single cell.

    Returns None if test succeeds, raise exception if test fails.
    """
    pass


################################################################################


class RegexMatch(NotebookTestCase):
  """Checks if given string exists anywhere in the cell."""
  def __init__(self, setup):
    self.regex = re.compile(setup)

  def check(self, cell):
    if not self.regex.search(str(cell)):
      raise Exception(
          "Could not find {} in {}".format(self.regex.pattern, cell))
