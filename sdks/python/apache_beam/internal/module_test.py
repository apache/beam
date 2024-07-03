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

"""Module used to define functions and classes used by the coder unit tests."""

# pytype: skip-file

import re
import sys
from typing import Any


class TopClass(object):
  class NestedClass(object):
    def __init__(self, datum):
      self.datum = 'X:%s' % datum

  class MiddleClass(object):
    class NestedClass(object):
      def __init__(self, datum):
        self.datum = 'Y:%s' % datum


def get_lambda_with_globals():
  return lambda s: re.findall(r'\w+', s)


def get_lambda_with_closure(message):
  return lambda: 'closure: %s' % message


class Xyz(object):
  """A class to be pickled."""
  def foo(self, s):
    return re.findall(r'\w+', s)


def create_class(datum):
  """Creates an unnamable class to be pickled."""
  class Z(object):
    def get(self):
      return 'Z:%s' % datum

  return Z()


XYZ_OBJECT = Xyz()


class RecursiveClass(object):
  """A class that contains a reference to itself."""

  SELF_TYPE: Any = None

  def __init__(self, datum):
    self.datum = 'RecursiveClass:%s' % datum


RecursiveClass.SELF_TYPE = RecursiveClass

# pylint: disable=exec-used
if sys.version_info >= (3, 7):
  # create dataclass to be pickled
  exec(
      '''
from dataclasses import dataclass
@dataclass
class DataClass:
  datum: str''')
