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

"""Unit tests for Exception."""

import logging
import sys
import unittest

from apache_beam.utils.exception import raise_exception


class UserDefinedError(Exception):
  pass


class UserDefinedErrorWithArgs(Exception):
  def __init__(self, arg1, arg2):
    self.arg1 = arg1
    self.arg2 = arg2


class TestRaiseException(unittest.TestCase):
  def test_builtin_error(self):
    with self.assertRaises(ValueError) as context:
      message = 'test message'
      try:
        raise ValueError(message)
      except:  # pylint: disable=bare-except
        tp, value, tb = sys.exc_info()
        raise_exception(tp, value, tb)
      self.assertTrue(message in str(context.exception))

  def test_custom_error(self):
    with self.assertRaises(UserDefinedError):
      try:
        raise UserDefinedError()
      except:  # pylint: disable=bare-except
        tp, value, tb = sys.exc_info()
        raise_exception(tp, value, tb)

  def test_custom_error_with_args(self):
    with self.assertRaises(UserDefinedErrorWithArgs) as context:
      arg1 = 'arg1'
      arg2 = 'arg2'
      try:
        raise UserDefinedErrorWithArgs(arg1, arg2)
      except:  # pylint: disable=bare-except
        tp, value, tb = sys.exc_info()
        raise_exception(tp, value, tb)
      self.assertEqual(arg1, context.exception.arg1)
      self.assertEqual(arg2, context.exception.arg2)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
