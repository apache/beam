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

"""Unit tests for the util module."""

import unittest

from apache_beam.internal.util import ArgumentPlaceholder
from apache_beam.internal.util import insert_values_in_args
from apache_beam.internal.util import remove_objects_from_args


class UtilTest(unittest.TestCase):

  def test_remove_objects_from_args(self):
    args, kwargs, objs = remove_objects_from_args(
        [1, 'a'], {'x': 1, 'y': 3.14}, (str, float))
    self.assertEquals([1, ArgumentPlaceholder()], args)
    self.assertEquals({'x': 1, 'y': ArgumentPlaceholder()}, kwargs)
    self.assertEquals(['a', 3.14], objs)

  def test_remove_objects_from_args_nothing_to_remove(self):
    args, kwargs, objs = remove_objects_from_args(
        [1, 2], {'x': 1, 'y': 2}, (str, float))
    self.assertEquals([1, 2], args)
    self.assertEquals({'x': 1, 'y': 2}, kwargs)
    self.assertEquals([], objs)

  def test_insert_values_in_args(self):
    values = ['a', 'b']
    args = [1, ArgumentPlaceholder()]
    kwargs = {'x': 1, 'y': ArgumentPlaceholder()}
    args, kwargs = insert_values_in_args(args, kwargs, values)
    self.assertEquals([1, 'a'], args)
    self.assertEquals({'x': 1, 'y': 'b'}, kwargs)

  def test_insert_values_in_args_nothing_to_insert(self):
    values = []
    args = [1, 'a']
    kwargs = {'x': 1, 'y': 'b'}
    args, kwargs = insert_values_in_args(args, kwargs, values)
    self.assertEquals([1, 'a'], args)
    self.assertEquals({'x': 1, 'y': 'b'}, kwargs)


if __name__ == '__main__':
  unittest.main()
