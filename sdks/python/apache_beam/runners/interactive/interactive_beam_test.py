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

"""Tests for apache_beam.runners.interactive.interactive_beam."""
# pytype: skip-file

from __future__ import absolute_import

import importlib
import unittest

from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie

# The module name is also a variable in module.
_module_name = 'apache_beam.runners.interactive.interactive_beam_test'


class InteractiveBeamTest(unittest.TestCase):
  def setUp(self):
    self._var_in_class_instance = 'a var in class instance, not directly used'
    ie.new_env()

  def test_watch_main_by_default(self):
    test_env = ie.InteractiveEnvironment()
    # Current Interactive Beam env fetched and the test env are 2 instances.
    self.assertNotEqual(id(ie.current_env()), id(test_env))
    self.assertEqual(ie.current_env().watching(), test_env.watching())

  def test_watch_a_module_by_name(self):
    test_env = ie.InteractiveEnvironment()
    ib.watch(_module_name)
    test_env.watch(_module_name)
    self.assertEqual(ie.current_env().watching(), test_env.watching())

  def test_watch_a_module_by_module_object(self):
    test_env = ie.InteractiveEnvironment()
    module = importlib.import_module(_module_name)
    ib.watch(module)
    test_env.watch(module)
    self.assertEqual(ie.current_env().watching(), test_env.watching())

  def test_watch_locals(self):
    # test_env serves as local var too.
    test_env = ie.InteractiveEnvironment()
    ib.watch(locals())
    test_env.watch(locals())
    self.assertEqual(ie.current_env().watching(), test_env.watching())

  def test_watch_class_instance(self):
    test_env = ie.InteractiveEnvironment()
    ib.watch(self)
    test_env.watch(self)
    self.assertEqual(ie.current_env().watching(), test_env.watching())


if __name__ == '__main__':
  unittest.main()
