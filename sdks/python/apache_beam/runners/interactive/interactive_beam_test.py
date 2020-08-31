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
import sys
import unittest

import apache_beam as beam
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner as ir

# TODO(BEAM-8288): clean up the work-around of nose tests using Python2 without
# unittest.mock module.
try:
  from unittest.mock import patch
except ImportError:
  from mock import patch

# The module name is also a variable in module.
_module_name = 'apache_beam.runners.interactive.interactive_beam_test'


def _get_watched_pcollections_with_variable_names():
  watched_pcollections = {}
  for watching in ie.current_env().watching():
    for key, val in watching:
      if hasattr(val, '__class__') and isinstance(val, beam.pvalue.PCollection):
        watched_pcollections[val] = key
  return watched_pcollections


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

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_show_always_watch_given_pcolls(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    pcoll = p | 'Create' >> beam.Create(range(10))
    # The pcoll is not watched since watch(locals()) is not explicitly called.
    self.assertFalse(pcoll in _get_watched_pcollections_with_variable_names())
    # The call of show watches pcoll.
    ib.show(pcoll)
    self.assertTrue(pcoll in _get_watched_pcollections_with_variable_names())

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_show_mark_pcolls_computed_when_done(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    pcoll = p | 'Create' >> beam.Create(range(10))
    self.assertFalse(pcoll in ie.current_env().computed_pcollections)
    # The call of show marks pcoll computed.
    ib.show(pcoll)
    self.assertTrue(pcoll in ie.current_env().computed_pcollections)

  @unittest.skipIf(
      sys.version_info < (3, 6),
      'The tests require at least Python 3.6 to work.')
  @patch('apache_beam.runners.interactive.interactive_beam.visualize')
  def test_show_handles_dict_of_pcolls(self, mocked_visualize):
    p = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    pcoll = p | 'Create' >> beam.Create(range(10))
    ie.current_env().mark_pcollection_computed([pcoll])
    ie.current_env()._is_in_ipython = True
    ie.current_env()._is_in_notebook = True
    ib.show({'pcoll': pcoll})
    mocked_visualize.assert_called_once()

  @unittest.skipIf(
      sys.version_info < (3, 6),
      'The tests require at least Python 3.6 to work.')
  @patch('apache_beam.runners.interactive.interactive_beam.visualize')
  def test_show_handles_iterable_of_pcolls(self, mocked_visualize):
    p = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    pcoll = p | 'Create' >> beam.Create(range(10))
    ie.current_env().mark_pcollection_computed([pcoll])
    ie.current_env()._is_in_ipython = True
    ie.current_env()._is_in_notebook = True
    ib.show([pcoll])
    mocked_visualize.assert_called_once()

  @unittest.skipIf(
      sys.version_info < (3, 6),
      'The tests require at least Python 3.6 to work.')
  @patch('apache_beam.runners.interactive.interactive_beam.visualize')
  def test_show_noop_when_pcoll_container_is_invalid(self, mocked_visualize):
    class SomeRandomClass:
      def __init__(self, pcoll):
        self._pcoll = pcoll

    p = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    pcoll = p | 'Create' >> beam.Create(range(10))
    ie.current_env().mark_pcollection_computed([pcoll])
    ie.current_env()._is_in_ipython = True
    ie.current_env()._is_in_notebook = True
    self.assertRaises(ValueError, ib.show, SomeRandomClass(pcoll))
    mocked_visualize.assert_not_called()


if __name__ == '__main__':
  unittest.main()
