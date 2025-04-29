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

"""Unit tests for the pickler module."""

# pytype: skip-file

import random
import sys
import threading
import types
import unittest

from parameterized import param
from parameterized import parameterized

from apache_beam.internal import module_test
from apache_beam.internal import pickler
from apache_beam.internal.pickler import dumps
from apache_beam.internal.pickler import loads


class PicklerTest(unittest.TestCase):

  NO_MAPPINGPROXYTYPE = not hasattr(types, "MappingProxyType")

  @parameterized.expand([
      param(pickle_lib='dill'),
      param(pickle_lib='cloudpickle'),
  ])
  def test_basics(self, pickle_lib):
    pickler.set_library(pickle_lib)

    self.assertEqual([1, 'a', ('z', )], loads(dumps([1, 'a', ('z', )])))
    fun = lambda x: 'xyz-%s' % x
    self.assertEqual('xyz-abc', loads(dumps(fun))('abc'))

  @parameterized.expand([
      param(pickle_lib='dill'),
      param(pickle_lib='cloudpickle'),
  ])
  def test_lambda_with_globals(self, pickle_lib):
    """Tests that the globals of a function are preserved."""
    pickler.set_library(pickle_lib)

    # The point of the test is that the lambda being called after unpickling
    # relies on having the re module being loaded.
    self.assertEqual(['abc', 'def'],
                     loads(dumps(
                         module_test.get_lambda_with_globals()))('abc def'))

  @parameterized.expand([
      param(pickle_lib='dill'),
      param(pickle_lib='cloudpickle'),
  ])
  def test_lambda_with_main_globals(self, pickle_lib):
    pickler.set_library(pickle_lib)
    self.assertEqual(unittest, loads(dumps(lambda: unittest))())

  @parameterized.expand([
      param(pickle_lib='dill'),
      param(pickle_lib='cloudpickle'),
  ])
  def test_lambda_with_closure(self, pickle_lib):
    """Tests that the closure of a function is preserved."""
    pickler.set_library(pickle_lib)
    self.assertEqual(
        'closure: abc',
        loads(dumps(module_test.get_lambda_with_closure('abc')))())

  @parameterized.expand([
      param(pickle_lib='dill'),
      param(pickle_lib='cloudpickle'),
  ])
  def test_class(self, pickle_lib):
    """Tests that a class object is pickled correctly."""
    pickler.set_library(pickle_lib)
    self.assertEqual(['abc', 'def'],
                     loads(dumps(module_test.Xyz))().foo('abc def'))

  @parameterized.expand([
      param(pickle_lib='dill'),
      param(pickle_lib='cloudpickle'),
  ])
  def test_object(self, pickle_lib):
    """Tests that a class instance is pickled correctly."""
    pickler.set_library(pickle_lib)
    self.assertEqual(['abc', 'def'],
                     loads(dumps(module_test.XYZ_OBJECT)).foo('abc def'))

  @parameterized.expand([
      param(pickle_lib='dill'),
      param(pickle_lib='cloudpickle'),
  ])
  def test_nested_class(self, pickle_lib):
    """Tests that a nested class object is pickled correctly."""
    pickler.set_library(pickle_lib)
    self.assertEqual(
        'X:abc', loads(dumps(module_test.TopClass.NestedClass('abc'))).datum)
    self.assertEqual(
        'Y:abc',
        loads(dumps(module_test.TopClass.MiddleClass.NestedClass('abc'))).datum)

  @parameterized.expand([
      param(pickle_lib='dill'),
      param(pickle_lib='cloudpickle'),
  ])
  def test_dynamic_class(self, pickle_lib):
    """Tests that a nested class object is pickled correctly."""
    pickler.set_library(pickle_lib)
    self.assertEqual(
        'Z:abc', loads(dumps(module_test.create_class('abc'))).get())

  @parameterized.expand([
      param(pickle_lib='dill'),
      param(pickle_lib='cloudpickle'),
  ])
  def test_generators(self, pickle_lib):
    pickler.set_library(pickle_lib)
    with self.assertRaises(TypeError):
      dumps((_ for _ in range(10)))

  @parameterized.expand([
      param(pickle_lib='dill'),
      param(pickle_lib='cloudpickle'),
  ])
  def test_recursive_class(self, pickle_lib):
    pickler.set_library(pickle_lib)
    self.assertEqual(
        'RecursiveClass:abc',
        loads(dumps(module_test.RecursiveClass('abc').datum)))

  @parameterized.expand([
      param(pickle_lib='dill'),
      param(pickle_lib='cloudpickle'),
  ])
  def test_pickle_rlock(self, pickle_lib):
    pickler.set_library(pickle_lib)
    rlock_instance = threading.RLock()
    rlock_type = type(rlock_instance)

    self.assertIsInstance(loads(dumps(rlock_instance)), rlock_type)

  @parameterized.expand([
      param(pickle_lib='dill'),
      param(pickle_lib='cloudpickle'),
  ])
  def test_save_paths(self, pickle_lib):
    pickler.set_library(pickle_lib)
    f = loads(dumps(lambda x: x))
    co_filename = f.__code__.co_filename
    self.assertTrue(co_filename.endswith('pickler_test.py'))

  @unittest.skipIf(NO_MAPPINGPROXYTYPE, 'test if MappingProxyType introduced')
  @parameterized.expand([
      param(pickle_lib='dill'),
      param(pickle_lib='cloudpickle'),
  ])
  def test_dump_and_load_mapping_proxy(self, pickle_lib):
    pickler.set_library(pickle_lib)
    self.assertEqual(
        'def', loads(dumps(types.MappingProxyType({'abc': 'def'})))['abc'])
    self.assertEqual(
        types.MappingProxyType, type(loads(dumps(types.MappingProxyType({})))))

  # pylint: disable=exec-used
  @unittest.skipIf(sys.version_info < (3, 7), 'Python 3.7 or above only')
  @parameterized.expand([
      param(pickle_lib='dill'),
      param(pickle_lib='cloudpickle'),
  ])
  def test_dataclass(self, pickle_lib):
    exec(
        '''
from apache_beam.internal.module_test import DataClass
self.assertEqual(DataClass(datum='abc'), loads(dumps(DataClass(datum='abc'))))
    ''')

  def maybe_get_sets_with_different_iteration_orders(self):
    # Use a mix of types in an attempt to create sets with the same elements
    # whose iteration order is different.
    elements = [
        100,
        'hello',
        3.14159,
        True,
        None,
        -50,
        'world',
        False, (1, 2), (4, 3), ('hello', 'world')
    ]
    set1 = set(elements)
    # Try random addition orders until finding an order that works.
    for _ in range(100):
      set2 = set()
      random.shuffle(elements)
      for e in elements:
        set2.add(e)
      if list(set1) != list(set2):
        break
    return set1, set2

  def test_best_effort_determinism(self):
    pickler.set_library('dill')
    set1, set2 = self.maybe_get_sets_with_different_iteration_orders()
    self.assertEqual(
        dumps(set1, enable_best_effort_determinism=True),
        dumps(set2, enable_best_effort_determinism=True))
    # The test relies on the sets having different iteration orders for the
    # elements. Iteration order is implementation dependent and undefined,
    # meaning the test won't always be able to setup these conditions.
    if list(set1) == list(set2):
      self.skipTest('Set iteration orders matched. Test results inconclusive.')

  def test_disable_best_effort_determinism(self):
    pickler.set_library('dill')
    set1, set2 = self.maybe_get_sets_with_different_iteration_orders()
    # The test relies on the sets having different iteration orders for the
    # elements. Iteration order is implementation dependent and undefined,
    # meaning the test won't always be able to setup these conditions.
    if list(set1) == list(set2):
      self.skipTest('Set iteration orders matched. Unable to complete test.')
    self.assertNotEqual(
        dumps(set1, enable_best_effort_determinism=False),
        dumps(set2, enable_best_effort_determinism=False))


if __name__ == '__main__':
  unittest.main()
