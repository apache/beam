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

"""Unit tests for the cloudpickle_pickler module."""

# pytype: skip-file

import sys
import threading
import types
import unittest

from apache_beam.internal import module_test
from apache_beam.internal.cloudpickle_pickler import dumps
from apache_beam.internal.cloudpickle_pickler import loads


class PicklerTest(unittest.TestCase):

  NO_MAPPINGPROXYTYPE = not hasattr(types, "MappingProxyType")

  def test_basics(self):
    self.assertEqual([1, 'a', (u'z', )], loads(dumps([1, 'a', (u'z', )])))
    fun = lambda x: 'xyz-%s' % x
    self.assertEqual('xyz-abc', loads(dumps(fun))('abc'))

  def test_lambda_with_globals(self):
    """Tests that the globals of a function are preserved."""

    # The point of the test is that the lambda being called after unpickling
    # relies on having the re module being loaded.
    self.assertEqual(['abc', 'def'],
                     loads(dumps(
                         module_test.get_lambda_with_globals()))('abc def'))

  def test_lambda_with_main_globals(self):
    self.assertEqual(unittest, loads(dumps(lambda: unittest))())

  def test_lambda_with_closure(self):
    """Tests that the closure of a function is preserved."""
    self.assertEqual(
        'closure: abc',
        loads(dumps(module_test.get_lambda_with_closure('abc')))())

  def test_class_object_pickled(self):
    self.assertEqual(['abc', 'def'],
                     loads(dumps(module_test.Xyz))().foo('abc def'))

  def test_class_instance_pickled(self):
    self.assertEqual(['abc', 'def'],
                     loads(dumps(module_test.XYZ_OBJECT)).foo('abc def'))

  def test_pickling_preserves_closure_of_a_function(self):
    self.assertEqual(
        'X:abc', loads(dumps(module_test.TopClass.NestedClass('abc'))).datum)
    self.assertEqual(
        'Y:abc',
        loads(dumps(module_test.TopClass.MiddleClass.NestedClass('abc'))).datum)

  def test_pickle_dynamic_class(self):
    self.assertEqual(
        'Z:abc', loads(dumps(module_test.create_class('abc'))).get())

  def test_generators(self):
    with self.assertRaises(TypeError):
      dumps((_ for _ in range(10)))

  def test_recursive_class(self):
    self.assertEqual(
        'RecursiveClass:abc',
        loads(dumps(module_test.RecursiveClass('abc').datum)))

  def test_function_with_external_reference(self):
    out_of_scope_var = 'expected_value'

    def foo():
      return out_of_scope_var

    self.assertEqual('expected_value', loads(dumps(foo))())

  def test_pickle_rlock(self):
    rlock_instance = threading.RLock()
    rlock_type = type(rlock_instance)

    self.assertIsInstance(loads(dumps(rlock_instance)), rlock_type)

  @unittest.skipIf(NO_MAPPINGPROXYTYPE, 'test if MappingProxyType introduced')
  def test_dump_and_load_mapping_proxy(self):
    self.assertEqual(
        'def', loads(dumps(types.MappingProxyType({'abc': 'def'})))['abc'])
    self.assertEqual(
        types.MappingProxyType, type(loads(dumps(types.MappingProxyType({})))))

  # pylint: disable=exec-used
  @unittest.skipIf(sys.version_info < (3, 7), 'Python 3.7 or above only')
  def test_dataclass(self):
    exec(
        '''
from apache_beam.internal.module_test import DataClass
self.assertEqual(DataClass(datum='abc'), loads(dumps(DataClass(datum='abc'))))
    ''')


if __name__ == '__main__':
  unittest.main()
