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

import threading
import types
import unittest

from apache_beam.coders import proto2_coder_test_messages_pb2
from apache_beam.internal import module_test
from apache_beam.internal.cloudpickle_pickler import dumps
from apache_beam.internal.cloudpickle_pickler import loads
from apache_beam.utils import shared

GLOBAL_DICT_REF = module_test.GLOBAL_DICT


# Allow weakref to dict
class DictWrapper(dict):
  pass


MAIN_MODULE_DICT = DictWrapper()


def acquire_dict():
  return DictWrapper()


class PicklerTest(unittest.TestCase):

  NO_MAPPINGPROXYTYPE = not hasattr(types, "MappingProxyType")

  def test_globals_main_are_pickled_by_value(self):
    self.assertIsNot(MAIN_MODULE_DICT, loads(dumps(lambda: MAIN_MODULE_DICT))())

  def test_globals_shared_are_pickled_by_reference(self):
    shared_handler = shared.Shared()
    original_dict = shared_handler.acquire(acquire_dict)

    unpickled_dict = loads(
        dumps(lambda: shared_handler.acquire(acquire_dict)))()

    self.assertIs(original_dict, unpickled_dict)

  def test_module_globals_are_pickled_by_value_when_directly_referenced(self):
    global_dict = loads(dumps(module_test.GLOBAL_DICT))

    self.assertIsNot(module_test.GLOBAL_DICT, global_dict)

  def test_function_main_with_explicit_module_reference_pickles_by_reference(
      self):
    def returns_global_dict():
      return module_test.GLOBAL_DICT

    self.assertIs(module_test.GLOBAL_DICT, loads(dumps(returns_global_dict))())

  def test_function_main_with_indirect_module_reference_pickles_by_value(self):
    def returns_global_dict():
      return GLOBAL_DICT_REF

    self.assertIsNot(
        module_test.GLOBAL_DICT, loads(dumps(returns_global_dict))())

  def test_function_referencing_unpicklable_object_works_when_imported(self):
    self.assertEqual(
        module_test.UNPICKLABLE_INSTANCE,
        loads(dumps(module_test.fn_returns_unpicklable))())

  def test_closure_with_unpicklable_object_fails_when_imported(self):
    # The entire closure is pickled by value, and therefore module_test is
    # not imported. Requires the module global to be pickled by value.
    with self.assertRaises(Exception):
      loads(dumps(module_test.closure_contains_unpicklable()))

  def test_closure_with_explicit_self_import_can_reference_unpicklable_objects(
      self):
    # The closure imports module_test within the function definition
    # and returns self.UNPICKLABLE_INSTANCE. This allows cloudpickle
    # to use submimort to reference module_test.UNPICKLABLE_INSTANCE
    self.assertIs(
        module_test.UNPICKLABLE_INSTANCE,
        loads(dumps(module_test.closure_contains_unpicklable_imports_self()))())

  def test_closure_main_can_reference_unpicklable_module_objects(self):
    def outer():
      def inner():
        return module_test.UNPICKLABLE_INSTANCE

      return inner

    # Uses subimport to reference module_test.UNPICKLABLE_INSTANCE rather than
    # recreate.
    self.assertIs(module_test.UNPICKLABLE_INSTANCE, loads(dumps(outer()))())

  def test_pickle_nested_enum_descriptor(self):
    NestedEnum = proto2_coder_test_messages_pb2.MessageD.NestedEnum

    def fn():
      return NestedEnum.TWO

    self.assertEqual(fn(), loads(dumps(fn))())

  def test_pickle_top_level_enum_descriptor(self):
    TopLevelEnum = proto2_coder_test_messages_pb2.TopLevelEnum

    def fn():
      return TopLevelEnum.ONE

    self.assertEqual(fn(), loads(dumps(fn))())

  def test_basics(self):
    self.assertEqual([1, 'a', ('z', )], loads(dumps([1, 'a', ('z', )])))
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

  def test_pickle_lock(self):
    lock_instance = threading.Lock()
    lock_type = type(lock_instance)

    self.assertIsInstance(loads(dumps(lock_instance)), lock_type)

  @unittest.skipIf(NO_MAPPINGPROXYTYPE, 'test if MappingProxyType introduced')
  def test_dump_and_load_mapping_proxy(self):
    self.assertEqual(
        'def', loads(dumps(types.MappingProxyType({'abc': 'def'})))['abc'])
    self.assertEqual(
        types.MappingProxyType, type(loads(dumps(types.MappingProxyType({})))))

  # pylint: disable=exec-used
  def test_dataclass(self):
    exec(
        '''
from apache_beam.internal.module_test import DataClass
self.assertEqual(DataClass(datum='abc'), loads(dumps(DataClass(datum='abc'))))
    ''')

  def test_best_effort_determinism_not_implemented(self):
    with self.assertLogs('apache_beam.internal.cloudpickle_pickler',
                         "WARNING") as l:
      dumps(123, enable_best_effort_determinism=True)
      self.assertIn(
          'Ignoring unsupported option: enable_best_effort_determinism',
          '\n'.join(l.output))


if __name__ == '__main__':
  unittest.main()
