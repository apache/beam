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

"""Tests for generating stable identifiers to use for Pickle serialization."""

import hashlib
import unittest

from parameterized import parameterized

# pylint: disable=unused-import
from apache_beam.internal import code_object_pickler
from apache_beam.internal.test_data import module_1
from apache_beam.internal.test_data import module_1_class_added
from apache_beam.internal.test_data import module_1_function_added
from apache_beam.internal.test_data import module_1_global_variable_added
from apache_beam.internal.test_data import module_1_lambda_variable_added
from apache_beam.internal.test_data import module_1_local_variable_added
from apache_beam.internal.test_data import module_1_local_variable_removed
from apache_beam.internal.test_data import module_1_nested_function_2_added
from apache_beam.internal.test_data import module_1_nested_function_added
from apache_beam.internal.test_data import module_2
from apache_beam.internal.test_data import module_2_modified
from apache_beam.internal.test_data import module_3
from apache_beam.internal.test_data import module_3_modified
from apache_beam.internal.test_data import module_with_default_argument


def top_level_function():
  return 1


top_level_lambda = lambda x: 1


def get_nested_function():
  def nested_function():
    return 1

  return nested_function


def get_lambda_from_dictionary():
  d = {"a": lambda x: 1, "b": lambda y: 2}
  return d["a"]


def get_lambda_from_dictionary_same_args():
  d = {"a": lambda x: 1, "b": lambda x: x + 1}
  return d["a"]


def function_with_lambda_default_argument(fn=lambda x: 1):
  return fn


def function_with_function_default_argument(fn=top_level_function):
  return fn


def function_decorator(f):
  return lambda x: f(f(x))


@function_decorator
def add_one(x):
  return x + 1


class ClassWithFunction:
  def process(self):
    return 1


class ClassWithStaticMethod:
  @staticmethod
  def static_method():
    return 1


class ClassWithClassMethod:
  @classmethod
  def class_method(cls):
    return 1


class ClassWithNestedFunction:
  def process(self):
    def nested_function():
      return 1

    return nested_function


class ClassWithLambda:
  def process(self):
    return lambda: 1


class ClassWithNestedClass:
  class InnerClass:
    def process(self):
      return 1


class ClassWithNestedLambda:
  def process(self):
    def get_lambda_from_dictionary():
      d = {"a": lambda x: 1, "b": lambda y: 2}
      return d["a"]

    return get_lambda_from_dictionary()


prefix = __name__

test_cases = [
    (top_level_function, f"{prefix}.top_level_function"
     ".__code__"),
    (top_level_lambda, f"{prefix}.top_level_lambda"
     ".__code__"),
    (
        get_nested_function(), (
            f"{prefix}.get_nested_function"
            ".__code__.co_consts[nested_function]")),
    (
        get_lambda_from_dictionary(),
        (
            f"{prefix}"
            ".get_lambda_from_dictionary.__code__.co_consts[<lambda>, ('x',)]")
    ),
    (
        get_lambda_from_dictionary_same_args(),
        (
            f"{prefix}"
            ".get_lambda_from_dictionary_same_args.__code__.co_consts"
            "[<lambda>, ('x',), " + hashlib.md5(
                get_lambda_from_dictionary_same_args().__code__.co_code).
            hexdigest() + "]")),
    (
        function_with_lambda_default_argument(),
        (
            f"{prefix}"
            ".function_with_lambda_default_argument.__defaults__[0].__code__")),
    (
        function_with_function_default_argument(),
        f"{prefix}.top_level_function"
        ".__code__"),
    (add_one, f"{prefix}.function_decorator"
     ".__code__.co_consts[<lambda>]"),
    (
        ClassWithFunction.process,
        f"{prefix}.ClassWithFunction"
        ".process.__code__"),
    (
        ClassWithStaticMethod.static_method,
        f"{prefix}.ClassWithStaticMethod"
        ".static_method.__code__"),
    (
        ClassWithClassMethod.class_method,
        f"{prefix}.ClassWithClassMethod"
        ".class_method.__code__"),
    (
        ClassWithNestedFunction().process(),
        (
            f"{prefix}.ClassWithNestedFunction.process.__code__.co_consts"
            "[nested_function]")),
    (
        ClassWithLambda().process(),
        f"{prefix}.ClassWithLambda.process.__code__.co_consts[<lambda>]"),
    (
        ClassWithNestedClass.InnerClass().process,
        f"{prefix}.ClassWithNestedClass.InnerClass.process.__code__"),
    (
        ClassWithNestedLambda().process(),
        (
            f"{prefix}"
            ".ClassWithNestedLambda.process.__code__.co_consts"
            "[get_lambda_from_dictionary].co_consts[<lambda>, ('x',)]")),
    (
        ClassWithNestedLambda.process,
        f"{prefix}.ClassWithNestedLambda.process.__code__"),
]


class CodeObjectIdentifierGenerationTest(unittest.TestCase):
  @parameterized.expand(test_cases)
  def test_get_code_object_identifier(self, callable, expected_path):
    actual = code_object_pickler.get_code_object_identifier(callable)
    self.assertEqual(actual, expected_path)

  @parameterized.expand(test_cases)
  def test_get_code_from_identifier(self, expected_callable, path):
    actual = code_object_pickler.get_code_from_identifier(path)
    self.assertEqual(actual, expected_callable.__code__)

  @parameterized.expand(test_cases)
  def test_roundtrip(self, callable, unused_path):
    path = code_object_pickler.get_code_object_identifier(callable)
    actual = code_object_pickler.get_code_from_identifier(path)
    self.assertEqual(actual, callable.__code__)


class GetCodeFromCodeObjectIdentifierTest(unittest.TestCase):
  def test_empty_path_raises_exception(self):
    with self.assertRaisesRegex(ValueError, "Path must not be empty"):
      code_object_pickler.get_code_from_identifier("")

  def test_invalid_default_index_raises_exception(self):
    with self.assertRaisesRegex(ValueError, "out of bounds"):
      code_object_pickler.get_code_from_identifier(
          "apache_beam.internal.test_data.module_with_default_argument."
          "function_with_lambda_default_argument.__defaults__[1]")

  def test_invalid_single_name_path_raises_exception(self):
    with self.assertRaisesRegex(AttributeError,
                                "Could not find code object with path"):
      code_object_pickler.get_code_from_identifier(
          "apache_beam.internal.test_data.module_3."
          "my_function.__code__.co_consts[something]")

  def test_invalid_lambda_with_args_path_raises_exception(self):
    with self.assertRaisesRegex(AttributeError,
                                "Could not find code object with path"):
      code_object_pickler.get_code_from_identifier(
          "apache_beam.internal.test_data.module_3."
          "my_function.__code__.co_consts[<lambda>, ('x',)]")

  def test_invalid_lambda_with_hash_path_raises_exception(self):
    with self.assertRaisesRegex(AttributeError,
                                "Could not find code object with path"):
      code_object_pickler.get_code_from_identifier(
          "apache_beam.internal.test_data.module_3."
          "my_function.__code__.co_consts[<lambda>, ('',), 1234567890]")

  def test_adding_local_variable_in_class_preserves_object(self):
    self.assertEqual(
        code_object_pickler.get_code_from_identifier(
            code_object_pickler.get_code_object_identifier(
                module_2.AddLocalVariable.my_method(self)).replace(
                    "module_2", "module_2_modified")),
        module_2_modified.AddLocalVariable.my_method(self).__code__,
    )

  def test_removing_local_variable_in_class_preserves_object(self):
    self.assertEqual(
        code_object_pickler.get_code_from_identifier(
            code_object_pickler.get_code_object_identifier(
                module_2.RemoveLocalVariable.my_method(self)).replace(
                    "module_2", "module_2_modified")),
        module_2_modified.RemoveLocalVariable.my_method(self).__code__,
    )

  def test_adding_lambda_variable_in_class_preserves_object(self):
    self.assertEqual(
        code_object_pickler.get_code_from_identifier(
            code_object_pickler.get_code_object_identifier(
                module_2.AddLambdaVariable.my_method(self)).replace(
                    "module_2", "module_2_modified")),
        module_2_modified.AddLambdaVariable.my_method(self).__code__,
    )

  def test_removing_lambda_variable_in_class_changes_object(self):
    with self.assertRaisesRegex(AttributeError, "object has no attribute"):
      code_object_pickler.get_code_from_identifier(
          code_object_pickler.get_code_object_identifier(
              module_2.RemoveLambdaVariable.my_method(self)).replace(
                  "module_2", "module_2_modified"))

  def test_adding_nested_function_in_class_preserves_object(self):
    self.assertEqual(
        code_object_pickler.get_code_from_identifier(
            code_object_pickler.get_code_object_identifier(
                module_2.ClassWithNestedFunction.my_method(self)).replace(
                    "module_2", "module_2_modified")),
        module_2_modified.ClassWithNestedFunction.my_method(self).__code__,
    )

  def test_adding_nested_function_2_in_class_preserves_object(self):
    self.assertEqual(
        code_object_pickler.get_code_from_identifier(
            code_object_pickler.get_code_object_identifier(
                module_2.ClassWithNestedFunction2.my_method(self)).replace(
                    "module_2", "module_2_modified")),
        module_2_modified.ClassWithNestedFunction2.my_method(self).__code__,
    )

  def test_adding_new_function_in_class_preserves_object(self):
    self.assertEqual(
        code_object_pickler.get_code_from_identifier(
            code_object_pickler.get_code_object_identifier(
                module_2.ClassWithTwoMethods.my_method(self)).replace(
                    "module_2", "module_2_modified")),
        module_2_modified.ClassWithTwoMethods.my_method(self).__code__,
    )

  def test_removing_method_in_class_preserves_object(self):
    self.assertEqual(
        code_object_pickler.get_code_from_identifier(
            code_object_pickler.get_code_object_identifier(
                module_2.RemoveMethod.my_method(self)).replace(
                    "module_2", "module_2_modified")),
        module_2_modified.RemoveMethod.my_method(self).__code__,
    )

  def test_adding_global_variable_preserves_object(self):
    self.assertEqual(
        code_object_pickler.get_code_from_identifier(
            code_object_pickler.get_code_object_identifier(
                module_1.my_function()).replace(
                    "module_1",
                    "module_1_global_variable_added",
                )),
        module_1_global_variable_added.my_function().__code__,
    )

  def test_adding_top_level_function_preserves_object(self):
    self.assertEqual(
        code_object_pickler.get_code_from_identifier(
            code_object_pickler.get_code_object_identifier(
                module_1.my_function()).replace(
                    "module_1", "module_1_function_added")),
        module_1_function_added.my_function().__code__,
    )

  def test_adding_local_variable_in_function_preserves_object(self):
    self.assertEqual(
        code_object_pickler.get_code_from_identifier(
            code_object_pickler.get_code_object_identifier(
                module_1.my_function()).replace(
                    "module_1", "module_1_local_variable_added")),
        module_1_local_variable_added.my_function().__code__,
    )

  def test_removing_local_variable_in_function_preserves_object(self):
    self.assertEqual(
        code_object_pickler.get_code_from_identifier(
            code_object_pickler.get_code_object_identifier(
                module_1.my_function()).replace(
                    "module_1", "module_1_local_variable_removed")),
        module_1_local_variable_removed.my_function().__code__,
    )

  def test_adding_nested_function_in_function_preserves_object(self):
    self.assertEqual(
        code_object_pickler.get_code_from_identifier(
            code_object_pickler.get_code_object_identifier(
                module_1.my_function()).replace(
                    "module_1", "module_1_nested_function_added")),
        module_1_nested_function_added.my_function().__code__,
    )

  def test_adding_nested_function_2_in_function_preserves_object(self):
    self.assertEqual(
        code_object_pickler.get_code_from_identifier(
            code_object_pickler.get_code_object_identifier(
                module_1.my_function()).replace(
                    "module_1", "module_1_nested_function_2_added")),
        module_1_nested_function_2_added.my_function().__code__,
    )

  def test_adding_class_preserves_object(self):
    self.assertEqual(
        code_object_pickler.get_code_from_identifier(
            code_object_pickler.get_code_object_identifier(
                module_1.my_function()).replace(
                    "module_1", "module_1_class_added")),
        module_1_class_added.my_function().__code__,
    )

  def test_adding_lambda_variable_in_function_preserves_object(self):
    self.assertEqual(
        code_object_pickler.get_code_from_identifier(
            code_object_pickler.get_code_object_identifier(
                module_1.my_function()).replace(
                    "module_1", "module_1_lambda_variable_added")),
        module_1_lambda_variable_added.my_function().__code__,
    )

  def test_removing_lambda_variable_in_function_raises_exception(self):
    with self.assertRaisesRegex(AttributeError, "object has no attribute"):
      code_object_pickler.get_code_from_identifier(
          code_object_pickler.get_code_object_identifier(
              module_3.my_function()).replace("module_3", "module_3_modified"))


class CodePathStabilityTest(unittest.TestCase):
  def test_adding_local_variable_in_class_preserves_path(self):
    self.assertEqual(
        code_object_pickler.get_code_object_identifier(
            module_2.AddLocalVariable.my_method(self)).replace(
                "module_2", "module_name"),
        code_object_pickler.get_code_object_identifier(
            module_2_modified.AddLocalVariable.my_method(self)).replace(
                "module_2_modified", "module_name"),
    )

  def test_removing_local_variable_in_class_preserves_path(self):
    self.assertEqual(
        code_object_pickler.get_code_object_identifier(
            module_2.RemoveLocalVariable.my_method(self)).replace(
                "module_2", "module_name"),
        code_object_pickler.get_code_object_identifier(
            module_2_modified.RemoveLocalVariable.my_method(self)).replace(
                "module_2_modified", "module_name"),
    )

  def test_adding_lambda_variable_in_class_changes_path(self):
    self.assertNotEqual(
        code_object_pickler.get_code_object_identifier(
            module_2.AddLambdaVariable.my_method(self)).replace(
                "module_2", "module_name"),
        code_object_pickler.get_code_object_identifier(
            module_2_modified.AddLambdaVariable.my_method(self)).replace(
                "module_2_modified", "module_name"),
    )

  def test_removing_lambda_variable_in_class_changes_path(self):
    self.assertNotEqual(
        code_object_pickler.get_code_object_identifier(
            module_2.RemoveLambdaVariable.my_method(self)).replace(
                "module_2", "module_name"),
        code_object_pickler.get_code_object_identifier(
            module_2_modified.RemoveLambdaVariable.my_method(self)).replace(
                "module_2_modified", "module_name"),
    )

  def test_adding_nested_function_in_class_preserves_path(self):
    self.assertEqual(
        code_object_pickler.get_code_object_identifier(
            module_2.ClassWithNestedFunction.my_method(self)).replace(
                "module_2", "module_name"),
        code_object_pickler.get_code_object_identifier(
            module_2_modified.ClassWithNestedFunction.my_method(self)).replace(
                "module_2_modified", "module_name"),
    )

  def test_adding_nested_function_2_in_class_preserves_path(self):
    self.assertEqual(
        code_object_pickler.get_code_object_identifier(
            module_2.ClassWithNestedFunction2.my_method(self)).replace(
                "module_2", "module_name"),
        code_object_pickler.get_code_object_identifier(
            module_2_modified.ClassWithNestedFunction2.my_method(self)).replace(
                "module_2_modified", "module_name"),
    )

  def test_adding_new_function_in_class_preserves_path(self):
    self.assertEqual(
        code_object_pickler.get_code_object_identifier(
            module_2.ClassWithTwoMethods.my_method(self)).replace(
                "module_2", "module_name"),
        code_object_pickler.get_code_object_identifier(
            module_2_modified.ClassWithTwoMethods.my_method(self)).replace(
                "module_2_modified", "module_name"),
    )

  def test_removing_function_in_class_preserves_path(self):
    self.assertEqual(
        code_object_pickler.get_code_object_identifier(
            module_2.RemoveMethod.my_method(self)).replace(
                "module_2", "module_name"),
        code_object_pickler.get_code_object_identifier(
            module_2_modified.RemoveMethod.my_method(self)).replace(
                "module_2_modified", "module_name"),
    )

  def test_adding_global_variable_preserves_path(self):
    self.assertEqual(
        code_object_pickler.get_code_object_identifier(
            module_1.my_function()).replace("module_1", "module_name"),
        code_object_pickler.get_code_object_identifier(
            module_1_global_variable_added.my_function()).replace(
                "module_1_global_variable_added", "module_name"),
    )

  def test_adding_top_level_function_preserves_path(self):
    self.assertEqual(
        code_object_pickler.get_code_object_identifier(
            module_1.my_function()).replace("module_1", "module_name"),
        code_object_pickler.get_code_object_identifier(
            module_1_function_added.my_function()).replace(
                "module_1_function_added", "module_name"),
    )

  def test_adding_local_variable_in_function_preserves_path(self):
    self.assertEqual(
        code_object_pickler.get_code_object_identifier(
            module_1.my_function()).replace("module_1", "module_name"),
        code_object_pickler.get_code_object_identifier(
            module_1_local_variable_added.my_function()).replace(
                "module_1_local_variable_added", "module_name"),
    )

  def test_removing_local_variable_in_function_preserves_path(self):
    self.assertEqual(
        code_object_pickler.get_code_object_identifier(
            module_1.my_function()).replace("module_1", "module_name"),
        code_object_pickler.get_code_object_identifier(
            module_1_local_variable_removed.my_function()).replace(
                "module_1_local_variable_removed", "module_name"),
    )

  def test_adding_nested_function_in_function_preserves_path(self):
    self.assertEqual(
        code_object_pickler.get_code_object_identifier(
            module_1.my_function()).replace("module_1", "module_name"),
        code_object_pickler.get_code_object_identifier(
            module_1_nested_function_added.my_function()).replace(
                "module_1_nested_function_added", "module_name"),
    )

  def test_adding_nested_function_2_in_function_preserves_path(self):
    self.assertEqual(
        code_object_pickler.get_code_object_identifier(
            module_1.my_function()).replace("module_1", "module_name"),
        code_object_pickler.get_code_object_identifier(
            module_1_nested_function_2_added.my_function()).replace(
                "module_1_nested_function_2_added", "module_name"),
    )

  def test_adding_class_preserves_path(self):
    self.assertEqual(
        code_object_pickler.get_code_object_identifier(
            module_1.my_function()).replace("module_1", "module_name"),
        code_object_pickler.get_code_object_identifier(
            module_1_class_added.my_function()).replace(
                "module_1_class_added", "module_name"),
    )

  def test_adding_lambda_variable_in_function_changes_path(self):
    self.assertNotEqual(
        code_object_pickler.get_code_object_identifier(
            module_1.my_function()).replace("module_1", "module_name"),
        code_object_pickler.get_code_object_identifier(
            module_1_lambda_variable_added.my_function()).replace(
                "module_1_lambda_variable_added", "module_name"),
    )

  def test_removing_lambda_variable_in_function_changes_path(self):
    self.assertNotEqual(
        code_object_pickler.get_code_object_identifier(
            module_3.my_function()).replace("module_3", "module_name"),
        code_object_pickler.get_code_object_identifier(
            module_3_modified.my_function()).replace(
                "module_3_modified", "module_name"),
    )


if __name__ == "__main__":
  unittest.main()
