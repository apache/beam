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

import unittest
import apache_beam.internal.test_cases

from apache_beam.internal import code_object_pickler
from apache_beam.internal.test_cases import after_module_add_function
from apache_beam.internal.test_cases import after_module_add_lambda_variable
from apache_beam.internal.test_cases import after_module_add_variable
from apache_beam.internal.test_cases import after_module_remove_lambda_variable
from apache_beam.internal.test_cases import after_module_remove_variable
from apache_beam.internal.test_cases import after_module_with_classes
from apache_beam.internal.test_cases import after_module_with_global_variable
from apache_beam.internal.test_cases import after_module_with_nested_function
from apache_beam.internal.test_cases import after_module_with_nested_function_2
from apache_beam.internal.test_cases import after_module_with_single_class
from apache_beam.internal.test_cases import before_module_with_classes
from apache_beam.internal.test_cases import before_module_with_functions
from apache_beam.internal.test_cases import before_module_with_lambdas


class CodePathTest(unittest.TestCase):
  def test_get_code_from_stable_reference_empty_path(self):
    with self.assertRaisesRegex(ValueError, "Path must not be empty"):
      code_object_pickler._get_code_from_stable_reference("")

  def test_get_code_from_stable_reference_invalid_default_index(self):
    with self.assertRaisesRegex(ValueError, "out of bounds"):
      code_object_pickler._get_code_from_stable_reference(
          "apache_beam.internal.test_cases.module_with_default_argument."
          "function_with_lambda_default_argument.__defaults__[1]")

  def test_get_code_from_stable_reference_invalid_single_name_path(self):
    with self.assertRaisesRegex(AttributeError,
                                "Could not find code object with path"):
      code_object_pickler._get_code_from_stable_reference(
          "apache_beam.internal.test_cases.before_module_with_lambdas."
          "my_function.__code__.co_consts[something]")

  def test_get_code_from_stable_reference_invalid_lambda_with_args_path(self):
    with self.assertRaisesRegex(AttributeError,
                                "Could not find code object with path"):
      code_object_pickler._get_code_from_stable_reference(
          "apache_beam.internal.test_cases.before_module_with_lambdas."
          "my_function.__code__.co_consts[<lambda>, ('x',)]")

  def test_get_code_from_stable_reference_invalid_lambda_with_hash_path(self):
    with self.assertRaisesRegex(AttributeError,
                                "Could not find code object with path"):
      code_object_pickler._get_code_from_stable_reference(
          "apache_beam.internal.test_cases.before_module_with_lambdas."
          "my_function.__code__.co_consts[<lambda>, ('',), 1234567890]")

  def test_get_code_add_local_variable_in_class(self):
    self.assertEqual(
        code_object_pickler._get_code_from_stable_reference(
            code_object_pickler._get_code_path(
                before_module_with_classes.AddLocalVariable.my_method()).
            replace("before_module_with_classes", "after_module_with_classes")),
        after_module_with_classes.AddLocalVariable.my_method().__code__,
    )

  def test_get_code_remove_local_variable_in_class(self):
    self.assertEqual(
        code_object_pickler._get_code_from_stable_reference(
            code_object_pickler._get_code_path(
                before_module_with_classes.RemoveLocalVariable.my_method()).
            replace("before_module_with_classes", "after_module_with_classes")),
        after_module_with_classes.RemoveLocalVariable.my_method().__code__,
    )

  def test_get_code_add_lambda_variable_in_class(self):
    self.assertEqual(
        code_object_pickler._get_code_from_stable_reference(
            code_object_pickler._get_code_path(
                before_module_with_classes.AddLambdaVariable.my_method()).
            replace("before_module_with_classes", "after_module_with_classes")),
        after_module_with_classes.AddLambdaVariable.my_method().__code__,
    )

  def test_get_code_remove_lambda_variable_in_class(self):
    with self.assertRaisesRegex(AttributeError, "object has no attribute"):
      code_object_pickler._get_code_from_stable_reference(
          code_object_pickler._get_code_path(
              before_module_with_classes.RemoveLambdaVariable.my_method()).
          replace("before_module_with_classes", "after_module_with_classes"))

  def test_get_code_nested_function_in_class(self):
    self.assertEqual(
        code_object_pickler._get_code_from_stable_reference(
            code_object_pickler._get_code_path(
                before_module_with_classes.ClassWithNestedFunction.my_method()).
            replace("before_module_with_classes", "after_module_with_classes")),
        after_module_with_classes.ClassWithNestedFunction.my_method().__code__,
    )

  def test_get_code_nested_function_2_in_class(self):
    self.assertEqual(
        code_object_pickler._get_code_from_stable_reference(
            code_object_pickler._get_code_path(
                before_module_with_classes.ClassWithNestedFunction2.my_method()
            ).replace(
                "before_module_with_classes", "after_module_with_classes")),
        after_module_with_classes.ClassWithNestedFunction2.my_method().__code__,
    )

  def test_get_code_add_new_function_in_class(self):
    self.assertEqual(
        code_object_pickler._get_code_from_stable_reference(
            code_object_pickler._get_code_path(
                before_module_with_classes.ClassWithTwoMethods.my_method()).
            replace("before_module_with_classes", "after_module_with_classes")),
        after_module_with_classes.ClassWithTwoMethods.my_method().__code__,
    )

  def test_get_code_remove_method_in_class(self):
    self.assertEqual(
        code_object_pickler._get_code_from_stable_reference(
            code_object_pickler._get_code_path(
                before_module_with_classes.RemoveMethod.my_method()).replace(
                    "before_module_with_classes", "after_module_with_classes")),
        after_module_with_classes.RemoveMethod.my_method().__code__,
    )

  def test_get_code_add_global_variable(self):
    self.assertEqual(
        code_object_pickler._get_code_from_stable_reference(
            code_object_pickler._get_code_path(
                before_module_with_functions.my_function()).replace(
                    "before_module_with_functions",
                    "after_module_with_global_variable",
                )),
        after_module_with_global_variable.my_function().__code__,
    )

  def test_get_code_add_top_level_function(self):
    self.assertEqual(
        code_object_pickler._get_code_from_stable_reference(
            code_object_pickler._get_code_path(
                before_module_with_functions.my_function()).replace(
                    "before_module_with_functions",
                    "after_module_add_function")),
        after_module_add_function.my_function().__code__,
    )

  def test_get_code_add_local_variable_in_function(self):
    self.assertEqual(
        code_object_pickler._get_code_from_stable_reference(
            code_object_pickler._get_code_path(
                before_module_with_functions.my_function()).replace(
                    "before_module_with_functions",
                    "after_module_add_variable")),
        after_module_add_variable.my_function().__code__,
    )

  def test_get_code_remove_local_variable_in_function(self):
    self.assertEqual(
        code_object_pickler._get_code_from_stable_reference(
            code_object_pickler._get_code_path(
                before_module_with_functions.my_function()).replace(
                    "before_module_with_functions",
                    "after_module_remove_variable")),
        after_module_remove_variable.my_function().__code__,
    )

  def test_get_code_nested_function_in_function(self):
    self.assertEqual(
        code_object_pickler._get_code_from_stable_reference(
            code_object_pickler._get_code_path(
                before_module_with_functions.my_function()).replace(
                    "before_module_with_functions",
                    "after_module_with_nested_function")),
        after_module_with_nested_function.my_function().__code__,
    )

  def test_get_code_nested_function_2_in_function(self):
    self.assertEqual(
        code_object_pickler._get_code_from_stable_reference(
            code_object_pickler._get_code_path(
                before_module_with_functions.my_function()).replace(
                    "before_module_with_functions",
                    "after_module_with_nested_function_2")),
        after_module_with_nested_function_2.my_function().__code__,
    )

  def test_get_code_add_class(self):
    self.assertEqual(
        code_object_pickler._get_code_from_stable_reference(
            code_object_pickler._get_code_path(
                before_module_with_functions.my_function()).replace(
                    "before_module_with_functions",
                    "after_module_with_single_class")),
        after_module_with_single_class.my_function().__code__,
    )

  def test_get_code_add_lambda_variable_in_function(self):
    self.assertEqual(
        code_object_pickler._get_code_from_stable_reference(
            code_object_pickler._get_code_path(
                before_module_with_functions.my_function()).replace(
                    "before_module_with_functions",
                    "after_module_add_lambda_variable")),
        after_module_add_lambda_variable.my_function().__code__,
    )

  def test_get_code_remove_lambda_variable_in_function(self):
    with self.assertRaisesRegex(AttributeError, "object has no attribute"):
      code_object_pickler._get_code_from_stable_reference(
          code_object_pickler._get_code_path(
              before_module_with_lambdas.my_function()).replace(
                  "before_module_with_lambdas",
                  "after_module_remove_lambda_variable"))

  def test_identifiers_new_local_variable_in_class(self):
    self.assertEqual(
        code_object_pickler._get_code_path(
            before_module_with_classes.AddLocalVariable.my_method()).replace(
                "before_module_with_classes", "module_name"),
        code_object_pickler._get_code_path(
            after_module_with_classes.AddLocalVariable.my_method()).replace(
                "after_module_with_classes", "module_name"),
    )

  def test_identifiers_remove_local_variable_in_class(self):
    self.assertEqual(
        code_object_pickler._get_code_path(
            before_module_with_classes.RemoveLocalVariable.my_method()).replace(
                "before_module_with_classes", "module_name"),
        code_object_pickler._get_code_path(
            after_module_with_classes.RemoveLocalVariable.my_method()).replace(
                "after_module_with_classes", "module_name"),
    )

  def test_identifiers_add_lambda_variable_in_class(self):
    self.assertNotEqual(
        code_object_pickler._get_code_path(
            before_module_with_classes.AddLambdaVariable.my_method()).replace(
                "before_module_with_classes", "module_name"),
        code_object_pickler._get_code_path(
            after_module_with_classes.AddLambdaVariable.my_method()).replace(
                "after_module_with_classes", "module_name"),
    )

  def test_identifiers_remove_lambda_variable_in_class(self):
    self.assertNotEqual(
        code_object_pickler._get_code_path(
            before_module_with_classes.RemoveLambdaVariable.my_method()).
        replace("before_module_with_classes", "module_name"),
        code_object_pickler._get_code_path(
            after_module_with_classes.RemoveLambdaVariable.my_method()).replace(
                "after_module_with_classes", "module_name"),
    )

  def test_identifiers_nested_function_in_class(self):
    self.assertEqual(
        code_object_pickler._get_code_path(
            before_module_with_classes.ClassWithNestedFunction.my_method()
        ).replace("before_module_with_classes", "module_name"),
        code_object_pickler._get_code_path(
            after_module_with_classes.ClassWithNestedFunction.my_method()).
        replace("after_module_with_classes", "module_name"),
    )

  def test_identifiers_nested_function_2_in_class(self):
    self.assertEqual(
        code_object_pickler._get_code_path(
            before_module_with_classes.ClassWithNestedFunction2.my_method()
        ).replace("before_module_with_classes", "module_name"),
        code_object_pickler._get_code_path(
            after_module_with_classes.ClassWithNestedFunction2.my_method()).
        replace("after_module_with_classes", "module_name"),
    )

  def test_identifiers_add_new_function_in_class(self):
    self.assertEqual(
        code_object_pickler._get_code_path(
            before_module_with_classes.ClassWithTwoMethods.my_method()).replace(
                "before_module_with_classes", "module_name"),
        code_object_pickler._get_code_path(
            after_module_with_classes.ClassWithTwoMethods.my_method()).replace(
                "after_module_with_classes", "module_name"),
    )

  def test_identifiers_remove_function_in_class(self):
    self.assertEqual(
        code_object_pickler._get_code_path(
            before_module_with_classes.RemoveMethod.my_method()).replace(
                "before_module_with_classes", "module_name"),
        code_object_pickler._get_code_path(
            after_module_with_classes.RemoveMethod.my_method()).replace(
                "after_module_with_classes", "module_name"),
    )

  def test_identifiers_add_global_variable(self):
    self.assertEqual(
        code_object_pickler._get_code_path(
            before_module_with_functions.my_function()).replace(
                "before_module_with_functions", "module_name"),
        code_object_pickler._get_code_path(
            after_module_with_global_variable.my_function()).replace(
                "after_module_with_global_variable", "module_name"),
    )

  def test_identifiers_add_top_level_function(self):
    self.assertEqual(
        code_object_pickler._get_code_path(
            before_module_with_functions.my_function()).replace(
                "before_module_with_functions", "module_name"),
        code_object_pickler._get_code_path(
            after_module_add_function.my_function()).replace(
                "after_module_add_function", "module_name"),
    )

  def test_identifiers_add_local_variable_in_function(self):
    self.assertEqual(
        code_object_pickler._get_code_path(
            before_module_with_functions.my_function()).replace(
                "before_module_with_functions", "module_name"),
        code_object_pickler._get_code_path(
            after_module_add_variable.my_function()).replace(
                "after_module_add_variable", "module_name"),
    )

  def test_identifiers_remove_local_variable_in_function(self):
    self.assertEqual(
        code_object_pickler._get_code_path(
            before_module_with_functions.my_function()).replace(
                "before_module_with_functions", "module_name"),
        code_object_pickler._get_code_path(
            after_module_remove_variable.my_function()).replace(
                "after_module_remove_variable", "module_name"),
    )

  def test_identifiers_nested_function_in_function(self):
    self.assertEqual(
        code_object_pickler._get_code_path(
            before_module_with_functions.my_function()).replace(
                "before_module_with_functions", "module_name"),
        code_object_pickler._get_code_path(
            after_module_with_nested_function.my_function()).replace(
                "after_module_with_nested_function", "module_name"),
    )

  def test_identifiers_nested_function_2_in_function(self):
    self.assertEqual(
        code_object_pickler._get_code_path(
            before_module_with_functions.my_function()).replace(
                "before_module_with_functions", "module_name"),
        code_object_pickler._get_code_path(
            after_module_with_nested_function_2.my_function()).replace(
                "after_module_with_nested_function_2", "module_name"),
    )

  def test_identifiers_add_class(self):
    self.assertEqual(
        code_object_pickler._get_code_path(
            before_module_with_functions.my_function()).replace(
                "before_module_with_functions", "module_name"),
        code_object_pickler._get_code_path(
            after_module_with_single_class.my_function()).replace(
                "after_module_with_single_class", "module_name"),
    )

  def test_identifiers_add_lambda_variable_in_function(self):
    self.assertNotEqual(
        code_object_pickler._get_code_path(
            before_module_with_functions.my_function()).replace(
                "before_module_with_functions", "module_name"),
        code_object_pickler._get_code_path(
            after_module_add_lambda_variable.my_function()).replace(
                "after_module_add_lambda_variable", "module_name"),
    )

  def test_identifiers_remove_lambda_variable_in_function(self):
    self.assertNotEqual(
        code_object_pickler._get_code_path(
            before_module_with_lambdas.my_function()).replace(
                "before_module_with_lambdas", "module_name"),
        code_object_pickler._get_code_path(
            after_module_remove_lambda_variable.my_function()).replace(
                "after_module_remove_lambda_variable", "module_name"),
    )


if __name__ == "__main__":
  unittest.main()
