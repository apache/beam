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

import hashlib
import unittest

from apache_beam.internal import code_object_pickler
from parameterized import parameterized


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


test_cases = [
    (
        top_level_function,
        "apache_beam.internal.code_object_pickler_test.top_level_function"
        ".__code__"),
    (
        top_level_lambda,
        "apache_beam.internal.code_object_pickler_test.top_level_lambda"
        ".__code__"),
    (
        get_nested_function(),
        (
            "apache_beam.internal.code_object_pickler_test.get_nested_function"
            ".__code__.co_consts[nested_function]")),
    (
        get_lambda_from_dictionary(),
        (
            "apache_beam.internal.code_object_pickler_test"
            ".get_lambda_from_dictionary.__code__.co_consts[<lambda>, ('x',)]")
    ),
    (
        get_lambda_from_dictionary_same_args(),
        (
            "apache_beam.internal.code_object_pickler_test"
            ".get_lambda_from_dictionary_same_args.__code__.co_consts[<lambda>, ('x',), "
            + hashlib.md5(
                get_lambda_from_dictionary_same_args.__code__.co_code).hexdigest() +
            "]")),
    (
        function_with_lambda_default_argument(),
        (
            "apache_beam.internal.code_object_pickler_test"
            ".function_with_lambda_default_argument.__defaults__[0].__code__")),
    (
        function_with_function_default_argument(),
        "apache_beam.internal.code_object_pickler_test.top_level_function"
        ".__code__"),
    (
        add_one,
        "apache_beam.internal.code_object_pickler_test.function_decorator"
        ".__code__.co_consts[<lambda>]"),
    (
        ClassWithFunction.process,
        "apache_beam.internal.code_object_pickler_test.ClassWithFunction.process"
        ".__code__"),
    (
        ClassWithStaticMethod.static_method,
        "apache_beam.internal.code_object_pickler_test.ClassWithStaticMethod"
        ".static_method.__code__"),
    (
        ClassWithClassMethod.class_method,
        "apache_beam.internal.code_object_pickler_test.ClassWithClassMethod"
        ".class_method.__code__"),
    (
        ClassWithNestedFunction().process(),
        (
            "apache_beam.internal.code_object_pickler_test"
            ".ClassWithNestedFunction.process.__code__.co_consts"
            "[nested_function]")),
    (
        ClassWithLambda().process(),
        "apache_beam.internal.code_object_pickler_test.ClassWithLambda.process"
        ".__code__.co_consts[<lambda>]"),
    (
        ClassWithNestedClass.InnerClass().process,
        "apache_beam.internal.code_object_pickler_test.ClassWithNestedClass"
        ".InnerClass.process.__code__"),
    (
        ClassWithNestedLambda().process(),
        (
            "apache_beam.internal.code_object_pickler_test.ClassWithNestedLambda"
            ".process.__code__.co_consts[get_lambda_from_dictionary].co_consts"
            "[<lambda>, ('x',)]")),
    (
        ClassWithNestedLambda.process,
        "apache_beam.internal.code_object_pickler_test.ClassWithNestedLambda"
        ".process.__code__"),
]


class DillTest(unittest.TestCase):
  @parameterized.expand(test_cases)
  def test_get_code_path(self, callable, expected):
    actual = code_object_pickler._get_code_path(callable)
    self.assertEqual(actual, expected)

  @parameterized.expand(test_cases)
  def test_get_code_from_stable_reference(self, callable, path):
    actual = code_object_pickler._get_code_from_stable_reference(path)
    self.assertEqual(actual, callable.__code__)

  @parameterized.expand(test_cases)
  def test_roundtrip(self, callable, _):
    path = code_object_pickler._get_code_path(callable)
    actual = code_object_pickler._get_code_from_stable_reference(path)
    self.assertEqual(actual, callable.__code__)


if __name__ == "__main__":
  unittest.main()
