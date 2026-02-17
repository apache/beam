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

"""Module for testing code path generation with classes.
Counterpart to before_module_with_classes and is used as a test case
for various code changes.
"""


class AddLocalVariable:
  def my_method(self):
    a = 1  # pylint: disable=unused-variable
    b = lambda: 2
    new_local_variable = 3  # pylint: disable=unused-variable
    return b


class RemoveLocalVariable:
  def my_method(self):
    b = lambda: 2
    return b


class AddLambdaVariable:
  def my_method(self):
    a = 1  # pylint: disable=unused-variable
    b = lambda: 2
    c = lambda: 3  # pylint: disable=unused-variable
    return b


class RemoveLambdaVariable:
  def my_method(self):
    b = lambda: 2
    return b


class ClassWithNestedFunction:
  def my_method(self):
    def nested_function():  # pylint: disable=unused-variable
      c = 3
      return c

    a = 1  # pylint: disable=unused-variable
    b = lambda: 2
    return b


class ClassWithNestedFunction2:
  def my_method(self):
    a = 1  # pylint: disable=unused-variable
    b = lambda: 2

    def nested_function():  # pylint: disable=unused-variable
      c = 3
      return c

    return b


class ClassWithTwoMethods:
  def another_method(self):
    a = 1  # pylint: disable=unused-variable
    b = lambda: 2
    return b

  def my_method(self):
    a = 1  # pylint: disable=unused-variable
    b = lambda: 2
    return b


class RemoveMethod:
  def my_method(self):
    a = 1  # pylint: disable=unused-variable

    b = lambda: 2
    return b
