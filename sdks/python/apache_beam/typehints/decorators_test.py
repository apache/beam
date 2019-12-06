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

"""Tests for decorators module."""

from __future__ import absolute_import

import sys
import unittest

from apache_beam.typehints import Any
from apache_beam.typehints import List
from apache_beam.typehints import WithTypeHints
from apache_beam.typehints import decorators

decorators._enable_from_callable = True


class IOTypeHintsTest(unittest.TestCase):

  def test_get_signature(self):
    # Basic coverage only to make sure function works in Py2 and Py3.
    def fn(a, b=1, *c, **d):
      return a, b, c, d
    s = decorators.get_signature(fn)
    self.assertListEqual(list(s.parameters), ['a', 'b', 'c', 'd'])

  def test_get_signature_builtin(self):
    # Tests a builtin function for 3.7+ and fallback result for older versions.
    s = decorators.get_signature(list)
    if sys.version_info < (3, 7):
      self.assertListEqual(list(s.parameters),
                           ['_', '__unknown__varargs', '__unknown__keywords'])
    else:
      self.assertListEqual(list(s.parameters),
                           ['iterable'])
    self.assertEqual(s.return_annotation, List[Any])

  def test_from_callable_without_annotations(self):
    # Python 2 doesn't support annotations. See decorators_test_py3.py for that.
    def fn(a, b=None, *args, **kwargs):
      return a, b, args, kwargs
    th = decorators.IOTypeHints.from_callable(fn)
    self.assertIsNone(th)

  def test_from_callable_builtin(self):
    th = decorators.IOTypeHints.from_callable(len)
    self.assertIsNone(th)

  def test_from_callable_method_descriptor(self):
    # from_callable() injects an annotation in this special type of builtin.
    th = decorators.IOTypeHints.from_callable(str.strip)
    if sys.version_info >= (3, 7):
      self.assertEqual(th.input_types, ((str, Any), {}))
    else:
      self.assertEqual(th.input_types,
                       ((str, decorators._ANY_VAR_POSITIONAL),
                        {'__unknown__keywords': decorators._ANY_VAR_KEYWORD}))
    self.assertEqual(th.output_types, ((Any,), {}))


class WithTypeHintsTest(unittest.TestCase):
  def test_get_type_hints_no_settings(self):
    class Base(WithTypeHints):
      pass

    th = Base().get_type_hints()
    self.assertEqual(th.input_types, None)
    self.assertEqual(th.output_types, None)

  def test_get_type_hints_class_decorators(self):
    @decorators.with_input_types(int, str)
    @decorators.with_output_types(int)
    class Base(WithTypeHints):
      pass

    th = Base().get_type_hints()
    self.assertEqual(th.input_types, ((int, str), {}))
    self.assertEqual(th.output_types, ((int, ), {}))

  def test_get_type_hints_class_defaults(self):
    class Base(WithTypeHints):
      def default_type_hints(self):
        return decorators.IOTypeHints(
            input_types=((int, str), {}),
            output_types=((int, ), {}))

    th = Base().get_type_hints()
    self.assertEqual(th.input_types, ((int, str), {}))
    self.assertEqual(th.output_types, ((int, ), {}))

  def test_get_type_hints_precedence_defaults_over_decorators(self):
    @decorators.with_input_types(int)
    @decorators.with_output_types(str)
    class Base(WithTypeHints):
      def default_type_hints(self):
        return decorators.IOTypeHints(
            input_types=((float, ), {}))

    th = Base().get_type_hints()
    self.assertEqual(th.input_types, ((float, ), {}))
    self.assertEqual(th.output_types, ((str, ), {}))

  def test_get_type_hints_precedence_instance_over_defaults(self):
    class Base(WithTypeHints):
      def default_type_hints(self):
        return decorators.IOTypeHints(
            input_types=((float, ), {}), output_types=((str, ), {}))

    th = Base().with_input_types(int).get_type_hints()
    self.assertEqual(th.input_types, ((int, ), {}))
    self.assertEqual(th.output_types, ((str, ), {}))

  def test_inherits_does_not_modify(self):
    # See BEAM-8629.
    @decorators.with_output_types(int)
    class Subclass(WithTypeHints):
      def __init__(self):
        pass  # intentionally avoiding super call
    # These should be equal, but not the same object lest mutating the instance
    # mutates the class.
    self.assertFalse(
        Subclass()._get_or_create_type_hints() is Subclass._type_hints)
    self.assertEqual(
        Subclass().get_type_hints(), Subclass._type_hints)
    self.assertNotEqual(
        Subclass().with_input_types(str)._type_hints,
        Subclass._type_hints)


if __name__ == '__main__':
  unittest.main()
