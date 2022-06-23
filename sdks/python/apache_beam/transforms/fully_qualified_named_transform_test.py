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
# pytype: skip-file

import unittest

from mock import call
from mock import patch

import apache_beam as beam
from apache_beam.runners.portability import expansion_service
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.external import ImplicitSchemaPayloadBuilder
from apache_beam.transforms.fully_qualified_named_transform import PYTHON_FULLY_QUALIFIED_NAMED_TRANSFORM_URN
from apache_beam.transforms.fully_qualified_named_transform import FullyQualifiedNamedTransform
from apache_beam.utils import python_callable


class FullyQualifiedNamedTransformTest(unittest.TestCase):
  def test_test_transform(self):
    with beam.Pipeline() as p:
      assert_that(
          p | beam.Create(['a', 'b', 'c']) | _TestTransform('x', 'y'),
          equal_to(['xay', 'xby', 'xcy']))

  def test_expand(self):
    with FullyQualifiedNamedTransform.with_filter('*'):
      with beam.Pipeline() as p:
        assert_that(
            p | beam.Create(['a', 'b', 'c'])
            | FullyQualifiedNamedTransform(
                'apache_beam.transforms.fully_qualified_named_transform_test'
                '._TestTransform', ('x', ), {'suffix': 'y'}),
            equal_to(['xay', 'xby', 'xcy']))

  def test_static_constructor(self):
    with FullyQualifiedNamedTransform.with_filter('*'):
      with beam.Pipeline() as p:
        assert_that(
            p | beam.Create(['a', 'b', 'c'])
            | FullyQualifiedNamedTransform(
                'apache_beam.transforms.fully_qualified_named_transform_test'
                '._TestTransform.create', ('x', ), {'suffix': 'y'}),
            equal_to(['xay', 'xby', 'xcy']))

  def test_as_external_transform(self):
    with FullyQualifiedNamedTransform.with_filter('*'):
      with beam.Pipeline() as p:
        assert_that(
            p
            | beam.Create(['a', 'b', 'c'])
            | beam.ExternalTransform(
                PYTHON_FULLY_QUALIFIED_NAMED_TRANSFORM_URN,
                ImplicitSchemaPayloadBuilder({
                    'constructor': 'apache_beam.transforms'
                    '.fully_qualified_named_transform_test._TestTransform',
                    'args': beam.Row(arg0='x'),
                    'kwargs': beam.Row(suffix='y'),
                }),
                expansion_service.ExpansionServiceServicer()),
            equal_to(['xay', 'xby', 'xcy']))

  def test_as_external_transform_no_args(self):
    with FullyQualifiedNamedTransform.with_filter('*'):
      with beam.Pipeline() as p:
        assert_that(
            p
            | beam.Create(['a', 'b', 'c'])
            | beam.ExternalTransform(
                PYTHON_FULLY_QUALIFIED_NAMED_TRANSFORM_URN,
                ImplicitSchemaPayloadBuilder({
                    'constructor': 'apache_beam.transforms'
                    '.fully_qualified_named_transform_test._TestTransform',
                    'kwargs': beam.Row(prefix='x', suffix='y'),
                }),
                expansion_service.ExpansionServiceServicer()),
            equal_to(['xay', 'xby', 'xcy']))

  def test_as_external_transform_no_kwargs(self):
    with FullyQualifiedNamedTransform.with_filter('*'):
      with beam.Pipeline() as p:
        assert_that(
            p
            | beam.Create(['a', 'b', 'c'])
            | beam.ExternalTransform(
                PYTHON_FULLY_QUALIFIED_NAMED_TRANSFORM_URN,
                ImplicitSchemaPayloadBuilder({
                    'constructor': 'apache_beam.transforms'
                    '.fully_qualified_named_transform_test._TestTransform',
                    'args': beam.Row(arg0='x', arg1='y'),
                }),
                expansion_service.ExpansionServiceServicer()),
            equal_to(['xay', 'xby', 'xcy']))

  def test_callable_transform(self):
    with FullyQualifiedNamedTransform.with_filter('*'):
      with beam.Pipeline() as p:
        assert_that(
            p | beam.Create(['a', 'b', 'c'])
            | FullyQualifiedNamedTransform(
                '__callable__',  # the next argument is a callable to be applied
                (
                  python_callable.PythonCallableWithSource("""
                      def func(pcoll, x):
                        return pcoll | beam.Map(lambda e: e + x)
                      """),
                  'x'  # arguments passed to the callable
                ),
                {}),
            equal_to(['ax', 'bx', 'cx']))

  def test_constructor_transform(self):
    with FullyQualifiedNamedTransform.with_filter('*'):
      with beam.Pipeline() as p:
        assert_that(
            p | beam.Create(['a', 'b', 'c'])
            | FullyQualifiedNamedTransform(
                '__constructor__',  # the next argument constructs a PTransform
                (),
                {
                'source': python_callable.PythonCallableWithSource("""
                    class MyTransform(beam.PTransform):
                      def __init__(self, x):
                        self._x = x
                      def expand(self, pcoll):
                        return pcoll | beam.Map(lambda e: e + self._x)
                    """),
                'x': 'x'  # arguments passed to the above constructor
                }
                ),
            equal_to(['ax', 'bx', 'cx']))

  def test_glob_filter(self):
    with FullyQualifiedNamedTransform.with_filter('*'):
      self.assertIs(
          FullyQualifiedNamedTransform._resolve('apache_beam.Row'), beam.Row)

    with FullyQualifiedNamedTransform.with_filter('apache_beam.*'):
      self.assertIs(
          FullyQualifiedNamedTransform._resolve('apache_beam.Row'), beam.Row)

    with FullyQualifiedNamedTransform.with_filter('apache_beam.foo.*'):
      with self.assertRaises(ValueError):
        FullyQualifiedNamedTransform._resolve('apache_beam.Row')

  @patch('importlib.import_module')
  def test_resolve_by_path_segment(self, mock_import_module):
    mock_import_module.return_value = None
    with FullyQualifiedNamedTransform.with_filter('*'):
      FullyQualifiedNamedTransform._resolve('a.b.c.d')
    mock_import_module.assert_has_calls(
        [call('a'), call('a.b'), call('a.b.c'), call('a.b.c.d')])

  def test_resolve(self):
    # test _resolve with the module that is not exposed to the top level
    with FullyQualifiedNamedTransform.with_filter('*'):
      dataframe_transform = FullyQualifiedNamedTransform._resolve(
          'apache_beam.dataframe.transforms.DataframeTransform')
      from apache_beam.dataframe.transforms import DataframeTransform
      self.assertIs(dataframe_transform, DataframeTransform)

    # test _resolve with the module that will never be exposed
    # to the top level in the future
    with FullyQualifiedNamedTransform.with_filter('*'):
      argument_placeholder = FullyQualifiedNamedTransform._resolve(
          'apache_beam.internal.util.ArgumentPlaceholder')
      from apache_beam.internal.util import ArgumentPlaceholder
      self.assertIs(argument_placeholder, ArgumentPlaceholder)


class _TestTransform(beam.PTransform):
  @classmethod
  def create(cls, *args, **kwargs):
    return cls(*args, **kwargs)

  def __init__(self, prefix, suffix):
    self._prefix = prefix
    self._suffix = suffix

  def expand(self, pcoll):
    return pcoll | beam.Map(lambda s: self._prefix + s + self._suffix)


if __name__ == '__main__':
  unittest.main()
