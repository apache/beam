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

import apache_beam as beam
from apache_beam.runners.portability import expansion_service
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.external import ImplicitSchemaPayloadBuilder
from apache_beam.transforms.fully_qualified_named_transform import PYTHON_FULLY_QUALIFIED_NAMED_TRANSFORM_URN
from apache_beam.transforms.fully_qualified_named_transform import FullyQualifiedNamedTransform


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
