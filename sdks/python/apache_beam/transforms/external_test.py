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

"""Unit tests for the transform.external classes."""

# pytype: skip-file

from __future__ import absolute_import

import logging
import sys
import typing
import unittest

from past.builtins import unicode

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.coders import RowCoder
from apache_beam.portability.api.external_transforms_pb2 import ExternalConfigurationPayload
from apache_beam.runners.portability import expansion_service
from apache_beam.runners.portability.expansion_service_test import FibTransform
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.external import ImplicitSchemaPayloadBuilder
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder
from apache_beam.typehints import typehints
from apache_beam.typehints.native_type_compatibility import convert_to_beam_type


def get_payload(args):
  return ExternalConfigurationPayload(configuration=args)


class PayloadBase(object):
  values = {
      'integer_example': 1,
      'boolean': True,
      'string_example': u'thing',
      'list_of_strings': [u'foo', u'bar'],
      'mapping': {
          u'key': 1.1
      },
      'optional_integer': None,
  }

  bytes_values = {
      'integer_example': 1,
      'boolean': True,
      'string_example': 'thing',
      'list_of_strings': ['foo', 'bar'],
      'mapping': {
          'key': 1.1
      },
      'optional_integer': None,
  }

  def get_payload_from_typing_hints(self, values):
    """Return ExternalConfigurationPayload based on python typing hints"""
    raise NotImplementedError

  def get_payload_from_beam_typehints(self, values):
    """Return ExternalConfigurationPayload based on beam typehints"""
    raise NotImplementedError

  def test_typing_payload_builder(self):
    result = self.get_payload_from_typing_hints(self.values)
    decoded = RowCoder(result.schema).decode(result.payload)
    for key, value in self.values.items():
      self.assertEqual(getattr(decoded, key), value)

  # TODO(BEAM-7372): Drop py2 specific "bytes" tests
  def test_typing_payload_builder_with_bytes(self):
    """
    string_utf8 coder will be used even if values are not unicode in python 2.x
    """
    result = self.get_payload_from_typing_hints(self.bytes_values)
    decoded = RowCoder(result.schema).decode(result.payload)
    for key, value in self.values.items():
      self.assertEqual(getattr(decoded, key), value)

  def test_typehints_payload_builder(self):
    result = self.get_payload_from_typing_hints(self.values)
    decoded = RowCoder(result.schema).decode(result.payload)
    for key, value in self.values.items():
      self.assertEqual(getattr(decoded, key), value)

  # TODO(BEAM-7372): Drop py2 specific "bytes" tests
  def test_typehints_payload_builder_with_bytes(self):
    """
    string_utf8 coder will be used even if values are not unicode in python 2.x
    """
    result = self.get_payload_from_typing_hints(self.bytes_values)
    decoded = RowCoder(result.schema).decode(result.payload)
    for key, value in self.values.items():
      self.assertEqual(getattr(decoded, key), value)

  def test_optional_error(self):
    """
    value can only be None if typehint is Optional
    """
    with self.assertRaises(ValueError):
      self.get_payload_from_typing_hints({k: None for k in self.values})


class ExternalTuplePayloadTest(PayloadBase, unittest.TestCase):
  def get_payload_from_typing_hints(self, values):
    TestSchema = typing.NamedTuple(
        'TestSchema',
        [
            ('integer_example', int),
            ('boolean', bool),
            ('string_example', unicode),
            ('list_of_strings', typing.List[unicode]),
            ('mapping', typing.Mapping[unicode, float]),
            ('optional_integer', typing.Optional[int]),
        ])

    builder = NamedTupleBasedPayloadBuilder(TestSchema(**values))
    return builder.build()

  def get_payload_from_beam_typehints(self, values):
    raise unittest.SkipTest(
        "Beam typehints cannot be used with "
        "typing.NamedTuple")


class ExternalImplicitPayloadTest(unittest.TestCase):
  """
  ImplicitSchemaPayloadBuilder works very differently than the other payload
  builders
  """
  def test_implicit_payload_builder(self):
    builder = ImplicitSchemaPayloadBuilder(PayloadBase.values)
    result = builder.build()

    decoded = RowCoder(result.schema).decode(result.payload)
    for key, value in PayloadBase.values.items():
      # Note the default value in the getattr call.
      # ImplicitSchemaPayloadBuilder omits fields with valu=None since their
      # type cannot be inferred.
      self.assertEqual(getattr(decoded, key, None), value)

  def test_implicit_payload_builder_with_bytes(self):
    values = PayloadBase.bytes_values
    builder = ImplicitSchemaPayloadBuilder(values)
    result = builder.build()

    decoded = RowCoder(result.schema).decode(result.payload)
    if sys.version_info[0] < 3:
      for key, value in PayloadBase.bytes_values.items():
        # Note the default value in the getattr call.
        # ImplicitSchemaPayloadBuilder omits fields with valu=None since their
        # type cannot be inferred.
        self.assertEqual(getattr(decoded, key, None), value)
    else:
      for key, value in PayloadBase.values.items():
        # Note the default value in the getattr call.
        # ImplicitSchemaPayloadBuilder omits fields with valu=None since their
        # type cannot be inferred.
        self.assertEqual(getattr(decoded, key, None), value)

    # Verify we have not modified a cached type (BEAM-10766)
    # TODO(BEAM-7372): Remove when bytes coercion code is removed.
    self.assertEqual(typehints.List[bytes],
                     convert_to_beam_type(typing.List[bytes]))


class ExternalTransformTest(unittest.TestCase):
  def test_pipeline_generation(self):
    pipeline = beam.Pipeline()
    _ = (
        pipeline
        | beam.Create(['a', 'b'])
        | beam.ExternalTransform(
            'beam:transforms:xlang:test:prefix',
            ImplicitSchemaPayloadBuilder({'data': u'0'}),
            expansion_service.ExpansionServiceServicer()))

    proto, _ = pipeline.to_runner_api(return_context=True)
    pipeline_from_proto = Pipeline.from_runner_api(
        proto, pipeline.runner, pipeline._options)

    # Original pipeline has the un-expanded external transform
    self.assertEqual([], pipeline.transforms_stack[0].parts[1].parts)

    # new pipeline has the expanded external transform
    self.assertNotEqual([],
                        pipeline_from_proto.transforms_stack[0].parts[1].parts)
    self.assertEqual(
        u'ExternalTransform(beam:transforms:xlang:test:prefix)/TestLabel',
        pipeline_from_proto.transforms_stack[0].parts[1].parts[0].full_label)

  def test_payload(self):
    with beam.Pipeline() as p:
      res = (
          p
          | beam.Create(['a', 'bb'], reshuffle=False)
          | beam.ExternalTransform(
              'payload', b's', expansion_service.ExpansionServiceServicer()))
      assert_that(res, equal_to(['as', 'bbs']))

  def test_nested(self):
    with beam.Pipeline() as p:
      assert_that(p | FibTransform(6), equal_to([8]))

  def test_unique_name(self):
    p = beam.Pipeline()
    _ = p | FibTransform(6)
    proto = p.to_runner_api()
    xforms = [x.unique_name for x in proto.components.transforms.values()]
    self.assertEqual(
        len(set(xforms)), len(xforms), msg='Transform names are not unique.')
    pcolls = [x.unique_name for x in proto.components.pcollections.values()]
    self.assertEqual(
        len(set(pcolls)), len(pcolls), msg='PCollection names are not unique.')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
