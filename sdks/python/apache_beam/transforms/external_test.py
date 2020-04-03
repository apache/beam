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
from apache_beam.coders import BooleanCoder
from apache_beam.coders import FloatCoder
from apache_beam.coders import IterableCoder
from apache_beam.coders import StrUtf8Coder
from apache_beam.coders import TupleCoder
from apache_beam.coders import VarIntCoder
from apache_beam.portability.api.external_transforms_pb2 import ConfigValue
from apache_beam.portability.api.external_transforms_pb2 import ExternalConfigurationPayload
from apache_beam.runners.portability import expansion_service
from apache_beam.runners.portability.expansion_service_test import FibTransform
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.external import ImplicitSchemaPayloadBuilder
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder


def get_payload(args):
  return ExternalConfigurationPayload(configuration=args)


class PayloadBase(object):
  values = {
      'integer_example': 1,
      'boolean': True,
      'string_example': u'thing',
      'list_of_strings': [u'foo', u'bar'],
      'optional_kv': (u'key', 1.1),
      'optional_integer': None,
  }

  bytes_values = {
      'integer_example': 1,
      'boolean': True,
      'string_example': 'thing',
      'list_of_strings': ['foo', 'bar'],
      'optional_kv': ('key', 1.1),
      'optional_integer': None,
  }

  args = {
      'integer_example': ConfigValue(
          coder_urn=['beam:coder:varint:v1'],
          payload=VarIntCoder().get_impl().encode_nested(
              values['integer_example'])),
      'boolean': ConfigValue(
          coder_urn=['beam:coder:bool:v1'],
          payload=BooleanCoder().get_impl().encode_nested(values['boolean'])),
      'string_example': ConfigValue(
          coder_urn=['beam:coder:string_utf8:v1'],
          payload=StrUtf8Coder().get_impl().encode_nested(
              values['string_example'])),
      'list_of_strings': ConfigValue(
          coder_urn=['beam:coder:iterable:v1', 'beam:coder:string_utf8:v1'],
          payload=IterableCoder(StrUtf8Coder()).get_impl().encode_nested(
              values['list_of_strings'])),
      'optional_kv': ConfigValue(
          coder_urn=[
              'beam:coder:kv:v1',
              'beam:coder:string_utf8:v1',
              'beam:coder:double:v1'
          ],
          payload=TupleCoder([StrUtf8Coder(),
                              FloatCoder()]).get_impl().encode_nested(
                                  values['optional_kv'])),
  }

  def get_payload_from_typing_hints(self, values):
    """Return ExternalConfigurationPayload based on python typing hints"""
    raise NotImplementedError

  def get_payload_from_beam_typehints(self, values):
    """Return ExternalConfigurationPayload based on beam typehints"""
    raise NotImplementedError

  def test_typing_payload_builder(self):
    result = self.get_payload_from_typing_hints(self.values)
    expected = get_payload(self.args)
    self.assertEqual(result, expected)

  def test_typing_payload_builder_with_bytes(self):
    """
    string_utf8 coder will be used even if values are not unicode in python 2.x
    """
    result = self.get_payload_from_typing_hints(self.bytes_values)
    expected = get_payload(self.args)
    self.assertEqual(result, expected)

  def test_typehints_payload_builder(self):
    result = self.get_payload_from_beam_typehints(self.values)
    expected = get_payload(self.args)
    self.assertEqual(result, expected)

  def test_typehints_payload_builder_with_bytes(self):
    """
    string_utf8 coder will be used even if values are not unicode in python 2.x
    """
    result = self.get_payload_from_beam_typehints(self.bytes_values)
    expected = get_payload(self.args)
    self.assertEqual(result, expected)

  def test_optional_error(self):
    """
    value can only be None if typehint is Optional
    """
    with self.assertRaises(RuntimeError):
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
            ('optional_kv', typing.Optional[typing.Tuple[unicode, float]]),
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
    expected = get_payload(PayloadBase.args)
    self.assertEqual(result, expected)

  def test_implicit_payload_builder_with_bytes(self):
    values = PayloadBase.bytes_values
    builder = ImplicitSchemaPayloadBuilder(values)
    result = builder.build()
    if sys.version_info[0] < 3:
      # in python 2.x bytes coder will be inferred
      args = {
          'integer_example': ConfigValue(
              coder_urn=['beam:coder:varint:v1'],
              payload=VarIntCoder().get_impl().encode_nested(
                  values['integer_example'])),
          'boolean': ConfigValue(
              coder_urn=['beam:coder:bool:v1'],
              payload=BooleanCoder().get_impl().encode_nested(
                  values['boolean'])),
          'string_example': ConfigValue(
              coder_urn=['beam:coder:bytes:v1'],
              payload=StrUtf8Coder().get_impl().encode_nested(
                  values['string_example'])),
          'list_of_strings': ConfigValue(
              coder_urn=['beam:coder:iterable:v1', 'beam:coder:bytes:v1'],
              payload=IterableCoder(StrUtf8Coder()).get_impl().encode_nested(
                  values['list_of_strings'])),
          'optional_kv': ConfigValue(
              coder_urn=[
                  'beam:coder:kv:v1',
                  'beam:coder:bytes:v1',
                  'beam:coder:double:v1'
              ],
              payload=TupleCoder([StrUtf8Coder(),
                                  FloatCoder()]).get_impl().encode_nested(
                                      values['optional_kv'])),
      }
      expected = get_payload(args)
      self.assertEqual(result, expected)
    else:
      expected = get_payload(PayloadBase.args)
      self.assertEqual(result, expected)


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
