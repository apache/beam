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

import dataclasses
import logging
import os
import tempfile
import typing
import unittest

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.coders import RowCoder
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.portability.api.external_transforms_pb2 import BuilderMethod
from apache_beam.portability.api.external_transforms_pb2 import ExternalConfigurationPayload
from apache_beam.portability.api.external_transforms_pb2 import JavaClassLookupPayload
from apache_beam.runners import pipeline_context
from apache_beam.runners.portability import expansion_service
from apache_beam.runners.portability.expansion_service_test import FibTransform
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.external import AnnotationBasedPayloadBuilder
from apache_beam.transforms.external import ImplicitSchemaPayloadBuilder
from apache_beam.transforms.external import JavaClassLookupPayloadBuilder
from apache_beam.transforms.external import JavaExternalTransform
from apache_beam.transforms.external import JavaJarExpansionService
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder
from apache_beam.typehints import typehints
from apache_beam.typehints.native_type_compatibility import convert_to_beam_type
from apache_beam.utils import proto_utils

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.runners.dataflow.internal import apiclient
except ImportError:
  apiclient = None  # type: ignore
# pylint: enable=wrong-import-order, wrong-import-position


def get_payload(cls):
  payload = ExternalConfigurationPayload()
  payload.ParseFromString(cls._payload)
  return payload


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

  def test_typehints_payload_builder(self):
    result = self.get_payload_from_typing_hints(self.values)
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
            ('string_example', str),
            ('list_of_strings', typing.List[str]),
            ('mapping', typing.Mapping[str, float]),
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
    for key, value in PayloadBase.values.items():
      # Note the default value in the getattr call.
      # ImplicitSchemaPayloadBuilder omits fields with valu=None since their
      # type cannot be inferred.
      self.assertEqual(getattr(decoded, key, None), value)

    # Verify we have not modified a cached type (BEAM-10766)
    # TODO(BEAM-7372): Remove when bytes coercion code is removed.
    self.assertEqual(
        typehints.List[bytes], convert_to_beam_type(typing.List[bytes]))


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

  @unittest.skipIf(apiclient is None, 'GCP dependencies are not installed')
  def test_pipeline_generation_with_runner_overrides(self):
    pipeline_properties = [
        '--dataflow_endpoint=ignored',
        '--job_name=test-job',
        '--project=test-project',
        '--staging_location=ignored',
        '--temp_location=/dev/null',
        '--no_auth',
        '--dry_run=True',
        '--sdk_location=container',
        '--runner=DataflowRunner',
        '--streaming'
    ]

    with beam.Pipeline(options=PipelineOptions(pipeline_properties)) as p:
      _ = (
          p
          | beam.io.ReadFromPubSub(
              subscription=
              'projects/dummy-project/subscriptions/dummy-subscription')
          | beam.ExternalTransform(
              'beam:transforms:xlang:test:prefix',
              ImplicitSchemaPayloadBuilder({'data': u'0'}),
              expansion_service.ExpansionServiceServicer()))

    pipeline_proto, _ = p.to_runner_api(return_context=True)

    pubsub_read_transform = None
    external_transform = None
    proto_transforms = pipeline_proto.components.transforms
    for id in proto_transforms:
      if 'beam:transforms:xlang:test:prefix' in proto_transforms[
          id].unique_name:
        external_transform = proto_transforms[id]
      if 'ReadFromPubSub' in proto_transforms[id].unique_name:
        pubsub_read_transform = proto_transforms[id]

    if not (pubsub_read_transform and external_transform):
      raise ValueError(
          'Could not find an external transform and the PubSub read transform '
          'in the pipeline')

    self.assertEqual(1, len(list(pubsub_read_transform.outputs.values())))
    self.assertEqual(
        list(pubsub_read_transform.outputs.values()),
        list(external_transform.inputs.values()))

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

  def test_external_empty_spec_translation(self):
    pipeline = beam.Pipeline()
    external_transform = beam.ExternalTransform(
        'beam:transforms:xlang:test:prefix',
        ImplicitSchemaPayloadBuilder({'data': u'0'}),
        expansion_service.ExpansionServiceServicer())
    _ = (pipeline | beam.Create(['a', 'b']) | external_transform)
    pipeline.run().wait_until_finish()

    external_transform_label = (
        'ExternalTransform(beam:transforms:xlang:test:prefix)/TestLabel')
    for transform in external_transform._expanded_components.transforms.values(
    ):
      # We clear the spec of one of the external transforms.
      if transform.unique_name == external_transform_label:
        transform.spec.Clear()

    context = pipeline_context.PipelineContext()
    proto_pipeline = pipeline.to_runner_api(context=context)

    proto_transform = None
    for transform in proto_pipeline.components.transforms.values():
      if (transform.unique_name ==
          'ExternalTransform(beam:transforms:xlang:test:prefix)/TestLabel'):
        proto_transform = transform

    self.assertIsNotNone(proto_transform)
    self.assertTrue(str(proto_transform).strip().find('spec {') == -1)

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

  def test_external_transform_finder_non_leaf(self):
    pipeline = beam.Pipeline()
    _ = (
        pipeline
        | beam.Create(['a', 'b'])
        | beam.ExternalTransform(
            'beam:transforms:xlang:test:prefix',
            ImplicitSchemaPayloadBuilder({'data': u'0'}),
            expansion_service.ExpansionServiceServicer())
        | beam.Map(lambda x: x))
    pipeline.run().wait_until_finish()

    self.assertTrue(pipeline.contains_external_transforms)

  def test_external_transform_finder_leaf(self):
    pipeline = beam.Pipeline()
    _ = (
        pipeline
        | beam.Create(['a', 'b'])
        | beam.ExternalTransform(
            'beam:transforms:xlang:test:nooutput',
            ImplicitSchemaPayloadBuilder({'data': u'0'}),
            expansion_service.ExpansionServiceServicer()))
    pipeline.run().wait_until_finish()

    self.assertTrue(pipeline.contains_external_transforms)


class ExternalAnnotationPayloadTest(PayloadBase, unittest.TestCase):
  def get_payload_from_typing_hints(self, values):
    class AnnotatedTransform(beam.ExternalTransform):
      URN = 'beam:external:fakeurn:v1'

      def __init__(
          self,
          integer_example: int,
          boolean: bool,
          string_example: str,
          list_of_strings: typing.List[str],
          mapping: typing.Mapping[str, float],
          optional_integer: typing.Optional[int] = None,
          expansion_service=None):
        super().__init__(
            self.URN,
            AnnotationBasedPayloadBuilder(
                self,
                integer_example=integer_example,
                boolean=boolean,
                string_example=string_example,
                list_of_strings=list_of_strings,
                mapping=mapping,
                optional_integer=optional_integer,
            ),
            expansion_service)

    return get_payload(AnnotatedTransform(**values))

  def get_payload_from_beam_typehints(self, values):
    class AnnotatedTransform(beam.ExternalTransform):
      URN = 'beam:external:fakeurn:v1'

      def __init__(
          self,
          integer_example: int,
          boolean: bool,
          string_example: str,
          list_of_strings: typehints.List[str],
          mapping: typehints.Dict[str, float],
          optional_integer: typehints.Optional[int] = None,
          expansion_service=None):
        super().__init__(
            self.URN,
            AnnotationBasedPayloadBuilder(
                self,
                integer_example=integer_example,
                boolean=boolean,
                string_example=string_example,
                list_of_strings=list_of_strings,
                mapping=mapping,
                optional_integer=optional_integer,
            ),
            expansion_service)

    return get_payload(AnnotatedTransform(**values))


class ExternalDataclassesPayloadTest(PayloadBase, unittest.TestCase):
  def get_payload_from_typing_hints(self, values):
    @dataclasses.dataclass
    class DataclassTransform(beam.ExternalTransform):
      URN = 'beam:external:fakeurn:v1'

      integer_example: int
      boolean: bool
      string_example: str
      list_of_strings: typing.List[str]
      mapping: typing.Mapping[str, float] = dataclasses.field(default=dict)
      optional_integer: typing.Optional[int] = None
      expansion_service: dataclasses.InitVar[typing.Optional[str]] = None

    return get_payload(DataclassTransform(**values))

  def get_payload_from_beam_typehints(self, values):
    @dataclasses.dataclass
    class DataclassTransform(beam.ExternalTransform):
      URN = 'beam:external:fakeurn:v1'

      integer_example: int
      boolean: bool
      string_example: str
      list_of_strings: typehints.List[str]
      mapping: typehints.Dict[str, float] = {}
      optional_integer: typehints.Optional[int] = None
      expansion_service: dataclasses.InitVar[typehints.Optional[str]] = None

    return get_payload(DataclassTransform(**values))


class JavaClassLookupPayloadBuilderTest(unittest.TestCase):
  def _verify_row(self, schema, row_payload, expected_values):
    row = RowCoder(schema).decode(row_payload)

    for attr_name, expected_value in expected_values.items():
      self.assertTrue(hasattr(row, attr_name))
      value = getattr(row, attr_name)
      self.assertEqual(expected_value, value)

  def test_build_payload_with_constructor(self):
    payload_builder = JavaClassLookupPayloadBuilder('dummy_class_name')

    payload_builder.with_constructor('abc', 123, str_field='def', int_field=456)
    payload_bytes = payload_builder.payload()
    payload_from_bytes = proto_utils.parse_Bytes(
        payload_bytes, JavaClassLookupPayload)
    self.assertTrue(isinstance(payload_from_bytes, JavaClassLookupPayload))
    self.assertFalse(payload_from_bytes.constructor_method)
    self._verify_row(
        payload_from_bytes.constructor_schema,
        payload_from_bytes.constructor_payload, {
            'ignore0': 'abc',
            'ignore1': 123,
            'str_field': 'def',
            'int_field': 456
        })

  def test_build_payload_with_constructor_method(self):
    payload_builder = JavaClassLookupPayloadBuilder('dummy_class_name')
    payload_builder.with_constructor_method(
        'dummy_constructor_method', 'abc', 123, str_field='def', int_field=456)
    payload_bytes = payload_builder.payload()
    payload_from_bytes = proto_utils.parse_Bytes(
        payload_bytes, JavaClassLookupPayload)
    self.assertTrue(isinstance(payload_from_bytes, JavaClassLookupPayload))
    self.assertEqual(
        'dummy_constructor_method', payload_from_bytes.constructor_method)
    self._verify_row(
        payload_from_bytes.constructor_schema,
        payload_from_bytes.constructor_payload, {
            'ignore0': 'abc',
            'ignore1': 123,
            'str_field': 'def',
            'int_field': 456
        })

  def test_build_payload_with_builder_methods(self):
    payload_builder = JavaClassLookupPayloadBuilder('dummy_class_name')
    payload_builder.with_constructor('abc', 123, str_field='def', int_field=456)
    payload_builder.add_builder_method(
        'builder_method1', 'abc1', 1234, str_field1='abc2', int_field1=2345)
    payload_builder.add_builder_method(
        'builder_method2', 'abc3', 3456, str_field2='abc4', int_field2=4567)
    payload_bytes = payload_builder.payload()
    payload_from_bytes = proto_utils.parse_Bytes(
        payload_bytes, JavaClassLookupPayload)
    self.assertTrue(isinstance(payload_from_bytes, JavaClassLookupPayload))
    self._verify_row(
        payload_from_bytes.constructor_schema,
        payload_from_bytes.constructor_payload, {
            'ignore0': 'abc',
            'ignore1': 123,
            'str_field': 'def',
            'int_field': 456
        })
    self.assertEqual(2, len(payload_from_bytes.builder_methods))
    builder_method = payload_from_bytes.builder_methods[0]
    self.assertTrue(isinstance(builder_method, BuilderMethod))
    self.assertEqual('builder_method1', builder_method.name)

    self._verify_row(
        builder_method.schema,
        builder_method.payload,
        {
            'ignore0': 'abc1',
            'ignore1': 1234,
            'str_field1': 'abc2',
            'int_field1': 2345
        })

    builder_method = payload_from_bytes.builder_methods[1]
    self.assertTrue(isinstance(builder_method, BuilderMethod))
    self.assertEqual('builder_method2', builder_method.name)
    self._verify_row(
        builder_method.schema,
        builder_method.payload,
        {
            'ignore0': 'abc3',
            'ignore1': 3456,
            'str_field2': 'abc4',
            'int_field2': 4567
        })

  def test_build_payload_with_constructor_twice_fails(self):
    payload_builder = JavaClassLookupPayloadBuilder('dummy_class_name')
    payload_builder.with_constructor('abc')
    with self.assertRaises(ValueError):
      payload_builder.with_constructor('def')

  def test_implicit_builder_with_constructor(self):
    constructor_transform = (
        JavaExternalTransform('org.pkg.MyTransform')('abc').withIntProperty(5))

    payload_bytes = constructor_transform._payload_builder.payload()
    payload_from_bytes = proto_utils.parse_Bytes(
        payload_bytes, JavaClassLookupPayload)
    self.assertEqual('org.pkg.MyTransform', payload_from_bytes.class_name)
    self._verify_row(
        payload_from_bytes.constructor_schema,
        payload_from_bytes.constructor_payload, {'ignore0': 'abc'})
    builder_method = payload_from_bytes.builder_methods[0]
    self.assertEqual('withIntProperty', builder_method.name)
    self._verify_row(
        builder_method.schema, builder_method.payload, {'ignore0': 5})

  def test_implicit_builder_with_constructor_method(self):
    constructor_transform = JavaExternalTransform('org.pkg.MyTransform').of(
        str_field='abc').withProperty(int_field=1234).build()

    payload_bytes = constructor_transform._payload_builder.payload()
    payload_from_bytes = proto_utils.parse_Bytes(
        payload_bytes, JavaClassLookupPayload)
    self.assertEqual('of', payload_from_bytes.constructor_method)
    self._verify_row(
        payload_from_bytes.constructor_schema,
        payload_from_bytes.constructor_payload, {'str_field': 'abc'})
    with_property_method = payload_from_bytes.builder_methods[0]
    self.assertEqual('withProperty', with_property_method.name)
    self._verify_row(
        with_property_method.schema,
        with_property_method.payload, {'int_field': 1234})
    build_method = payload_from_bytes.builder_methods[1]
    self.assertEqual('build', build_method.name)
    self._verify_row(build_method.schema, build_method.payload, {})


class JavaJarExpansionServiceTest(unittest.TestCase):
  def test_classpath(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      try:
        # Avoid having to prefix everything in our test strings.
        oldwd = os.getcwd()
        os.chdir(temp_dir)
        # Touch some files for globing.
        with open('a1.jar', 'w') as _:
          pass

        service = JavaJarExpansionService(
            'main.jar', classpath=['a*.jar', 'b.jar'])
        self.assertEqual(
            service._default_args(),
            ['{{PORT}}', '--filesToStage=main.jar,a1.jar,b.jar'])

      finally:
        os.chdir(oldwd)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
