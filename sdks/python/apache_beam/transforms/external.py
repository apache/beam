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

"""Defines Transform whose expansion is implemented elsewhere.

No backward compatibility guarantees. Everything in this module is experimental.
"""
# pytype: skip-file

import contextlib
import copy
import functools
import glob
import logging
import threading
from collections import OrderedDict
from typing import Dict

import grpc

from apache_beam import pvalue
from apache_beam.coders import RowCoder
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.portability.api import beam_expansion_api_pb2
from apache_beam.portability.api import beam_expansion_api_pb2_grpc
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api.external_transforms_pb2 import BuilderMethod
from apache_beam.portability.api.external_transforms_pb2 import ExternalConfigurationPayload
from apache_beam.portability.api.external_transforms_pb2 import JavaClassLookupPayload
from apache_beam.runners import pipeline_context
from apache_beam.runners.portability import artifact_service
from apache_beam.transforms import ptransform
from apache_beam.typehints import native_type_compatibility
from apache_beam.typehints import row_type
from apache_beam.typehints.schemas import named_fields_to_schema
from apache_beam.typehints.schemas import named_tuple_from_schema
from apache_beam.typehints.schemas import named_tuple_to_schema
from apache_beam.typehints.trivial_inference import instance_to_type
from apache_beam.typehints.typehints import Union
from apache_beam.typehints.typehints import UnionConstraint
from apache_beam.utils import subprocess_server

DEFAULT_EXPANSION_SERVICE = 'localhost:8097'


def convert_to_typing_type(type_):
  if isinstance(type_, row_type.RowTypeConstraint):
    return named_tuple_from_schema(named_fields_to_schema(type_._fields))
  else:
    return native_type_compatibility.convert_to_typing_type(type_)


def _is_optional_or_none(typehint):
  return (
      type(None) in typehint.union_types if isinstance(
          typehint, UnionConstraint) else typehint is type(None))


def _strip_optional(typehint):
  if not _is_optional_or_none(typehint):
    return typehint
  new_types = typehint.union_types.difference({type(None)})
  if len(new_types) == 1:
    return list(new_types)[0]
  return Union[new_types]


def iter_urns(coder, context=None):
  yield coder.to_runner_api_parameter(context)[0]
  for child in coder._get_component_coders():
    for urn in iter_urns(child, context):
      yield urn


class PayloadBuilder(object):
  """
  Abstract base class for building payloads to pass to ExternalTransform.
  """
  def build(self):
    """
    :return: ExternalConfigurationPayload
    """
    raise NotImplementedError

  def payload(self):
    """
    The serialized ExternalConfigurationPayload

    :return: bytes
    """
    return self.build().SerializeToString()


class SchemaBasedPayloadBuilder(PayloadBuilder):
  """
  Base class for building payloads based on a schema that provides
  type information for each configuration value to encode.
  """
  def _get_named_tuple_instance(self):
    raise NotImplementedError()

  def build(self):
    row = self._get_named_tuple_instance()
    schema = named_tuple_to_schema(type(row))
    return ExternalConfigurationPayload(
        schema=schema, payload=RowCoder(schema).encode(row))


class ImplicitSchemaPayloadBuilder(SchemaBasedPayloadBuilder):
  """
  Build a payload that generates a schema from the provided values.
  """
  def __init__(self, values):
    self._values = values

  def _get_named_tuple_instance(self):
    # omit fields with value=None since we can't infer their type
    values = {
        key: value
        for key, value in self._values.items() if value is not None
    }

    schema = named_fields_to_schema([
        (key, convert_to_typing_type(instance_to_type(value))) for key,
        value in values.items()
    ])
    return named_tuple_from_schema(schema)(**values)


class NamedTupleBasedPayloadBuilder(SchemaBasedPayloadBuilder):
  """
  Build a payload based on a NamedTuple schema.
  """
  def __init__(self, tuple_instance):
    """
    :param tuple_instance: an instance of a typing.NamedTuple
    """
    super().__init__()
    self._tuple_instance = tuple_instance

  def _get_named_tuple_instance(self):
    return self._tuple_instance


class JavaClassLookupPayloadBuilder(PayloadBuilder):
  """
  Builds a payload for directly instantiating a Java transform using a
  constructor and builder methods.
  """

  IGNORED_ARG_FORMAT = 'ignore%d'

  def __init__(self, class_name):
    """
    :param class_name: fully qualified name of the transform class.
    """
    if not class_name:
      raise ValueError('Class name must not be empty')

    self._class_name = class_name
    self._constructor_method = None
    self._constructor_param_args = None
    self._constructor_param_kwargs = None
    self._builder_methods_and_params = OrderedDict()

  def _get_schema_proto_and_payload(self, *args, **kwargs):
    named_fields = []
    fields_to_values = OrderedDict()
    next_field_id = 0
    for value in args:
      if value is None:
        raise ValueError(
            'Received value None. None values are currently not supported')
      named_fields.append(
          ((JavaClassLookupPayloadBuilder.IGNORED_ARG_FORMAT % next_field_id),
           convert_to_typing_type(instance_to_type(value))))
      fields_to_values[(
          JavaClassLookupPayloadBuilder.IGNORED_ARG_FORMAT %
          next_field_id)] = value
      next_field_id += 1
    for key, value in kwargs.items():
      if not key:
        raise ValueError('Parameter name cannot be empty')
      if value is None:
        raise ValueError(
            'Received value None for key %s. None values are currently not '
            'supported' % key)
      named_fields.append(
          (key, convert_to_typing_type(instance_to_type(value))))
      fields_to_values[key] = value

    schema_proto = named_fields_to_schema(named_fields)
    row = named_tuple_from_schema(schema_proto)(**fields_to_values)
    schema = named_tuple_to_schema(type(row))

    payload = RowCoder(schema).encode(row)
    return (schema_proto, payload)

  def build(self):
    constructor_param_args = self._constructor_param_args or []
    constructor_param_kwargs = self._constructor_param_kwargs or {}
    constructor_schema, constructor_payload = (
        self._get_schema_proto_and_payload(
            *constructor_param_args, **constructor_param_kwargs))
    payload = JavaClassLookupPayload(
        class_name=self._class_name,
        constructor_schema=constructor_schema,
        constructor_payload=constructor_payload)
    if self._constructor_method:
      payload.constructor_method = self._constructor_method

    for builder_method_name, params in self._builder_methods_and_params.items():
      builder_method_args, builder_method_kwargs = params
      builder_method_schema, builder_method_payload = (
          self._get_schema_proto_and_payload(
              *builder_method_args, **builder_method_kwargs))
      builder_method = BuilderMethod(
          name=builder_method_name,
          schema=builder_method_schema,
          payload=builder_method_payload)
      builder_method.name = builder_method_name
      payload.builder_methods.append(builder_method)
    return payload

  def with_constructor(self, *args, **kwargs):
    """
    Specifies the Java constructor to use.
    Arguments provided using args and kwargs will be applied to the Java
    transform constructor in the specified order.

    :param args: parameter values of the constructor.
    :param kwargs: parameter names and values of the constructor.
    """
    if self._has_constructor():
      raise ValueError(
          'Constructor or constructor method can only be specified once')

    self._constructor_param_args = args
    self._constructor_param_kwargs = kwargs

  def with_constructor_method(self, method_name, *args, **kwargs):
    """
    Specifies the Java constructor method to use.
    Arguments provided using args and kwargs will be applied to the Java
    transform constructor method in the specified order.

    :param method_name: name of the constructor method.
    :param args: parameter values of the constructor method.
    :param kwargs: parameter names and values of the constructor method.
    """
    if self._has_constructor():
      raise ValueError(
          'Constructor or constructor method can only be specified once')

    self._constructor_method = method_name
    self._constructor_param_args = args
    self._constructor_param_kwargs = kwargs

  def add_builder_method(self, method_name, *args, **kwargs):
    """
    Specifies a Java builder method to be invoked after instantiating the Java
    transform class. Specified builder method will be applied in order.
    Arguments provided using args and kwargs will be applied to the Java
    transform builder method in the specified order.

    :param method_name: name of the builder method.
    :param args: parameter values of the builder method.
    :param kwargs:  parameter names and values of the builder method.
    """
    self._builder_methods_and_params[method_name] = (args, kwargs)

  def _has_constructor(self):
    return (
        self._constructor_method or self._constructor_param_args or
        self._constructor_param_kwargs)


class JavaExternalTransform(ptransform.PTransform):
  """A proxy for Java-implemented external transforms.

  One builds these transforms just as one would in Java, e.g.::

      transform = JavaExternalTransform('fully.qualified.ClassName'
          )(contructorArg, ... ).builderMethod(...)

  or::

      JavaExternalTransform('fully.qualified.ClassName').staticConstructor(
          ...).builderMethod1(...).builderMethod2(...)

  :param class_name: fully qualified name of the java class
  :param expansion_service: (Optional) an expansion service to use.  If none is
      provided, a default expansion service will be started.
  :param classpath: (Optional) A list paths to additional jars to place on the
      expansion service classpath.
  """
  def __init__(self, class_name, expansion_service=None, classpath=None):
    if expansion_service and classpath:
      raise ValueError(
          f'Only one of expansion_service ({expansion_service}) '
          f'or classpath ({classpath}) may be provided.')
    self._payload_builder = JavaClassLookupPayloadBuilder(class_name)
    self._classpath = classpath
    self._expansion_service = expansion_service
    # Beam explicitly looks for following attributes. Hence adding
    # 'None' values here to prevent '__getattr__' from being called.
    self.inputs = None
    self._fn_api_payload = None

  def __call__(self, *args, **kwargs):
    self._payload_builder.with_constructor(*args, **kwargs)
    return self

  def __getattr__(self, name):
    # Don't try to emulate special methods.
    if name.startswith('__') and name.endswith('__'):
      return super().__getattr__(name)
    else:
      return self[name]

  def __getitem__(self, name):
    # Use directly for keywords or attribute conflicts.
    def construct(*args, **kwargs):
      if self._payload_builder._has_constructor():
        builder_method = self._payload_builder.add_builder_method
      else:
        builder_method = self._payload_builder.with_constructor_method
      builder_method(name, *args, **kwargs)
      return self

    return construct

  def expand(self, pcolls):
    if self._expansion_service is None:
      self._expansion_service = BeamJarExpansionService(
          ':sdks:java:expansion-service:app:shadowJar',
          extra_args=['{{PORT}}', '--javaClassLookupAllowlistFile=*'],
          classpath=self._classpath)
    return pcolls | ExternalTransform(
        common_urns.java_class_lookup.urn,
        self._payload_builder,
        self._expansion_service)


class AnnotationBasedPayloadBuilder(SchemaBasedPayloadBuilder):
  """
  Build a payload based on an external transform's type annotations.
  """
  def __init__(self, transform, **values):
    """
    :param transform: a PTransform instance or class. type annotations will
                      be gathered from its __init__ method
    :param values: values to encode
    """
    self._transform = transform
    self._values = values

  def _get_named_tuple_instance(self):
    schema = named_fields_to_schema([
        (k, convert_to_typing_type(v)) for k,
        v in self._transform.__init__.__annotations__.items()
        if k in self._values
    ])
    return named_tuple_from_schema(schema)(**self._values)


class DataclassBasedPayloadBuilder(SchemaBasedPayloadBuilder):
  """
  Build a payload based on an external transform that uses dataclasses.
  """
  def __init__(self, transform):
    """
    :param transform: a dataclass-decorated PTransform instance from which to
                      gather type annotations and values
    """
    self._transform = transform

  def _get_named_tuple_instance(self):
    import dataclasses
    schema = named_fields_to_schema([
        (field.name, convert_to_typing_type(field.type))
        for field in dataclasses.fields(self._transform)
    ])
    return named_tuple_from_schema(schema)(
        **dataclasses.asdict(self._transform))


class ExternalTransform(ptransform.PTransform):
  """
    External provides a cross-language transform via expansion services in
    foreign SDKs.

    Experimental; no backwards compatibility guarantees.
  """
  _namespace_counter = 0

  # Variable name _namespace conflicts with DisplayData._namespace so we use
  # name _external_namespace here.
  _external_namespace = threading.local()

  _IMPULSE_PREFIX = 'impulse'

  def __init__(self, urn, payload, expansion_service=None):
    """Wrapper for an external transform with the given urn and payload.

    :param urn: the unique beam identifier for this transform
    :param payload: the payload, either as a byte string or a PayloadBuilder
    :param expansion_service: an expansion service implementing the beam
        ExpansionService protocol, either as an object with an Expand method
        or an address (as a str) to a grpc server that provides this method.
    """
    expansion_service = expansion_service or DEFAULT_EXPANSION_SERVICE
    if not urn and isinstance(payload, JavaClassLookupPayloadBuilder):
      urn = common_urns.java_class_lookup.urn
    self._urn = urn
    self._payload = (
        payload.payload() if isinstance(payload, PayloadBuilder) else payload)
    self._expansion_service = expansion_service
    self._external_namespace = self._fresh_namespace()
    self._inputs = {}  # type: Dict[str, pvalue.PCollection]
    self._outputs = {}  # type: Dict[str, pvalue.PCollection]

  def replace_named_inputs(self, named_inputs):
    self._inputs = named_inputs

  def replace_named_outputs(self, named_outputs):
    self._outputs = named_outputs

  def __post_init__(self, expansion_service):
    """
    This will only be invoked if ExternalTransform is used as a base class
    for a class decorated with dataclasses.dataclass
    """
    ExternalTransform.__init__(
        self, self.URN, DataclassBasedPayloadBuilder(self), expansion_service)

  def default_label(self):
    return '%s(%s)' % (self.__class__.__name__, self._urn)

  @classmethod
  def get_local_namespace(cls):
    return getattr(cls._external_namespace, 'value', 'external')

  @classmethod
  @contextlib.contextmanager
  def outer_namespace(cls, namespace):
    prev = cls.get_local_namespace()
    cls._external_namespace.value = namespace
    yield
    cls._external_namespace.value = prev

  @classmethod
  def _fresh_namespace(cls):
    # type: () -> str
    ExternalTransform._namespace_counter += 1
    return '%s_%d' % (cls.get_local_namespace(), cls._namespace_counter)

  def expand(self, pvalueish):
    # type: (pvalue.PCollection) -> pvalue.PCollection
    if isinstance(pvalueish, pvalue.PBegin):
      self._inputs = {}
    elif isinstance(pvalueish, (list, tuple)):
      self._inputs = {str(ix): pvalue for ix, pvalue in enumerate(pvalueish)}
    elif isinstance(pvalueish, dict):
      self._inputs = pvalueish
    else:
      self._inputs = {'input': pvalueish}
    pipeline = (
        next(iter(self._inputs.values())).pipeline
        if self._inputs else pvalueish.pipeline)
    context = pipeline_context.PipelineContext(
        component_id_map=pipeline.component_id_map)
    transform_proto = beam_runner_api_pb2.PTransform(
        unique_name=pipeline._current_transform().full_label,
        spec=beam_runner_api_pb2.FunctionSpec(
            urn=self._urn, payload=self._payload))
    for tag, pcoll in self._inputs.items():
      transform_proto.inputs[tag] = context.pcollections.get_id(pcoll)
      # Conversion to/from proto assumes producers.
      # TODO: Possibly loosen this.
      context.transforms.put_proto(
          '%s_%s' % (self._IMPULSE_PREFIX, tag),
          beam_runner_api_pb2.PTransform(
              unique_name='%s_%s' % (self._IMPULSE_PREFIX, tag),
              spec=beam_runner_api_pb2.FunctionSpec(
                  urn=common_urns.primitives.IMPULSE.urn),
              outputs={'out': transform_proto.inputs[tag]}))
    components = context.to_runner_api()
    request = beam_expansion_api_pb2.ExpansionRequest(
        components=components,
        namespace=self._external_namespace,  # type: ignore  # mypy thinks self._namespace is threading.local
        transform=transform_proto)

    with self._service() as service:
      response = service.Expand(request)
      if response.error:
        raise RuntimeError(response.error)
      self._expanded_components = response.components
      if any(env.dependencies
             for env in self._expanded_components.environments.values()):
        self._expanded_components = self._resolve_artifacts(
            self._expanded_components,
            service.artifact_service(),
            pipeline.local_tempdir)

    self._expanded_transform = response.transform
    self._expanded_requirements = response.requirements
    result_context = pipeline_context.PipelineContext(response.components)

    def fix_output(pcoll, tag):
      pcoll.pipeline = pipeline
      pcoll.tag = tag
      return pcoll

    self._outputs = {
        tag: fix_output(result_context.pcollections.get_by_id(pcoll_id), tag)
        for tag,
        pcoll_id in self._expanded_transform.outputs.items()
    }

    return self._output_to_pvalueish(self._outputs)

  @contextlib.contextmanager
  def _service(self):
    if isinstance(self._expansion_service, str):
      channel_options = [("grpc.max_receive_message_length", -1),
                         ("grpc.max_send_message_length", -1)]
      if hasattr(grpc, 'local_channel_credentials'):
        # Some environments may not support insecure channels. Hence use a
        # secure channel with local credentials here.
        # TODO: update this to support secure non-local channels.
        channel_factory_fn = functools.partial(
            grpc.secure_channel,
            self._expansion_service,
            grpc.local_channel_credentials(),
            options=channel_options)
      else:
        # local_channel_credentials is an experimental API which is unsupported
        # by older versions of grpc which may be pulled in due to other project
        # dependencies.
        channel_factory_fn = functools.partial(
            grpc.insecure_channel,
            self._expansion_service,
            options=channel_options)
      with channel_factory_fn() as channel:
        yield ExpansionAndArtifactRetrievalStub(channel)
    elif hasattr(self._expansion_service, 'Expand'):
      yield self._expansion_service
    else:
      with self._expansion_service as stub:
        yield stub

  def _resolve_artifacts(self, components, service, dest):
    for env in components.environments.values():
      if env.dependencies:
        resolved = list(
            artifact_service.resolve_artifacts(env.dependencies, service, dest))
        del env.dependencies[:]
        env.dependencies.extend(resolved)
    return components

  def _output_to_pvalueish(self, output_dict):
    if len(output_dict) == 1:
      return next(iter(output_dict.values()))
    else:
      return output_dict

  def to_runner_api_transform(self, context, full_label):
    pcoll_renames = {}
    renamed_tag_seen = False
    for tag, pcoll in self._inputs.items():
      if tag not in self._expanded_transform.inputs:
        if renamed_tag_seen:
          raise RuntimeError(
              'Ambiguity due to non-preserved tags: %s vs %s' % (
                  sorted(self._expanded_transform.inputs.keys()),
                  sorted(self._inputs.keys())))
        else:
          renamed_tag_seen = True
          tag, = self._expanded_transform.inputs.keys()
      pcoll_renames[self._expanded_transform.inputs[tag]] = (
          context.pcollections.get_id(pcoll))
    for tag, pcoll in self._outputs.items():
      pcoll_renames[self._expanded_transform.outputs[tag]] = (
          context.pcollections.get_id(pcoll))

    def _equivalent(coder1, coder2):
      return coder1 == coder2 or _normalize(coder1) == _normalize(coder2)

    def _normalize(coder_proto):
      normalized = copy.copy(coder_proto)
      normalized.spec.environment_id = ''
      # TODO(robertwb): Normalize components as well.
      return normalized

    for id, proto in self._expanded_components.coders.items():
      if id.startswith(self._external_namespace):
        context.coders.put_proto(id, proto)
      elif id in context.coders:
        if not _equivalent(context.coders._id_to_proto[id], proto):
          raise RuntimeError(
              'Re-used coder id: %s\n%s\n%s' %
              (id, context.coders._id_to_proto[id], proto))
      else:
        context.coders.put_proto(id, proto)
    for id, proto in self._expanded_components.windowing_strategies.items():
      if id.startswith(self._external_namespace):
        context.windowing_strategies.put_proto(id, proto)
    for id, proto in self._expanded_components.environments.items():
      if id.startswith(self._external_namespace):
        context.environments.put_proto(id, proto)
    for id, proto in self._expanded_components.pcollections.items():
      id = pcoll_renames.get(id, id)
      if id not in context.pcollections._id_to_obj.keys():
        context.pcollections.put_proto(id, proto)

    for id, proto in self._expanded_components.transforms.items():
      if id.startswith(self._IMPULSE_PREFIX):
        # Our fake inputs.
        continue
      assert id.startswith(
          self._external_namespace), (id, self._external_namespace)
      new_proto = beam_runner_api_pb2.PTransform(
          unique_name=proto.unique_name,
          # If URN is not set this is an empty spec.
          spec=proto.spec if proto.spec.urn else None,
          subtransforms=proto.subtransforms,
          inputs={
              tag: pcoll_renames.get(pcoll, pcoll)
              for tag,
              pcoll in proto.inputs.items()
          },
          outputs={
              tag: pcoll_renames.get(pcoll, pcoll)
              for tag,
              pcoll in proto.outputs.items()
          },
          environment_id=proto.environment_id)
      context.transforms.put_proto(id, new_proto)

    for requirement in self._expanded_requirements:
      context.add_requirement(requirement)

    return beam_runner_api_pb2.PTransform(
        unique_name=full_label,
        spec=self._expanded_transform.spec,
        subtransforms=self._expanded_transform.subtransforms,
        inputs={
            tag: pcoll_renames.get(pcoll, pcoll)
            for tag,
            pcoll in self._expanded_transform.inputs.items()
        },
        outputs={
            tag: pcoll_renames.get(pcoll, pcoll)
            for tag,
            pcoll in self._expanded_transform.outputs.items()
        },
        environment_id=self._expanded_transform.environment_id)


class ExpansionAndArtifactRetrievalStub(
    beam_expansion_api_pb2_grpc.ExpansionServiceStub):
  def __init__(self, channel, **kwargs):
    self._channel = channel
    self._kwargs = kwargs
    super().__init__(channel, **kwargs)

  def artifact_service(self):
    return beam_artifact_api_pb2_grpc.ArtifactRetrievalServiceStub(
        self._channel, **self._kwargs)


class JavaJarExpansionService(object):
  """An expansion service based on an Java Jar file.

  This can be passed into an ExternalTransform as the expansion_service
  argument which will spawn a subprocess using this jar to expand the
  transform.
  """
  def __init__(self, path_to_jar, extra_args=None, classpath=None):
    self._path_to_jar = path_to_jar
    self._extra_args = extra_args
    self._classpath = classpath or []
    self._service_count = 0

  @staticmethod
  def _expand_jars(jar):
    if glob.glob(jar):
      return glob.glob(jar)
    elif isinstance(jar, str) and (jar.startswith('http://') or
                                   jar.startswith('https://')):
      return [jar]
    else:
      # If the input JAR is not a local glob, nor an http/https URL, then
      # we assume that it's a gradle-style Java artifact in Maven Central,
      # in the form group:artifact:version, so we attempt to parse that way.
      try:
        group_id, artifact_id, version = jar.split(':')
      except ValueError:
        # If we are not able to find a JAR, nor a JAR artifact, nor a URL for
        # a JAR path, we still choose to include it in the path.
        logging.warning('Unable to parse %s into group:artifact:version.', jar)
        return [jar]
      path = subprocess_server.JavaJarServer.path_to_maven_jar(
          artifact_id, group_id, version)
      return [path]

  def _default_args(self):
    to_stage = ','.join([self._path_to_jar] + sum((
        JavaJarExpansionService._expand_jars(jar)
        for jar in self._classpath or []), []))
    return ['{{PORT}}', f'--filesToStage={to_stage}']

  def __enter__(self):
    if self._service_count == 0:
      self._path_to_jar = subprocess_server.JavaJarServer.local_jar(
          self._path_to_jar)
      if self._extra_args is None:
        self._extra_args = self._default_args()
      # Consider memoizing these servers (with some timeout).
      logging.info(
          'Starting a JAR-based expansion service from JAR %s ' + (
              'and with classpath: %s' %
              self._classpath if self._classpath else ''),
          self._path_to_jar)
      classpath_urls = [
          subprocess_server.JavaJarServer.local_jar(path)
          for jar in self._classpath
          for path in JavaJarExpansionService._expand_jars(jar)
      ]
      self._service_provider = subprocess_server.JavaJarServer(
          ExpansionAndArtifactRetrievalStub,
          self._path_to_jar,
          self._extra_args,
          classpath=classpath_urls)
      self._service = self._service_provider.__enter__()
    self._service_count += 1
    return self._service

  def __exit__(self, *args):
    self._service_count -= 1
    if self._service_count == 0:
      self._service_provider.__exit__(*args)


class BeamJarExpansionService(JavaJarExpansionService):
  """An expansion service based on an Beam Java Jar file.

  Attempts to use a locally-built copy of the jar based on the gradle target,
  if it exists, otherwise attempts to download and cache the released artifact
  corresponding to this version of Beam from the apache maven repository.
  """
  def __init__(
      self,
      gradle_target,
      extra_args=None,
      gradle_appendix=None,
      classpath=None):
    path_to_jar = subprocess_server.JavaJarServer.path_to_beam_jar(
        gradle_target, gradle_appendix)
    super().__init__(path_to_jar, extra_args, classpath=classpath)


def memoize(func):
  cache = {}

  def wrapper(*args):
    if args not in cache:
      cache[args] = func(*args)
    return cache[args]

  return wrapper
