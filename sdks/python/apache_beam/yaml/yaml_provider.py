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

"""This module defines Providers usable from yaml, which is a specification
for where to find and how to invoke services that vend implementations of
various PTransforms."""

import abc
import collections
import functools
import hashlib
import inspect
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
import warnings
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from typing import Any
from typing import Optional
from typing import Union

import docstring_parser
import yaml

import apache_beam as beam
import apache_beam.dataframe.io
import apache_beam.io
import apache_beam.transforms.util
from apache_beam import ManagedReplacement
from apache_beam.io.filesystems import FileSystems
from apache_beam.portability.api import schema_pb2
from apache_beam.runners import pipeline_context
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import external
from apache_beam.transforms import window
from apache_beam.transforms.fully_qualified_named_transform import FullyQualifiedNamedTransform
from apache_beam.typehints import schemas
from apache_beam.typehints import trivial_inference
from apache_beam.typehints.schemas import named_tuple_to_schema
from apache_beam.typehints.schemas import typing_to_runner_api
from apache_beam.utils import python_callable
from apache_beam.utils import subprocess_server
from apache_beam.version import __version__ as beam_version
from apache_beam.yaml import json_utils
from apache_beam.yaml import yaml_utils
from apache_beam.yaml.yaml_errors import maybe_with_exception_handling_transform_fn

_LOGGER = logging.getLogger(__name__)


class NotAvailableWithReason:
  """A False value that provides additional content.

  Primarily used to return a value from Provider.available().
  """
  def __init__(self, reason):
    self.reason = reason

  def __bool__(self):
    return False


class Provider(abc.ABC):
  """Maps transform types names and args to concrete PTransform instances."""
  @abc.abstractmethod
  def available(self) -> Union[bool, NotAvailableWithReason]:
    """Returns whether this provider is available to use in this environment."""
    raise NotImplementedError(type(self))

  @abc.abstractmethod
  def cache_artifacts(self) -> Optional[Iterable[str]]:
    raise NotImplementedError(type(self))

  @abc.abstractmethod
  def provided_transforms(self) -> Iterable[str]:
    """Returns a list of transform type names this provider can handle."""
    raise NotImplementedError(type(self))

  def config_schema(self, type):
    return None

  def description(self, type):
    return None

  def requires_inputs(self, typ: str, args: Mapping[str, Any]) -> bool:
    """Returns whether this transform requires inputs.

    Specifically, if this returns True and inputs are not provided than an error
    will be thrown.

    This is best-effort, primarily for better and earlier error messages.
    """
    return not typ.startswith('Read')

  @abc.abstractmethod
  def create_transform(
      self,
      typ: str,
      args: Mapping[str, Any],
      yaml_create_transform: Callable[
          [Mapping[str, Any], Iterable[beam.PCollection]], beam.PTransform]
  ) -> beam.PTransform:
    """Creates a PTransform instance for the given transform type and arguments.
    """
    raise NotImplementedError(type(self))

  def underlying_provider(self):
    """If this provider is simply a proxy to another provider, return the
    provider that should actually be used for affinity checking.
    """
    return self

  def affinity(self, other: "Provider"):
    """Returns a value approximating how good it would be for this provider
    to be used immediately following a transform from the other provider
    (e.g. to encourage fusion).
    """
    # TODO(yaml): This is a very rough heuristic. Consider doing better.
    # E.g. we could look at the expected environments themselves.
    # Possibly, we could provide multiple expansions and have the runner itself
    # choose the actual implementation based on fusion (and other) criteria.
    a = self.underlying_provider()
    b = other.underlying_provider()
    return a._affinity(b) + b._affinity(a)

  def _affinity(self, other: "Provider"):
    if self is other or self == other:
      return 100
    elif type(self) == type(other):
      return 10
    else:
      return 0

  @functools.cache  # pylint: disable=method-cache-max-size-none
  def with_extra_dependencies(self, dependencies: Iterable[str]):
    result = self._with_extra_dependencies(dependencies)
    if not hasattr(result, 'to_json'):
      result.to_json = lambda: {'type': type(result).__name__}
    return result

  def _with_extra_dependencies(self, dependencies: Iterable[str]):
    raise ValueError(
        'This provider of type %s does not support additional dependencies.' %
        type(self).__name__)


def as_provider(name, provider_or_constructor):
  if isinstance(provider_or_constructor, Provider):
    return provider_or_constructor
  else:
    return InlineProvider({name: provider_or_constructor})


def as_provider_list(name, lst):
  if not isinstance(lst, list):
    return as_provider_list(name, [lst])
  return [as_provider(name, x) for x in lst]


class ExternalProvider(Provider):
  """A Provider implemented via the cross language transform service."""
  _provider_types: dict[str, Callable[..., Provider]] = {}

  def __init__(self, urns, service, managed_replacement=None):
    """Initializes the ExternalProvider.

    Args:
      urns: a set of URNs that uniquely identify the transforms supported.
      service: the gradle target that identified the expansion service jar.
      managed_replacement (Optional): a map that defines the transform for
        which the SDK may replace the transform with an available managed
        transform.
    """
    self._urns = urns
    self._service = service
    self._schema_transforms = None
    self._managed_replacement = managed_replacement

  def provided_transforms(self):
    return self._urns.keys()

  def schema_transforms(self):
    if callable(self._service):
      self._service = self._service()
    if self._schema_transforms is None:
      try:
        self._schema_transforms = {
            config.identifier: config
            for config in external.SchemaAwareExternalTransform.discover(
                self._service, ignore_errors=True)
        }
      except Exception:
        # It's possible this service doesn't vend schema transforms.
        self._schema_transforms = {}
    return self._schema_transforms

  def config_schema(self, type):
    if self._urns[type] in self.schema_transforms():
      return named_tuple_to_schema(
          self.schema_transforms()[self._urns[type]].configuration_schema)

  def description(self, type):
    if self._urns[type] in self.schema_transforms():
      return self.schema_transforms()[self._urns[type]].description

  def requires_inputs(self, typ, args):
    if self._urns[typ] in self.schema_transforms():
      return bool(self.schema_transforms()[self._urns[typ]].inputs)
    else:
      return super().requires_inputs(typ, args)

  def create_transform(self, type, args, yaml_create_transform):
    if callable(self._service):
      self._service = self._service()
    urn = self._urns[type]
    if urn in self.schema_transforms():
      managed_replacement = None
      if self._managed_replacement and type in self._managed_replacement:
        managed_replacement = ManagedReplacement(
            underlying_transform_identifier=urn,
            update_compatibility_version=self._managed_replacement[type])

      return external.SchemaAwareExternalTransform(
          urn,
          self._service,
          rearrange_based_on_discovery=True,
          managed_replacement=managed_replacement,
          **args)
    else:
      return type >> self.create_external_transform(urn, args)

  def create_external_transform(self, urn, args):
    return external.ExternalTransform(
        urn,
        external.ImplicitSchemaPayloadBuilder(args).payload(),
        self._service)

  @classmethod
  def provider_from_spec(cls, source_path, spec):
    from apache_beam.yaml.yaml_transform import SafeLineLoader
    for required in ('type', 'transforms'):
      if required not in spec:
        raise ValueError(
            f'Missing {required} in provider '
            f'at line {SafeLineLoader.get_line(spec)}')
    urns = SafeLineLoader.strip_metadata(spec['transforms'])
    type = spec['type']
    config = SafeLineLoader.strip_metadata(spec.get('config', {}))
    extra_params = set(SafeLineLoader.strip_metadata(spec).keys()) - {
        'transforms', 'type', 'config'
    }
    if extra_params:
      raise ValueError(
          f'Unexpected parameters in provider of type {type} '
          f'at line {SafeLineLoader.get_line(spec)}: {extra_params}')
    if config.get('version', None) == 'BEAM_VERSION':
      config['version'] = beam_version
    if type in cls._provider_types:
      try:
        constructor = cls._provider_types[type]
        if 'provider_base_path' in inspect.signature(constructor).parameters:
          config['provider_base_path'] = source_path
        result = constructor(urns, **config)
        if not hasattr(result, 'to_json'):
          result.to_json = lambda: spec
        return result
      except Exception as exn:
        raise ValueError(
            f'Unable to instantiate provider of type {type} '
            f'at line {SafeLineLoader.get_line(spec)}: {exn}') from exn
    else:
      raise NotImplementedError(
          f'Unknown provider type: {type} '
          f'at line {SafeLineLoader.get_line(spec)}.')

  @classmethod
  def register_provider_type(cls, type_name):
    def apply(constructor):
      cls._provider_types[type_name] = constructor
      return constructor

    return apply


@ExternalProvider.register_provider_type('javaJar')
def java_jar(urns, provider_base_path, jar: str):
  if not os.path.exists(jar):
    parsed = urllib.parse.urlparse(jar)
    if not parsed.scheme or not parsed.netloc:
      raise ValueError(f'Invalid path or url: {jar}')
  return ExternalJavaProvider(
      urns, lambda: _join_url_or_filepath(provider_base_path, jar))


@ExternalProvider.register_provider_type('mavenJar')
def maven_jar(
    urns,
    *,
    artifact_id,
    group_id,
    version,
    repository=subprocess_server.JavaJarServer.MAVEN_CENTRAL_REPOSITORY,
    classifier=None,
    appendix=None):
  return ExternalJavaProvider(
      urns, lambda: subprocess_server.JavaJarServer.path_to_maven_jar(
          artifact_id=artifact_id, group_id=group_id, version=version,
          repository=repository, classifier=classifier, appendix=appendix))


@ExternalProvider.register_provider_type('beamJar')
def beam_jar(
    urns,
    *,
    gradle_target,
    managed_replacement=None,
    appendix=None,
    version=beam_version,
    artifact_id=None):
  return ExternalJavaProvider(
      urns, lambda: subprocess_server.JavaJarServer.path_to_beam_jar(
          gradle_target=gradle_target, version=version, artifact_id=artifact_id
      ),
      managed_replacement=managed_replacement)


@ExternalProvider.register_provider_type('docker')
def docker(urns, **config):
  raise NotImplementedError()


@ExternalProvider.register_provider_type('remote')
class RemoteProvider(ExternalProvider):
  _is_available = None

  def __init__(self, urns, address: str):
    super().__init__(urns, service=address)
    self._address = address

  def available(self):
    if self._is_available is None:
      try:
        with external.ExternalTransform.service(self._service) as service:
          service.ready(1)
          self._is_available = True
      except Exception:
        self._is_available = NotAvailableWithReason(
            f'Remote provider not reachable at {self._address}.')
    return self._is_available

  def cache_artifacts(self):
    pass


class ExternalJavaProvider(ExternalProvider):
  def __init__(
      self, urns, jar_provider, managed_replacement=None, classpath=None):
    super().__init__(
        urns, lambda: external.JavaJarExpansionService(
            jar_provider(), classpath=classpath),
        managed_replacement)
    self._jar_provider = jar_provider
    self._classpath = classpath

  def available(self):
    # Directly use shutil.which to find the Java executable cross-platform
    java_path = shutil.which(subprocess_server.JavaHelper.get_java())
    if java_path:
      return True
    # Return error message when not found
    return NotAvailableWithReason(
        'Unable to locate java executable: java not found in PATH or JAVA_HOME')

  def cache_artifacts(self):
    return [self._jar_provider()]

  def _with_extra_dependencies(self, dependencies: Iterable[str]):
    jars = sum((
        external.JavaJarExpansionService._expand_jars(dep)
        for dep in dependencies), [])
    return ExternalJavaProvider(
        self._urns,
        jar_provider=self._jar_provider,
        classpath=(list(self._classpath or []) + list(jars)))


@ExternalProvider.register_provider_type('python')
def python(urns, provider_base_path, packages=()):
  if packages:
    return ExternalPythonProvider(urns, provider_base_path, packages)
  else:
    return InlineProvider({
        name: python_callable.PythonCallableWithSource.load_from_source(
            constructor)
        for (name, constructor) in urns.items()
    })


@ExternalProvider.register_provider_type('pythonPackage')
class ExternalPythonProvider(ExternalProvider):
  def __init__(self, urns, provider_base_path, packages: Iterable[str]):
    def is_path_or_urn(package):
      return (
          '/' in package or urllib.parse.urlparse(package).scheme or
          os.path.exists(package))

    super().__init__(
        urns,
        PypiExpansionService([
            _join_url_or_filepath(provider_base_path, package)
            if is_path_or_urn(package) else package for package in packages
        ]))

    self._packages = packages

  def available(self):
    return True  # If we're running this script, we have Python installed.

  def cache_artifacts(self):
    return [self._service._venv()]

  def create_external_transform(self, urn, args):
    # Python transforms are "registered" by fully qualified name.
    if not re.match(r'^[\w.]*$', urn):
      # Treat it as source.
      args = {'source': urn, **args}
      urn = '__constructor__'
    return external.ExternalTransform(
        "beam:transforms:python:fully_qualified_named",
        external.ImplicitSchemaPayloadBuilder({
            'constructor': urn,
            'kwargs': args,
        }).payload(),
        self._service)

  def _affinity(self, other: "Provider"):
    if isinstance(other, InlineProvider):
      return 50
    else:
      return super()._affinity(other)

  def _with_extra_dependencies(self, dependencies: Iterable[str]):
    return ExternalPythonProvider(
        self._urns, None, set(self._packages).union(set(dependencies)))


@ExternalProvider.register_provider_type('yaml')
class YamlProvider(Provider):
  def __init__(self, transforms: Mapping[str, Mapping[str, Any]]):
    if not isinstance(transforms, dict):
      raise ValueError('Transform mapping must be a dict.')
    self._transforms = transforms

  def available(self):
    return True

  def cache_artifacts(self):
    pass

  def provided_transforms(self):
    return self._transforms.keys()

  def config_schema(self, type):
    return json_utils.json_schema_to_beam_schema(self.json_config_schema(type))

  def json_config_schema(self, type):
    return dict(
        type='object',
        additionalProperties=False,
        **self._transforms[type]['config_schema'])

  def description(self, type):
    return self._transforms[type].get('description')

  def requires_inputs(self, type, args):
    return self._transforms[type].get(
        'requires_inputs', super().requires_inputs(type, args))

  def create_transform(
      self,
      type: str,
      args: Mapping[str, Any],
      yaml_create_transform: Callable[
          [Mapping[str, Any], Iterable[beam.PCollection]], beam.PTransform]
  ) -> beam.PTransform:
    from apache_beam.yaml.yaml_transform import expand_jinja, preprocess
    from apache_beam.yaml.yaml_transform import SafeLineLoader
    spec = self._transforms[type]
    try:
      import jsonschema
      jsonschema.validate(args, self.json_config_schema(type))
    except ImportError:
      warnings.warn(
          'Please install jsonschema '
          f'for better provider validation of "{type}"')
    body = spec['body']
    # Stringify to apply jinja.
    if isinstance(body, str):
      body_str = body
    else:
      body_str = yaml.safe_dump(SafeLineLoader.strip_metadata(body))
    # Now re-parse resolved templatization.
    body = yaml.load(expand_jinja(body_str, args), Loader=SafeLineLoader)
    if (body.get('type') == 'chain' and 'input' not in body and
        spec.get('requires_inputs', True)):
      body['input'] = 'input'
    return yaml_create_transform(preprocess(body))  # type: ignore


# This is needed because type inference can't handle *args, **kwargs forwarding.
# TODO(BEAM-24755): Add support for type inference of through kwargs calls.
def fix_pycallable():
  from apache_beam.transforms.ptransform import label_from_callable

  def default_label(self):
    src = self._source.strip()
    last_line = src.split('\n')[-1]
    if last_line[0] != ' ' and len(last_line) < 72:
      return last_line
    return label_from_callable(self._callable)

  def _argspec_fn(self):
    return self._callable

  python_callable.PythonCallableWithSource.default_label = default_label
  python_callable.PythonCallableWithSource._argspec_fn = property(_argspec_fn)

  original_infer_return_type = trivial_inference.infer_return_type

  def infer_return_type(fn, *args, **kwargs):
    if isinstance(fn, python_callable.PythonCallableWithSource):
      fn = fn._callable
    return original_infer_return_type(fn, *args, **kwargs)

  trivial_inference.infer_return_type = infer_return_type

  original_fn_takes_side_inputs = (
      apache_beam.transforms.util.fn_takes_side_inputs)

  def fn_takes_side_inputs(fn):
    if isinstance(fn, python_callable.PythonCallableWithSource):
      fn = fn._callable
    return original_fn_takes_side_inputs(fn)

  apache_beam.transforms.util.fn_takes_side_inputs = fn_takes_side_inputs


class InlineProvider(Provider):
  def __init__(self, transform_factories, no_input_transforms=()):
    self._transform_factories = transform_factories
    self._no_input_transforms = set(no_input_transforms)

  def available(self):
    return True

  def cache_artifacts(self):
    pass

  def provided_transforms(self):
    return self._transform_factories.keys()

  def config_schema(self, typ):
    return self.config_schema_from_callable(self._transform_factories[typ])

  @classmethod
  def config_schema_from_callable(cls, factory):
    if isinstance(factory, type) and issubclass(factory, beam.PTransform):
      # https://bugs.python.org/issue40897
      params = dict(inspect.signature(factory.__init__).parameters)
      if 'self' in params:
        del params['self']
    else:
      params = inspect.signature(factory).parameters

    def type_of(p):
      t = p.annotation
      if t == p.empty:
        return Any
      else:
        return t

    docs = {
        param.arg_name: param.description
        for param in cls.get_docs(factory).params
    }

    names_and_types = [(name, typing_to_runner_api(type_of(p)))
                       for name, p in params.items()]
    return schema_pb2.Schema(
        fields=[
            schema_pb2.Field(name=name, type=type, description=docs.get(name))
            for (name, type) in names_and_types
        ])

  def description(self, typ):
    return self.description_from_callable(self._transform_factories[typ])

  @classmethod
  def description_from_callable(cls, factory):
    def empty_if_none(s):
      return s or ''

    docs = cls.get_docs(factory)
    return (
        empty_if_none(docs.short_description) +
        ('\n\n' if docs.blank_after_short_description else '\n') +
        empty_if_none(docs.long_description)).strip() or None

  @classmethod
  def get_docs(cls, factory):
    docstring = factory.__doc__ or ''
    # These "extra" docstring parameters are not relevant for YAML and mess
    # up the parsing.
    docstring = re.sub(
        r'Pandas Parameters\s+-----.*', '', docstring, flags=re.S)
    return docstring_parser.parse(
        docstring, docstring_parser.DocstringStyle.GOOGLE)

  def create_transform(self, type, args, yaml_create_transform):
    return self._transform_factories[type](**args)

  def to_json(self):
    return {'type': "InlineProvider"}

  def requires_inputs(self, typ, args):
    if typ in self._no_input_transforms:
      return False
    elif hasattr(self._transform_factories[typ], '_yaml_requires_inputs'):
      return self._transform_factories[typ]._yaml_requires_inputs
    else:
      return super().requires_inputs(typ, args)

  def _with_extra_dependencies(self, dependencies):
    external_provider = ExternalPythonProvider(  # disable yapf
        {
            typ: 'apache_beam.yaml.yaml_provider.standard_inline_providers.' +
            typ.replace('-', '_')
            for typ in self._transform_factories.keys()
        },
        '__inline__',
        dependencies)
    external_provider.to_json = self.to_json
    return external_provider


class MetaInlineProvider(InlineProvider):
  def create_transform(self, type, args, yaml_create_transform):
    return self._transform_factories[type](yaml_create_transform, **args)


# Note: This function is used to override the default provider by some
# users, so a change here will be breaking to those users. Change with
# caution.
def get_default_sql_provider():
  return beam_jar(
      urns={'Sql': 'beam:external:java:sql:v1'},
      gradle_target='sdks:java:extensions:sql:expansion-service:shadowJar')


class SqlBackedProvider(Provider):
  def __init__(
      self,
      transforms: Mapping[str, Callable[..., beam.PTransform]],
      sql_provider: Optional[Provider] = None):
    self._transforms = transforms
    if sql_provider is None:
      sql_provider = get_default_sql_provider()
    self._sql_provider = sql_provider

  def sql_provider(self):
    return self._sql_provider

  def provided_transforms(self):
    return self._transforms.keys()

  def config_schema(self, type):
    full_config = InlineProvider.config_schema_from_callable(
        self._transforms[type])
    # Omit the (first) query -> transform parameter.
    return schema_pb2.Schema(fields=full_config.fields[1:])

  def description(self, type):
    return InlineProvider.description_from_callable(self._transforms[type])

  def available(self):
    return self.sql_provider().available()

  def cache_artifacts(self):
    return self.sql_provider().cache_artifacts()

  def underlying_provider(self):
    return self.sql_provider()

  def to_json(self):
    return {'type': "SqlBackedProvider"}

  def create_transform(
      self, typ: str, args: Mapping[str, Any],
      yaml_create_transform: Any) -> beam.PTransform:
    return self._transforms[typ](
        lambda query: self.sql_provider().create_transform(
            'Sql', {'query': query}, yaml_create_transform),
        **args)


PRIMITIVE_NAMES_TO_ATOMIC_TYPE = {
    py_type.__name__: schema_type
    for (py_type, schema_type) in schemas.PRIMITIVE_TO_ATOMIC_TYPE.items()
    if py_type.__module__ != 'typing'
}


def element_to_rows(e):
  if isinstance(e, dict):
    return dicts_to_rows(e)
  else:
    return beam.Row(element=dicts_to_rows(e))


def dicts_to_rows(o):
  if isinstance(o, dict):
    return beam.Row(**{k: dicts_to_rows(v) for k, v in o.items()})
  elif isinstance(o, list):
    return [dicts_to_rows(e) for e in o]
  else:
    return o


def _unify_element_with_schema(element, target_schema):
  """Convert an element to match the target schema, preserving existing
    fields only."""
  if target_schema is None:
    return element

  # If element is already a named tuple, convert to dict first
  if hasattr(element, '_asdict'):
    element_dict = element._asdict()
  elif isinstance(element, dict):
    element_dict = element
  else:
    # This element is not a row-like object. If the target schema has a single
    # field, assume this element is the value for that field.
    if len(target_schema._fields) == 1:
      return target_schema(**{target_schema._fields[0]: element})
    else:
      return element

  # Create new element with only the fields that exist in the original
  # element plus None for fields that are expected but missing
  unified_dict = {}
  for field_name in target_schema._fields:
    if field_name in element_dict:
      value = element_dict[field_name]
      # Ensure the value matches the expected type
      # This is particularly important for list fields
      if value is not None and not isinstance(value, list) and hasattr(
          value, '__iter__') and not isinstance(
              value, (str, bytes)) and not hasattr(value, '_asdict'):
        # Convert iterables to lists if needed
        unified_dict[field_name] = list(value)
      else:
        unified_dict[field_name] = value
    else:
      unified_dict[field_name] = None

  return target_schema(**unified_dict)


class YamlProviders:
  class AssertEqual(beam.PTransform):
    """Asserts that the input contains exactly the elements provided.

    This is primarily used for testing; it will cause the entire pipeline to
    fail if the input to this transform is not exactly the set of `elements`
    given in the config parameter.

    As with Create, YAML/JSON-style mappings are interpreted as Beam rows,
    e.g.::

        type: AssertEqual
        input: SomeTransform
        config:
          elements:
             - {a: 0, b: "foo"}
             - {a: 1, b: "bar"}

    would ensure that `SomeTransform` produced exactly two elements with values
    `(a=0, b="foo")` and `(a=1, b="bar")` respectively.

    Args:
        elements: The set of elements that should belong to the PCollection.
            YAML/JSON-style mappings will be interpreted as Beam rows.
    """
    def __init__(self, elements: Iterable[Any]):
      self._elements = elements

    def expand(self, pcoll):
      def to_dict(row):
        # filter None when comparing
        temp_dict = {k: v for k, v in row._asdict().items() if v is not None}
        return dict(temp_dict.items())

      return assert_that(
          pcoll | beam.Map(to_dict),
          equal_to([to_dict(e) for e in dicts_to_rows(self._elements)]))

  @staticmethod
  def create(elements: Iterable[Any], reshuffle: Optional[bool] = True):
    """Creates a collection containing a specified set of elements.

    This transform always produces schema'd data. For example::

        type: Create
        config:
          elements: [1, 2, 3]

    will result in an output with three elements with a schema of
    Row(element=int) whereas YAML/JSON-style mappings will be interpreted
    directly as Beam rows, e.g.::

        type: Create
        config:
          elements:
             - {first: 0, second: {str: "foo", values: [1, 2, 3]}}
             - {first: 1, second: {str: "bar", values: [4, 5, 6]}}

    will result in a schema of the form (int, Row(string, list[int])).

    This can also be expressed as YAML::

        type: Create
        config:
          elements:
            - first: 0
              second:
                str: "foo"
                 values: [1, 2, 3]
            - first: 1
              second:
                str: "bar"
                 values: [4, 5, 6]

    Args:
        elements: The set of elements that should belong to the PCollection.
            YAML/JSON-style mappings will be interpreted as Beam rows.
            Primitives will be mapped to rows with a single "element" field.
        reshuffle: (optional) Whether to introduce a reshuffle (to possibly
            redistribute the work) if there is more than one element in the
            collection. Defaults to True.
    """
    # Though str and dict are technically iterable, we disallow them
    # as using the characters or keys respectively is almost certainly
    # not the intent.
    if not isinstance(elements, Iterable) or isinstance(elements, (dict, str)):
      raise TypeError('elements must be a list of elements')

    # Check if elements have different keys
    updated_elements = elements
    if elements and all(isinstance(e, dict) for e in elements):
      keys = [set(e.keys()) for e in elements]
      if len(set.union(*keys)) > min(len(k) for k in keys):
        # Merge all dictionaries to get all possible keys
        all_keys = set()
        for element in elements:
          if isinstance(element, dict):
            all_keys.update(element.keys())

        # Create a merged dictionary with all keys
        merged_dict = {}
        for key in all_keys:
          merged_dict[key] = None  # Use None as a default value

        # Update each element with the merged dictionary
        updated_elements = []
        for e in elements:
          if isinstance(e, dict):
            updated_elements.append({**merged_dict, **e})
          else:
            updated_elements.append(e)

    return beam.Create([element_to_rows(e) for e in updated_elements],
                       reshuffle=reshuffle is not False)

  # Or should this be posargs, args?
  # pylint: disable=dangerous-default-value
  @staticmethod
  def fully_qualified_named_transform(
      constructor: str,
      args: Optional[Iterable[Any]] = (),
      kwargs: Optional[Mapping[str, Any]] = {}):
    """A Python PTransform identified by fully qualified name.

    This allows one to import, construct, and apply any Beam Python transform.
    This can be useful for using transforms that have not yet been exposed
    via a YAML interface. Note, however, that conversion may be required if this
    transform does not accept or produce Beam Rows.

    For example::

        type: PyTransform
        config:
           constructor: apache_beam.pkg.mod.SomeClass
           args: [1, 'foo']
           kwargs:
             baz: 3

    can be used to access the transform
    `apache_beam.pkg.mod.SomeClass(1, 'foo', baz=3)`.

    See also the documentation on
    [Inlining
    Python](https://beam.apache.org/documentation/sdks/yaml-inline-python/).

    Args:
        constructor: Fully qualified name of a callable used to construct the
            transform.  Often this is a class such as
            `apache_beam.pkg.mod.SomeClass` but it can also be a function or
            any other callable that returns a PTransform.
        args: A list of parameters to pass to the callable as positional
            arguments.
        kwargs: A list of parameters to pass to the callable as keyword
            arguments.
    """
    with FullyQualifiedNamedTransform.with_filter('*'):
      return constructor >> FullyQualifiedNamedTransform(
          constructor, args, kwargs)

  # This intermediate is needed because there is no way to specify a tuple of
  # exactly zero or one PCollection in yaml (as they would be interpreted as
  # PBegin and the PCollection itself respectively).
  class Flatten(beam.PTransform):
    """Flattens multiple PCollections into a single PCollection.

    The elements of the resulting PCollection will be the (disjoint) union of
    all the elements of all the inputs.

    Note that in YAML transforms can always take a list of inputs which will
    be implicitly flattened.
    """
    def __init__(self):
      # Suppress the "label" argument from the superclass for better docs.
      # pylint: disable=useless-parent-delegation
      super().__init__()

    def _merge_schemas(self, pcolls):
      """Merge schemas from multiple PCollections to create a unified schema.

      This function creates a unified schema that contains all fields from all
      input PCollections. Fields are made optional to handle missing values.
      If fields have different types, they are unified to Optional[Any].
      """
      from apache_beam.typehints.schemas import named_fields_from_element_type

      # Collect all schemas
      schemas = []
      for pcoll in pcolls:
        if hasattr(pcoll, 'element_type') and pcoll.element_type:
          try:
            fields = named_fields_from_element_type(pcoll.element_type)
            schemas.append(dict(fields))
          except (ValueError, TypeError):
            # If we can't extract schema, skip this PCollection
            continue

      if not schemas:
        return None

      # Merge all field names and types.
      all_field_names = set().union(*(s.keys() for s in schemas))
      unified_fields = {}
      for name in all_field_names:
        present_types = {s[name] for s in schemas if name in s}
        if len(present_types) > 1:
          unified_fields[name] = Optional[Any]
        else:
          unified_fields[name] = Optional[present_types.pop()]

      # Create unified schema
      if unified_fields:
        from apache_beam.typehints.schemas import named_fields_to_schema
        from apache_beam.typehints.schemas import named_tuple_from_schema
        unified_schema = named_fields_to_schema(list(unified_fields.items()))
        return named_tuple_from_schema(unified_schema)

      return None

    def expand(self, pcolls):
      if isinstance(pcolls, beam.PCollection):
        pipeline_arg = {}
        pcolls = (pcolls, )
      elif isinstance(pcolls, dict):
        pipeline_arg = {}
        pcolls = tuple(pcolls.values())
      else:
        pipeline_arg = {'pipeline': pcolls.pipeline}
        pcolls = ()

      if not pcolls:
        return pcolls | beam.Flatten(**pipeline_arg)

      # Try to unify schemas
      unified_schema = self._merge_schemas(pcolls)

      if unified_schema is None:
        # No schema unification needed, use standard flatten
        return pcolls | beam.Flatten(**pipeline_arg)

      # Apply schema unification to each PCollection before flattening.
      unified_pcolls = []
      for i, pcoll in enumerate(pcolls):
        unified_pcoll = pcoll | f'UnifySchema{i}' >> beam.Map(
            _unify_element_with_schema,
            target_schema=unified_schema).with_output_types(unified_schema)
        unified_pcolls.append(unified_pcoll)

      # Flatten the unified PCollections
      return unified_pcolls | beam.Flatten(**pipeline_arg)

  class WindowInto(beam.PTransform):
    # pylint: disable=line-too-long

    """A window transform assigning windows to each element of a PCollection.

    The assigned windows will affect all downstream aggregating operations,
    which will aggregate by window as well as by key.

    See [the Beam documentation on windowing](https://beam.apache.org/documentation/programming-guide/#windowing)
    for more details.

    Sizes, offsets, periods and gaps (where applicable) must be defined using
    a time unit suffix 'ms', 's', 'm', 'h' or 'd' for milliseconds, seconds,
    minutes, hours or days, respectively. If a time unit is not specified, it
    will default to 's'.

    For example::

        windowing:
           type: fixed
           size: 30s

    Note that any Yaml transform can have a
    [windowing parameter](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/README.md#windowing),
    which is applied to its inputs (if any) or outputs (if there are no inputs)
    which means that explicit WindowInto operations are not typically needed.

    Args:
      windowing: the type and parameters of the windowing to perform
    """
    def __init__(self, windowing):
      self._window_transform = self._parse_window_spec(windowing)

    def expand(self, pcoll):
      return pcoll | self._window_transform

    @staticmethod
    def _parse_duration(value, name):
      time_units = {
          'ms': 0.001, 's': 1, 'm': 60, 'h': 60 * 60, 'd': 60 * 60 * 12
      }
      value, suffix = re.match(r'^(.*?)([^\d]*)$', str(value)).groups()
      # Default to seconds if time unit suffix is not defined
      if not suffix:
        suffix = 's'
      if not value:
        raise ValueError(
            f"Invalid windowing {name} value "
            f"'{suffix if not value else value}'. "
            f"Must provide numeric value.")
      if suffix not in time_units:
        raise ValueError((
            "Invalid windowing {} time unit '{}'. " +
            "Valid time units are {}.").format(
                name,
                suffix,
                ', '.join("'{}'".format(k) for k in time_units.keys())))
      return float(value) * time_units[suffix]

    @staticmethod
    def _parse_window_spec(spec):
      spec = dict(spec)
      window_type = spec.pop('type')
      # TODO: These are in seconds, perhaps parse duration strings meaningfully?
      if window_type == 'global':
        window_fn = window.GlobalWindows()
      elif window_type == 'fixed':
        window_fn = window.FixedWindows(
            YamlProviders.WindowInto._parse_duration(spec.pop('size'), 'size'),
            YamlProviders.WindowInto._parse_duration(
                spec.pop('offset', 0), 'offset'))
      elif window_type == 'sliding':
        window_fn = window.SlidingWindows(
            YamlProviders.WindowInto._parse_duration(spec.pop('size'), 'size'),
            YamlProviders.WindowInto._parse_duration(
                spec.pop('period'), 'period'),
            YamlProviders.WindowInto._parse_duration(
                spec.pop('offset', 0), 'offset'))
      elif window_type == 'sessions':
        window_fn = window.Sessions(
            YamlProviders.WindowInto._parse_duration(spec.pop('gap'), 'gap'))
      else:
        raise ValueError(f'Unknown window type {window_type}')
      if spec:
        raise ValueError(f'Unknown parameters {spec.keys()}')
      # TODO: Triggering, etc.
      return beam.WindowInto(window_fn)

  @staticmethod
  @beam.ptransform_fn
  @maybe_with_exception_handling_transform_fn
  def log_for_testing(
      pcoll, *, level: Optional[str] = 'INFO', prefix: Optional[str] = ''):
    """Logs each element of its input PCollection.

    The output of this transform is a copy of its input for ease of use in
    chain-style pipelines.

    Args:
      level: one of ERROR, INFO, or DEBUG, mapped to a corresponding
        language-specific logging level
      prefix: an optional identifier that will get prepended to the element
        being logged
    """
    # Keeping this simple to be language agnostic.
    # The intent is not to develop a logging library (and users can always do)
    # their own mappings to get fancier output.
    log_levels = {
        'ERROR': logging.error,
        'INFO': logging.info,
        'DEBUG': logging.debug,
    }
    if level not in log_levels:
      raise ValueError(
          f'Unknown log level {level} not in {list(log_levels.keys())}')
    logger = log_levels[level]

    def to_loggable_json_recursive(o):
      if isinstance(o, (str, bytes)):
        return str(o)
      elif callable(getattr(o, '_asdict', None)):
        return to_loggable_json_recursive(o._asdict())
      elif isinstance(o, Mapping) and callable(getattr(o, 'items', None)):
        return {str(k): to_loggable_json_recursive(v) for k, v in o.items()}
      elif isinstance(o, Iterable):
        return [to_loggable_json_recursive(x) for x in o]
      else:
        return o

    def log_and_return(x):
      logger(prefix + json.dumps(to_loggable_json_recursive(x)))
      return x

    return pcoll | "LogForTesting" >> beam.Map(log_and_return)

  @staticmethod
  def create_builtin_provider():
    return InlineProvider({
        'AssertEqual': YamlProviders.AssertEqual,
        'Create': YamlProviders.create,
        'LogForTesting': YamlProviders.log_for_testing,
        'PyTransform': YamlProviders.fully_qualified_named_transform,
        'Flatten': YamlProviders.Flatten,
        'WindowInto': YamlProviders.WindowInto,
    },
                          no_input_transforms=('Create', ))


class TranslatingProvider(Provider):
  def __init__(
      self,
      transforms: Mapping[str, Callable[..., beam.PTransform]],
      underlying_provider: Provider):
    self._transforms = transforms
    self._underlying_provider = underlying_provider

  def provided_transforms(self):
    return self._transforms.keys()

  def available(self):
    return self._underlying_provider.available()

  def cache_artifacts(self):
    return self._underlying_provider.cache_artifacts()

  def underlying_provider(self):
    return self._underlying_provider

  def to_json(self):
    return {'type': "TranslatingProvider"}

  def create_transform(
      self, typ: str, config: Mapping[str, Any],
      yaml_create_transform: Any) -> beam.PTransform:
    return self._transforms[typ](self._underlying_provider, **config)

  def _with_extra_dependencies(self, dependencies: Iterable[str]):
    return TranslatingProvider(
        self._transforms,
        self._underlying_provider._with_extra_dependencies(dependencies))


def create_java_builtin_provider():
  """Exposes built-in transforms from Java as well as Python to maximize
  opportunities for fusion.

  This class holds those transforms that require pre-processing of the configs.
  For those Java transforms that can consume the user-provided configs directly
  (or only need a simple renaming of parameters) a direct or renaming provider
  is the simpler choice.
  """

  # An alternative could be examining the capabilities of various environments
  # during (or as a pre-processing phase before) fusion to align environments
  # where possible.  This would also require extra care in skipping these
  # common transforms when doing the provider affinity analysis.

  def java_window_into(java_provider, windowing):
    """Use the `windowing` WindowingStrategy and invokes the Java class.

    Though it would not be that difficult to implement this in Java as well,
    we prefer to implement it exactly once for consistency (especially as
    it evolves).
    """
    windowing_strategy = YamlProviders.WindowInto._parse_window_spec(
        windowing).get_windowing(None)
    # No context needs to be preserved for the basic WindowFns.
    empty_context = pipeline_context.PipelineContext()
    return java_provider.create_transform(
        'WindowIntoStrategy',
        {
            'serialized_windowing_strategy': windowing_strategy.to_runner_api(
                empty_context).SerializeToString()
        },
        None)

  return TranslatingProvider(
      transforms={'WindowInto': java_window_into},
      underlying_provider=beam_jar(
          urns={
              'WindowIntoStrategy': (
                  'beam:schematransform:'
                  'org.apache.beam:yaml:window_into_strategy:v1')
          },
          gradle_target=
          'sdks:java:extensions:schemaio-expansion-service:shadowJar'))


class PypiExpansionService:
  """Expands transforms by fully qualified name in a virtual environment
  with the given dependencies.
  """
  if 'TOX_WORK_DIR' in os.environ:
    VENV_CACHE = tempfile.mkdtemp(
        prefix='test-venv-cache-', dir=os.environ['TOX_WORK_DIR'])
  elif 'RUNNER_WORKDIR' in os.environ:
    VENV_CACHE = tempfile.mkdtemp(
        prefix='test-venv-cache-', dir=os.environ['RUNNER_WORKDIR'])
  else:
    VENV_CACHE = os.path.expanduser("~/.apache_beam/cache/venvs")

  def __init__(
      self, packages: Iterable[str], base_python: str = sys.executable):
    if not isinstance(packages, Iterable) or isinstance(packages, str):
      raise TypeError(
          "Packages must be an iterable of strings, got %r" % packages)
    self._packages = list(packages)
    self._base_python = base_python

  @classmethod
  def _key(cls, base_python: str, packages: list[str]) -> str:
    def normalize_package(package):
      if os.path.exists(package):
        # Ignore the exact path by which this package was referenced,
        # but do create a new environment if it changed.
        with open(package, 'rb') as fin:
          return os.path.basename(package) + '-' + _file_digest(
              fin, 'sha256').hexdigest()
      else:
        # Assume urls and pypi identifiers are immutable.
        return package

    return json.dumps({
        'binary': base_python,
        'packages': sorted(normalize_package(p) for p in packages)
    },
                      sort_keys=True)

  @classmethod
  def _path(cls, base_python: str, packages: list[str]) -> str:
    return os.path.join(
        cls.VENV_CACHE,
        hashlib.sha256(cls._key(base_python,
                                packages).encode('utf-8')).hexdigest())

  @classmethod
  def _create_venv_from_scratch(
      cls, base_python: str, packages: list[str]) -> str:
    venv = cls._path(base_python, packages)
    if not os.path.exists(venv):
      try:
        subprocess.run([base_python, '-m', 'venv', venv], check=True)
        venv_python = os.path.join(venv, 'bin', 'python')
        venv_pip = os.path.join(venv, 'bin', 'pip')
        subprocess.run([venv_python, '-m', 'ensurepip'], check=True)
        # Issue warning when installing packages from PyPI
        _LOGGER.warning(
            " WARNING: Apache Beam is installing Python packages "
            "from PyPI at runtime.\n"
            "   This may pose security risks or cause instability due to "
            "repository availability.\n"
            "   Packages: %s\n"
            "   Consider pre-staging dependencies or using a private "
            "repository mirror.\n"
            "   For more information, see: "
            "https://beam.apache.org/documentation/sdks/python-dependencies/",
            ', '.join(packages))
        subprocess.run([venv_pip, 'install'] + packages, check=True)
        with open(venv + '-requirements.txt', 'w') as fout:
          fout.write('\n'.join(packages))
      except:  # pylint: disable=bare-except
        if os.path.exists(venv):
          shutil.rmtree(venv, ignore_errors=True)
        raise
    return venv

  @classmethod
  def _create_venv_from_clone(
      cls, base_python: str, packages: list[str]) -> str:
    venv = cls._path(base_python, packages)
    if not os.path.exists(venv):
      try:
        # Avoid hard dependency for environments where this is never used.
        import clonevirtualenv
        clonable_venv = cls._create_venv_to_clone(base_python)
        clonevirtualenv.clone_virtualenv(clonable_venv, venv)
        venv_pip = os.path.join(venv, 'bin', 'pip')
        # Issue warning when installing packages from PyPI
        _LOGGER.warning(
            "   WARNING: Apache Beam is installing Python packages "
            "from PyPI at runtime.\n"
            "   This may pose security risks or cause instability due to "
            "repository availability.\n"
            "   Packages: %s\n"
            "   Consider pre-staging dependencies or using a private "
            "repository mirror.\n"
            "   For more information, see: "
            "https://beam.apache.org/documentation/sdks/python-dependencies/",
            ', '.join(packages))
        subprocess.run([venv_pip, 'install'] + packages, check=True)
        with open(venv + '-requirements.txt', 'w') as fout:
          fout.write('\n'.join(packages))
      except:  # pylint: disable=bare-except
        if os.path.exists(venv):
          shutil.rmtree(venv, ignore_errors=True)
        raise
    return venv

  @classmethod
  def _create_venv_to_clone(cls, base_python: str) -> str:
    if '.dev' in beam_version:
      base_venv = os.path.dirname(os.path.dirname(base_python))
      print('Cloning dev environment from', base_venv)
      return base_venv
    return cls._create_venv_from_scratch(
        base_python,
        [
            'apache_beam[dataframe,gcp,test,yaml]==' + beam_version,
            'virtualenv-clone'
        ])

  def _venv(self) -> str:
    return self._create_venv_from_clone(self._base_python, self._packages)

  def __enter__(self):
    venv = self._venv()
    self._service_provider = subprocess_server.SubprocessServer(
        external.ExpansionAndArtifactRetrievalStub,
        [
            os.path.join(venv, 'bin', 'python'),
            '-m',
            'apache_beam.runners.portability.expansion_service_main',
            '--port',
            '{{PORT}}',
            '--fully_qualified_name_glob=*',
            '--pickle_library=cloudpickle',
            '--requirements_file=' + os.path.join(venv + '-requirements.txt'),
            '--serve_loopback_worker',
        ])
    self._service = self._service_provider.__enter__()
    return self._service

  def __exit__(self, *args):
    self._service_provider.__exit__(*args)
    self._service = None


@ExternalProvider.register_provider_type('renaming')
class RenamingProvider(Provider):
  def __init__(
      self,
      transforms,
      provider_base_path,
      mappings,
      underlying_provider,
      defaults=None):
    if isinstance(underlying_provider, dict):
      underlying_provider = ExternalProvider.provider_from_spec(
          provider_base_path, underlying_provider)
    self._transforms = transforms
    self._underlying_provider = underlying_provider
    for transform in transforms.keys():
      if transform not in mappings:
        raise ValueError(f'Missing transform {transform} in mappings.')
    self._mappings = self.expand_mappings(mappings)
    self._defaults = defaults or {}

  @staticmethod
  def expand_mappings(mappings):
    if not isinstance(mappings, dict):
      raise ValueError(
          "RenamingProvider mappings must be dict of transform "
          "mappings.")
    for key, value in mappings.items():
      if isinstance(value, str):
        if value not in mappings.keys():
          raise ValueError(
              "RenamingProvider transform mappings must be dict or "
              "specify transform that has mappings within same "
              "provider.")
        mappings[key] = mappings[value]
    return mappings

  def available(self) -> bool:
    return self._underlying_provider.available()

  def provided_transforms(self) -> Iterable[str]:
    return self._transforms.keys()

  def config_schema(self, type):
    underlying_schema = self._underlying_provider.config_schema(
        self._transforms[type])
    if underlying_schema is None:
      return None
    defaults = self._defaults.get(type, {})
    underlying_schema_fields = {f.name: f for f in underlying_schema.fields}
    missing = set(self._mappings[type].values()) - set(
        underlying_schema_fields.keys())
    if missing:
      if 'kwargs' in underlying_schema_fields.keys():
        # These are likely passed by keyword argument dict rather than missing.
        for field_name in missing:
          underlying_schema_fields[field_name] = schema_pb2.Field(
              name=field_name, type=typing_to_runner_api(Any))
      else:
        raise ValueError(
            f"Mapping destinations {missing} for {type} are not in the "
            f"underlying config schema {list(underlying_schema_fields.keys())}")

    def with_name(
        original: schema_pb2.Field, new_name: str) -> schema_pb2.Field:
      result = schema_pb2.Field()
      result.CopyFrom(original)
      result.name = new_name
      return result

    return schema_pb2.Schema(
        fields=[
            with_name(underlying_schema_fields[dest], src)
            for (src, dest) in self._mappings[type].items()
            if dest not in defaults
        ])

  def description(self, typ):
    return self._underlying_provider.description(self._transforms[typ])

  def requires_inputs(self, typ, args):
    return self._underlying_provider.requires_inputs(
        self._transforms[typ], args)

  def create_transform(
      self,
      typ: str,
      args: Mapping[str, Any],
      yaml_create_transform: Callable[
          [Mapping[str, Any], Iterable[beam.PCollection]], beam.PTransform]
  ) -> beam.PTransform:
    """Creates a PTransform instance for the given transform type and arguments.
    """
    mappings = self._mappings[typ]
    remapped_args = {
        mappings.get(key, key): value
        for key, value in args.items()
    }
    for key, value in self._defaults.get(typ, {}).items():
      if key not in remapped_args:
        remapped_args[key] = value
    return self._underlying_provider.create_transform(
        self._transforms[typ], remapped_args, yaml_create_transform)

  def _affinity(self, other):
    raise NotImplementedError(
        'Should not be calling _affinity directly on this provider.')

  def underlying_provider(self):
    return self._underlying_provider.underlying_provider()

  def cache_artifacts(self):
    self._underlying_provider.cache_artifacts()

  def _with_extra_dependencies(self, dependencies: Iterable[str]):
    return RenamingProvider(
        self._transforms,
        None,
        self._mappings,
        self._underlying_provider._with_extra_dependencies(dependencies),
        self._defaults)


def _as_list(func):
  @functools.wraps(func)
  def wrapper(*args, **kwargs):
    return list(func(*args, **kwargs))

  return wrapper


def _join_url_or_filepath(base, path):
  if not base:
    return path

  if urllib.parse.urlparse(path).scheme:
    # path is an absolute path with scheme (whether it is the same as base or
    # not).
    return path

  # path is a relative path or an absolute path without scheme (e.g. /a/b/c)
  base_scheme = urllib.parse.urlparse(base, '').scheme
  if base_scheme and base_scheme in urllib.parse.uses_relative:
    return urllib.parse.urljoin(base, path)
  else:
    if FileSystems.join(base, "") == base:
      # base ends with a filesystem separator
      return FileSystems.join(base, path)
    return FileSystems.join(FileSystems.split(base)[0], path)


def _read_url_or_filepath(path):
  scheme = urllib.parse.urlparse(path, '').scheme
  if scheme and scheme in urllib.parse.uses_netloc:
    with urllib.request.urlopen(path) as response:
      return response.read()
  else:
    with FileSystems.open(path) as fin:
      return fin.read()


def load_providers(source_path: str) -> Iterable[Provider]:
  from apache_beam.yaml.yaml_transform import SafeLineLoader
  provider_specs = yaml.load(
      _read_url_or_filepath(source_path), Loader=SafeLineLoader)
  if not isinstance(provider_specs, list):
    raise ValueError(f"Provider file {source_path} must be a list of Providers")
  return parse_providers(source_path, provider_specs)


@_as_list
def parse_providers(source_path,
                    provider_specs: Iterable[Mapping]) -> Iterable[Provider]:
  from apache_beam.yaml.yaml_transform import SafeLineLoader
  for provider_spec in provider_specs:
    if 'include' in provider_spec:
      if len(SafeLineLoader.strip_metadata(provider_spec)) != 1:
        raise ValueError(
            f"When using include, it must be the only parameter: "
            f"{provider_spec} "
            f"at {source_path}:{SafeLineLoader.get_line(provider_spec)}")
      include_path = _join_url_or_filepath(
          source_path, provider_spec['include'])
      try:
        yield from load_providers(include_path)

      except Exception as exn:
        raise ValueError(
            f"Error loading providers from {include_path} included at "
            f"{source_path}:{SafeLineLoader.get_line(provider_spec)}\n" +
            str(exn)) from exn
    else:
      yield ExternalProvider.provider_from_spec(source_path, provider_spec)


def merge_providers(*provider_sets) -> Mapping[str, Iterable[Provider]]:
  result = collections.defaultdict(list)
  for provider_set in provider_sets:
    if isinstance(provider_set, Provider):
      provider = provider_set
      provider_set = {
          transform_type: [provider]
          for transform_type in provider.provided_transforms()
      }
    elif isinstance(provider_set, list):
      provider_set = merge_providers(*provider_set)
    for transform_type, providers in provider_set.items():
      result[transform_type].extend(providers)
  return result


@functools.cache
def standard_providers():
  from apache_beam.yaml.yaml_combine import create_combine_providers
  from apache_beam.yaml.yaml_mapping import create_mapping_providers
  from apache_beam.yaml.yaml_join import create_join_providers
  from apache_beam.yaml.yaml_io import io_providers
  from apache_beam.yaml.yaml_specifiable import create_spec_providers

  return merge_providers(
      YamlProviders.create_builtin_provider(),
      create_java_builtin_provider(),
      create_mapping_providers(),
      create_combine_providers(),
      create_join_providers(),
      io_providers(),
      create_spec_providers(),
      load_providers(yaml_utils.locate_data_file('standard_providers.yaml')))


def _file_digest(fileobj, digest):
  if hasattr(hashlib, 'file_digest'):  # Python 3.11+
    return hashlib.file_digest(fileobj, digest)
  else:
    hasher = hashlib.new(digest)
    data = fileobj.read(1 << 20)
    while data:
      hasher.update(data)
      data = fileobj.read(1 << 20)
    return hasher


class _InlineProviderNamespace:
  """Gives fully qualified names to inline providers from standard_providers().

  This is needed to upgrade InlineProvider to ExternalPythonProvider.
  """
  def __getattr__(self, name):
    typ = name.replace('_', '-')
    for provider in standard_providers()[typ]:
      if isinstance(provider, InlineProvider):
        return provider._transform_factories[typ]
    raise ValueError(f"No inline provider found for {name}")


standard_inline_providers = _InlineProviderNamespace()
