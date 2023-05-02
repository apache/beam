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

import collections
import hashlib
import json
import os
import subprocess
import sys
import uuid
from typing import Any
from typing import Iterable
from typing import Mapping

import yaml
from yaml.loader import SafeLoader

import apache_beam as beam
import apache_beam.dataframe.io
import apache_beam.io
import apache_beam.transforms.util
from apache_beam.portability.api import schema_pb2
from apache_beam.transforms import external
from apache_beam.transforms import window
from apache_beam.transforms.fully_qualified_named_transform import FullyQualifiedNamedTransform
from apache_beam.typehints import schemas
from apache_beam.typehints import trivial_inference
from apache_beam.utils import python_callable
from apache_beam.utils import subprocess_server
from apache_beam.version import __version__ as beam_version


class Provider:
  """Maps transform types names and args to concrete PTransform instances."""
  def available(self) -> bool:
    """Returns whether this provider is available to use in this environment."""
    raise NotImplementedError(type(self))

  def provided_transforms(self) -> Iterable[str]:
    """Returns a list of transform type names this provider can handle."""
    raise NotImplementedError(type(self))

  def create_transform(
      self, typ: str, args: Mapping[str, Any]) -> beam.PTransform:
    """Creates a PTransform instance for the given transform type and arguments.
    """
    raise NotImplementedError(type(self))


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
  def __init__(self, urns, service):
    self._urns = urns
    self._service = service
    self._schema_transforms = None

  def provided_transforms(self):
    return self._urns.keys()

  def create_transform(self, type, args):
    if callable(self._service):
      self._service = self._service()
    if self._schema_transforms is None:
      try:
        self._schema_transforms = [
            config.identifier
            for config in external.SchemaAwareExternalTransform.discover(
                self._service)
        ]
      except Exception:
        self._schema_transforms = []
    urn = self._urns[type]
    if urn in self._schema_transforms:
      return external.SchemaAwareExternalTransform(urn, self._service, **args)
    else:
      return type >> self.create_external_transform(urn, args)

  def create_external_transform(self, urn, args):
    return external.ExternalTransform(
        urn,
        external.ImplicitSchemaPayloadBuilder(args).payload(),
        self._service)

  @staticmethod
  def provider_from_spec(spec):
    urns = spec['transforms']
    type = spec['type']
    if spec.get('version', None) == 'BEAM_VERSION':
      spec['version'] = beam_version
    if type == 'javaJar':
      return ExternalJavaProvider(urns, lambda: spec['jar'])
    elif type == 'mavenJar':
      return ExternalJavaProvider(
          urns,
          lambda: subprocess_server.JavaJarServer.path_to_maven_jar(
              **{
                  key: value
                  for (key, value) in spec.items() if key in [
                      'artifact_id',
                      'group_id',
                      'version',
                      'repository',
                      'classifier',
                      'appendix'
                  ]
              }))
    elif type == 'beamJar':
      return ExternalJavaProvider(
          urns,
          lambda: subprocess_server.JavaJarServer.path_to_beam_jar(
              **{
                  key: value
                  for (key, value) in spec.items() if key in
                  ['gradle_target', 'version', 'appendix', 'artifact_id']
              }))
    elif type == 'pythonPackage':
      return ExternalPythonProvider(urns, spec['packages'])
    elif type == 'remote':
      return RemoteProvider(spec['address'])
    elif type == 'docker':
      raise NotImplementedError()
    else:
      raise NotImplementedError(f'Unknown provider type: {type}')


class RemoteProvider(ExternalProvider):
  _is_available = None

  def available(self):
    if self._is_available is None:
      try:
        with external.ExternalTransform.service(self._service) as service:
          service.ready(1)
          self._is_available = True
      except Exception:
        self._is_available = False
    return self._is_available


class ExternalJavaProvider(ExternalProvider):
  def __init__(self, urns, jar_provider):
    super().__init__(
        urns, lambda: external.JavaJarExpansionService(jar_provider()))

  def available(self):
    # pylint: disable=subprocess-run-check
    return subprocess.run(['which', 'java'],
                          capture_output=True).returncode == 0


class ExternalPythonProvider(ExternalProvider):
  def __init__(self, urns, packages):
    super().__init__(urns, PypiExpansionService(packages))

  def available(self):
    return True  # If we're running this script, we have Python installed.

  def create_external_transform(self, urn, args):
    # Python transforms are "registered" by fully qualified name.
    return external.ExternalTransform(
        "beam:transforms:python:fully_qualified_named",
        external.ImplicitSchemaPayloadBuilder({
            'constructor': urn,
            'kwargs': args,
        }).payload(),
        self._service)


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
  def __init__(self, transform_factories):
    self._transform_factories = transform_factories

  def available(self):
    return True

  def provided_transforms(self):
    return self._transform_factories.keys()

  def create_transform(self, type, args):
    return self._transform_factories[type](**args)

  def to_json(self):
    return {'type': "InlineProvider"}


PRIMITIVE_NAMES_TO_ATOMIC_TYPE = {
    py_type.__name__: schema_type
    for (py_type, schema_type) in schemas.PRIMITIVE_TO_ATOMIC_TYPE.items()
    if py_type.__module__ != 'typing'
}


def create_builtin_provider():
  def with_schema(**args):
    # TODO: This is preliminary.
    def parse_type(spec):
      if spec in PRIMITIVE_NAMES_TO_ATOMIC_TYPE:
        return schema_pb2.FieldType(
            atomic_type=PRIMITIVE_NAMES_TO_ATOMIC_TYPE[spec])
      elif isinstance(spec, list):
        if len(spec) != 1:
          raise ValueError("Use single-element lists to denote list types.")
        else:
          return schema_pb2.FieldType(
              iterable_type=schema_pb2.IterableType(
                  element_type=parse_type(spec[0])))
      elif isinstance(spec, dict):
        return schema_pb2.FieldType(
            iterable_type=schema_pb2.RowType(schema=parse_schema(spec[0])))
      else:
        raise ValueError("Unknown schema type: {spec}")

    def parse_schema(spec):
      return schema_pb2.Schema(
          fields=[
              schema_pb2.Field(name=key, type=parse_type(value), id=ix)
              for (ix, (key, value)) in enumerate(spec.items())
          ],
          id=str(uuid.uuid4()))

    named_tuple = schemas.named_tuple_from_schema(parse_schema(args))
    names = list(args.keys())

    def extract_field(x, name):
      if isinstance(x, dict):
        return x[name]
      else:
        return getattr(x, name)

    return 'WithSchema(%s)' % ', '.join(names) >> beam.Map(
        lambda x: named_tuple(*[extract_field(x, name) for name in names])
    ).with_output_types(named_tuple)

  # Or should this be posargs, args?
  # pylint: disable=dangerous-default-value
  def fully_qualified_named_transform(constructor, args=(), kwargs={}):
    with FullyQualifiedNamedTransform.with_filter('*'):
      return constructor >> FullyQualifiedNamedTransform(
          constructor, args, kwargs)

  # This intermediate is needed because there is no way to specify a tuple of
  # exactly zero or one PCollection in yaml (as they would be interpreted as
  # PBegin and the PCollection itself respectively).
  class Flatten(beam.PTransform):
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
      return pcolls | beam.Flatten(**pipeline_arg)

  class WindowInto(beam.PTransform):
    def __init__(self, windowing):
      self._window_transform = self._parse_window_spec(windowing)

    def expand(self, pcoll):
      return pcoll | self._window_transform

    @staticmethod
    def _parse_window_spec(spec):
      spec = dict(spec)
      window_type = spec.pop('type')
      # TODO: These are in seconds, perhaps parse duration strings meaningfully?
      if window_type == 'global':
        window_fn = window.GlobalWindows()
      elif window_type == 'fixed':
        window_fn = window.FixedWindows(spec.pop('size'), spec.pop('offset', 0))
      elif window_type == 'sliding':
        window_fn = window.SlidingWindows(
            spec.pop('size'), spec.pop('period'), spec.pop('offset', 0))
      elif window_type == 'sessions':
        window_fn = window.FixedWindows(spec.pop('gap'))
      if spec:
        raise ValueError(f'Unknown parameters {spec.keys()}')
      # TODO: Triggering, etc.
      return beam.WindowInto(window_fn)

  ios = {
      key: getattr(apache_beam.io, key)
      for key in dir(apache_beam.io)
      if key.startswith('ReadFrom') or key.startswith('WriteTo')
  }

  return InlineProvider(
      dict({
          'Create': lambda elements,
          reshuffle=True: beam.Create(elements, reshuffle),
          'PyMap': lambda fn: beam.Map(
              python_callable.PythonCallableWithSource(fn)),
          'PyMapTuple': lambda fn: beam.MapTuple(
              python_callable.PythonCallableWithSource(fn)),
          'PyFlatMap': lambda fn: beam.FlatMap(
              python_callable.PythonCallableWithSource(fn)),
          'PyFlatMapTuple': lambda fn: beam.FlatMapTuple(
              python_callable.PythonCallableWithSource(fn)),
          'PyFilter': lambda keep: beam.Filter(
              python_callable.PythonCallableWithSource(keep)),
          'PyTransform': fully_qualified_named_transform,
          'PyToRow': lambda fields: beam.Select(
              **{
                  name: python_callable.PythonCallableWithSource(fn)
                  for (name, fn) in fields.items()
              }),
          'WithSchema': with_schema,
          'Flatten': Flatten,
          'WindowInto': WindowInto,
          'GroupByKey': beam.GroupByKey,
      },
           **ios))


class PypiExpansionService:
  """Expands transforms by fully qualified name in a virtual environment
  with the given dependencies.
  """
  VENV_CACHE = os.path.expanduser("~/.apache_beam/cache/venvs")

  def __init__(self, packages, base_python=sys.executable):
    self._packages = packages
    self._base_python = base_python

  def _key(self):
    return json.dumps({'binary': self._base_python, 'packages': self._packages})

  def _venv(self):
    venv = os.path.join(
        self.VENV_CACHE,
        hashlib.sha256(self._key().encode('utf-8')).hexdigest())
    if not os.path.exists(venv):
      python_binary = os.path.join(venv, 'bin', 'python')
      subprocess.run([self._base_python, '-m', 'venv', venv], check=True)
      subprocess.run([python_binary, '-m', 'ensurepip'], check=True)
      subprocess.run([python_binary, '-m', 'pip', 'install'] + self._packages,
                     check=True)
      with open(venv + '-requirements.txt', 'w') as fout:
        fout.write('\n'.join(self._packages))
    return venv

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
            '--requirements_file=' + os.path.join(venv + '-requirements.txt')
        ])
    self._service = self._service_provider.__enter__()
    return self._service

  def __exit__(self, *args):
    self._service_provider.__exit__(*args)
    self._service = None


def parse_providers(provider_specs):
  providers = collections.defaultdict(list)
  for provider_spec in provider_specs:
    provider = ExternalProvider.provider_from_spec(provider_spec)
    for transform_type in provider.provided_transforms():
      providers[transform_type].append(provider)
      # TODO: Do this better.
      provider.to_json = lambda result=provider_spec: result
  return providers


def merge_providers(*provider_sets):
  result = collections.defaultdict(list)
  for provider_set in provider_sets:
    for transform_type, providers in provider_set.items():
      result[transform_type].extend(providers)
  return result


def standard_providers():
  builtin_providers = collections.defaultdict(list)
  builtin_provider = create_builtin_provider()
  for transform_type in builtin_provider.provided_transforms():
    builtin_providers[transform_type].append(builtin_provider)
  with open(os.path.join(os.path.dirname(__file__),
                         'standard_providers.yaml')) as fin:
    standard_providers = yaml.load(fin, Loader=SafeLoader)
  return merge_providers(builtin_providers, parse_providers(standard_providers))
