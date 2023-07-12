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

"""Unit tests for yaml transform provider."""
import collections
import copy
import json
import logging
import subprocess
import unittest

import mock

import apache_beam as beam
from apache_beam import beam_expansion_api_pb2, ExternalTransform, SchemaAwareExternalTransform, ParDo, \
  JavaJarExpansionService
from apache_beam.transforms import external
from apache_beam.transforms.ptransform import _NamedPTransform
from apache_beam.version import __version__ as beam_version
from apache_beam.yaml.yaml_provider import ExternalProvider, InlineProvider, MetaInlineProvider, Provider, \
  ExternalPythonProvider, ExternalJavaProvider, PypiExpansionService, RemoteProvider, merge_providers, as_provider, \
  parse_external_providers, as_provider_list

# pytype: skip-file


class ProviderTest(unittest.TestCase):
  """
  Base class for test cases that involve creating a transform Provider for
  YAML pipelines.
  """
  def __init__(self, method_name='runProviderTest'):
    super().__init__(method_name)
    self._type = 'MockProvider'
    self.providers = {self._type: Provider()}

  def test_available(self):
    """Assert that the base method is not implemented"""
    with self.assertRaises(NotImplementedError):
      for provider in self.providers.values():
        provider.available()

  def test_provided_transforms(self):
    """Assert that the base method is not implemented"""
    with self.assertRaises(NotImplementedError):
      for provider in self.providers.values():
        provider.provided_transforms()

  def test_create_transform(self):
    """Assert that the base method is not implemented"""
    with self.assertRaises(NotImplementedError):
      for provider in self.providers.values():
        provider.create_transform("", {}, lambda: beam.PTransform())

  def test_affinity(self):
    """Test the affinity score of various provider comparisons"""
    for provider in self.providers.values():

      class MockProvider(Provider):
        """Mock provider class for testing affinity"""
        def available(self):
          pass

        def provided_transforms(self):
          pass

        def create_transform(self, typ, args, yaml_create_transform):
          pass

      self.assertEqual(200, provider.affinity(provider))
      self.assertEqual(20, provider.affinity(copy.copy(provider)))
      self.assertEqual(0, provider.affinity(MockProvider()))


class InlineProviderTest(ProviderTest):
  """Test class for InlineProvider tests."""
  def __init__(self, method_name='runInlineProviderTest'):
    super().__init__(method_name)
    self._type = 'MockInline'
    self._transform_name = 'MockTransform'
    self._called_inline = False

    def mock_transform(fn):
      """Mock transform callable supplied to Provider's transform factory"""
      self._called_inline = True
      return self._transform_name >> beam.ParDo(fn)

    self.providers = {self._type: InlineProvider({self._type: mock_transform})}

  def test_available(self):
    """Test that the provider is available (Inline providers should always be available)."""
    for provider in self.providers.values():
      self.assertTrue(provider.available())

  def test_provided_transforms(self):
    """Test that the provided transforms include the given transforms."""
    for provider_type, provider in self.providers.items():
      self.assertEqual([provider_type], list(provider.provided_transforms()))

  def test_create_transform(self):
    """Test that the create_transform method creates the correct transform."""
    def create_transform(transform, args):
      return self.providers.get(transform).create_transform(
          transform, args, create_transform)

    ptransform = create_transform(self._type, {'fn': lambda x: x})
    self.assertEqual(self._transform_name, ptransform.label)
    self.assertTrue(self._called_inline)
    if isinstance(ptransform, _NamedPTransform):
      self.assertIsInstance(ptransform.transform, ParDo)
      self.assertEqual("ParDo(CallableWrapperDoFn)", ptransform.transform.label)
    else:
      raise ValueError


class MetaInlineProviderTest(InlineProviderTest):
  """Test class for MetaInlineProvider tests."""
  def __init__(self, method_name="runMetaInlineProviderTest"):
    super().__init__(method_name)
    self._inner_type = self._type
    self._type = 'MockMetaInline'
    self._transform_name = 'MockMetaTransform'
    self._called_meta = False

    def mock_meta_transform(create_transform_fn, fn):
      """Mock transform that constructs inner transform based on given create_transform_fn"""
      self._called_meta = True
      return create_transform_fn(self._inner_type, {'fn': fn})

    self.providers[self._type] = MetaInlineProvider(
        {self._type: mock_meta_transform})

  def test_create_transform(self):
    """Test that the created transform is expanded from the meta transform.d"""
    super().test_create_transform()
    self.assertTrue(self._called_meta)


class ExternalProviderTest(ProviderTest):
  """Test class for ExternalProvider tests."""
  def __init__(self, method_name="runExternalProviderTest"):
    super().__init__(method_name)

    self._type = 'MockExternal'
    self._urn = 'beam:external:service:MockExternalTransform'

    self._schema_type = 'MockSchemaExternal'
    self._schema_urn = 'beam:external:service:MockSchemaExternalTransform'

    self._urns = {self._type: self._urn, self._schema_type: self._schema_urn}
    self._schema_service = self.MockSchemaExpansionService(self._schema_urn)

    self._spec = {'transforms': self._urns}
    self.providers = {
        self._type: ExternalProvider(self._spec, None),
        self._schema_type: ExternalProvider(self._spec, self._schema_service)
    }

  class MockSchemaExpansionService:
    """Mock expansion service for testing schema-aware external transforms."""
    def __init__(self, schema_urn):
      super().__init__()
      self._schema_urn = schema_urn

    def __enter__(self):
      return self

    def __exit__(self, unused1, unused2, unused3):
      pass

    def DiscoverSchemaTransform(self, unused_request=None):
      return beam_expansion_api_pb2.DiscoverSchemaTransformResponse(
          schema_transform_configs={
              self._schema_urn: beam_expansion_api_pb2.SchemaTransformConfig()
          })

  def create_transform_fn(self, transform, args):
    """Helper function for creating the given transform using the provider's create_transform method."""
    return self.providers.get(transform).create_transform(
        transform, args, self.create_transform_fn)

  def test_provided_transforms(self):
    """Test that provided transforms are the given transforms"""
    for provider in self.providers.values():
      self.assertEqual([self._type, self._schema_type],
                       list(provider.provided_transforms()))

  def test_create_transform(self):
    """Test that creating the transform correctly constructs an ExternalTransform"""
    ptransform = self.create_transform_fn(self._type, {})
    self.assertEqual(self._type, ptransform.label)
    self.assertEqual(
        "ExternalTransform(%s)" % self._urn, ptransform.transform.label)
    self.assertEqual(self.providers.get(self._type)._schema_transforms, [])
    self.assertIsInstance(ptransform.transform, ExternalTransform)

  def test_schema_aware_transform(self):
    """
    Test that creating the transform creates a SchemaAwareExternalTransform when supplied a
    schema-aware transform using the given expansion service.
    """
    ptransform = self.create_transform_fn(self._schema_type, {})
    self.assertEqual("SchemaAwareExternalTransform", ptransform.label)
    self.assertEqual([self._schema_urn],
                     self.providers.get(self._schema_type)._schema_transforms)
    self.assertIsInstance(ptransform, SchemaAwareExternalTransform)
    self.assertIsInstance(
        ptransform._expansion_service, self.MockSchemaExpansionService)

  def test_to_json(self):
    """Test that the json form is the given spec."""
    for provider in self.providers.values():
      self.assertEqual(self._spec, provider.to_json())

  def test_provider_from_spec_populates_beam_version(self):
    """Test that the version in the spec file is replaced when set to BEAM_VERSION."""
    spec = {
        'type': 'fakeType', 'transforms': self._urns, 'version': 'BEAM_VERSION'
    }
    with self.assertRaises(NotImplementedError):
      ExternalProvider.provider_from_spec(spec)
      self.assertEqual(beam_version, spec['version'])

  # TODO - this test will need to be updated when docker support is added.
  def test_docker_provider_from_spec_throws_error(self):
    """Test that the docker provider is not implemented."""
    spec = {'type': 'docker', 'transforms': self._urns}
    with self.assertRaises(NotImplementedError):
      ExternalProvider.provider_from_spec(spec)

  def test_unknown_provider_from_spec_throws_error(self):
    """Test that unknown providers throw error."""
    spec = {'type': 'unknown', 'transforms': self._urns}
    with self.assertRaises(NotImplementedError) as e:
      ExternalProvider.provider_from_spec(spec)
      self.assertTrue("Unknown provider type" in e.exception)

  def test_provider_from_spec_fails_without_type(self):
    """Test that providing a spec without a type throws an error."""
    spec = {'transforms': self._urns}
    with self.assertRaises(KeyError):
      ExternalProvider.provider_from_spec(spec)


class RemoteProviderTest(ExternalProviderTest):
  """Test class for RemoteProvider tests."""
  def __init__(self, method_name="runRemoteProviderTest"):
    super().__init__(method_name)

    self._address = {'127.0.0.1': '0000'}
    self._spec = {
        'type': 'remote', 'address': self._address, 'transforms': self._urns
    }
    self.providers = {
        self._type: RemoteProvider(self._spec),
        self._schema_type: RemoteProvider(self._spec)
    }

  @mock.patch('apache_beam.transforms.external.ExternalTransform.service')
  def test_available(self, mock_service):
    """Test that the provider is available when the service is ready."""
    self.assertIsNone(self.providers.get(self._schema_type)._is_available)
    self.assertTrue(self.providers.get(self._schema_type).available())
    self.assertTrue(self.providers.get(self._schema_type)._is_available)
    mock_service.assert_has_calls(mock_service.ready)

  @mock.patch('apache_beam.transforms.external.ExternalTransform.service')
  def test_not_available(self, mock_service):
    """Test that the provider is not available when the service is not ready."""
    mock_service().__enter__().ready.side_effect = Exception
    self.assertFalse(self.providers.get(self._schema_type).available())
    self.assertFalse(self.providers.get(self._schema_type)._is_available)
    mock_service().__enter__().ready.assert_called_once()

  @mock.patch('apache_beam.transforms.external.ExternalTransform.service')
  def test_expansion_service_fails_to_connect(self, mock_service):
    """Test that the provider is not available when the service fails to connect."""
    mock_service.side_effect = Exception
    self.assertFalse(self.providers.get(self._schema_type).available())
    self.assertFalse(self.providers.get(self._schema_type)._is_available)

  @mock.patch(
      'apache_beam.transforms.external.SchemaAwareExternalTransform.discover')
  def test_schema_aware_transform(self, mock_discover):
    """Calls parent test with a config used for remote providers."""
    config = mock.Mock()
    config.identifier = 'beam:external:service:MockSchemaExternalTransform'
    mock_discover.return_value = [config]
    ptransform = self.create_transform_fn(self._schema_type, {})

    self.assertEqual("SchemaAwareExternalTransform", ptransform.label)
    self.assertEqual([self._schema_urn],
                     self.providers.get(self._schema_type)._schema_transforms)
    self.assertIsInstance(ptransform, SchemaAwareExternalTransform)
    self.assertEqual(ptransform._expansion_service, self._address)

  def test_remote_provider_from_spec(self):
    """Test that a remote provider can be parsed from a spec."""
    spec = {
        'type': 'remote', 'address': self._address, 'transforms': self._urns
    }
    provider = ExternalProvider.provider_from_spec(spec)
    self.assertIsInstance(provider, RemoteProvider)
    self.assertEqual(self._address, provider._service)
    self.assertEqual(self._urns, provider._urns)
    self.assertEqual(None, provider._schema_transforms)

  def test_remote_provider_from_spec_fails_without_address(self):
    """Test that the spec must contain the address field."""
    spec = {'type': 'remote', 'transforms': self._urns}
    with self.assertRaises(KeyError):
      ExternalProvider.provider_from_spec(spec)


class ExternalJavaProviderTest(ExternalProviderTest):
  """Test class for ExternalJavaProvider tests."""
  def __init__(self, method_name="runExternalJavaProviderTest"):
    super().__init__(method_name)

    self._spec = {
        'type': 'beamJar',
        'gradle_target': 'path:to:external:jar',
        'version': '1.0.0',
        'transforms': self._urns
    }
    self.providers = {
        self._type: ExternalJavaProvider(self._spec, lambda: None),
        self._schema_type: ExternalJavaProvider(
            self._spec, lambda: self._schema_urn)
    }

  @mock.patch('subprocess.run', return_value=subprocess.CompletedProcess([], 0))
  def test_available(self, mock_subprocess):
    """Test that the provider is available when the java command is installed."""
    for provider in self.providers.values():
      self.assertTrue(provider.available())
    mock_subprocess.assert_called_with(['which', 'java'], capture_output=True)

  @mock.patch('subprocess.run', return_value=subprocess.CompletedProcess([], 1))
  def test_not_available(self, mock_subprocess):
    """Test that the provider is not available when the java command is not installed."""
    for provider in self.providers.values():
      self.assertFalse(provider.available())
    mock_subprocess.assert_called_with(['which', 'java'], capture_output=True)

  def test_schema_aware_transform(self):
    """Calls parent test with the JavaJarExpansionService overriden with a mock service."""
    external.JavaJarExpansionService = super().MockSchemaExpansionService
    super().test_schema_aware_transform()

  def java_provider_from_spec_asserts(self, provider):
    """Common asserts for testing parsing an ExternalJavaProvider from a spec file."""
    self.assertIsInstance(provider, ExternalJavaProvider)
    self.assertEqual(self._urns, provider._urns)
    self.assertEqual(None, provider._schema_transforms)
    self.assertIsInstance(provider._service(), JavaJarExpansionService)

  def test_java_jar_provider_from_spec_fails_without_jar_path(self):
    """Test that the spec must contain the jar path when parsing java jar."""
    spec = {'type': 'javaJar', 'transforms': self._urns}
    provider = ExternalProvider.provider_from_spec(spec)
    with self.assertRaises(KeyError):
      provider._service()

  def test_java_jar_provider_from_spec(self):
    """Test that provider is successfully parsed when spec is valid."""
    spec = {
        'type': 'javaJar',
        'jar': 'path/to/external.jar',
        'transforms': self._urns
    }
    provider = ExternalProvider.provider_from_spec(spec)
    self.java_provider_from_spec_asserts(provider)

  def test_beam_jar_provider_from_spec_service_fails_without_gradle_target(
      self):
    """Test that the spec must contain the gradle target when parsing beam jar."""
    spec = {'type': 'beamJar', 'transforms': self._urns}
    provider = ExternalProvider.provider_from_spec(spec)
    with self.assertRaises(TypeError):
      provider._service()

  def test_beam_jar_provider_from_spec(self):
    """Test that provider is successfully parsed when spec is valid."""
    spec = {
        'type': 'beamJar',
        'gradle_target': 'path:to:external:jar',
        'version': '1.0.0',
        'transforms': self._urns
    }
    provider = ExternalProvider.provider_from_spec(spec)
    self.java_provider_from_spec_asserts(provider)

  def test_maven_jar_provider_from_spec_service_fails_without_required_args(
      self):
    """Test that the spec must contain the required args when parsing maven jar."""
    spec = {'type': 'mavenJar', 'transforms': self._urns}
    provider = ExternalProvider.provider_from_spec(spec)
    with self.assertRaises(TypeError):
      provider._service()

  def test_maven_jar_provider_from_spec(self):
    """Test that provider is successfully parsed when spec is valid."""
    urns = {self._type: self._urn, self._schema_type: self._schema_urn}
    spec = {
        'type': 'mavenJar',
        'group_id': 'org.apache.beam',
        'artifact_id': 'test-library',
        'version': '1.0.0',
        'transforms': urns
    }
    provider = ExternalProvider.provider_from_spec(spec)
    self.java_provider_from_spec_asserts(provider)


class ExternalPythonProviderTest(ExternalProviderTest):
  """Test class for ExternalPythonProvider tests."""
  def __init__(self, method_name="runExternalPythonProviderTest"):
    super().__init__(method_name)

    self._urn = 'beam:transforms:python:fully_qualified_named'
    self._spec = {
        'type': 'pythonPackage',
        'transforms': self._urns,
        'packages': ['package']
    }
    self.providers = {self._type: ExternalPythonProvider(self._spec)}

  def test_available(self):
    """Test that the provider is always available since python must be available to run a yaml pipeline."""
    for provider in self.providers.values():
      self.assertTrue(provider.available())

  def test_schema_aware_transform(self):
    """ExternalPythonProvider does not have a SchemaAwareExternalTransform variant"""
    pass

  def test_provider_from_spec(self):
    """Test that provider is successfully parsed when spec is valid."""
    packages = ['package1', 'package2']
    spec = {
        'type': 'pythonPackage', 'transforms': self._urns, 'packages': packages
    }
    provider = ExternalProvider.provider_from_spec(spec)

    self.assertIsInstance(provider, ExternalPythonProvider)
    self.assertEqual(self._urns, provider._urns)
    self.assertEqual(None, provider._schema_transforms)
    self.assertIsInstance(provider._service, PypiExpansionService)
    self.assertEqual(packages, provider._service._packages)

  def test_affinity(self):
    """Extend the parent test with a specific assert for python v. inline provider."""
    super().test_affinity()
    for provider in self.providers.values():
      self.assertEqual(50, provider.affinity(InlineProvider(None)))


class ProviderUtilsTest(unittest.TestCase):
  """Test class for testing the various utility functions in yaml_provider.py"""
  def __init__(self, method_name='runProviderUtilsTest'):
    super().__init__(method_name)

  def test_as_provider_returns_given_provider(self):
    """Test that as_provider returns given provider when instance of Provider class."""
    provider = Provider()
    self.assertIs(provider, as_provider('MockProvider', provider))

  def test_as_provider_converts_constructor_to_provider(self):
    """Test that as_provider creates an InlineProvider when supplied a callable."""
    name = 'MockProvider'
    provider = as_provider(name, lambda fn: ParDo(fn))
    self.assertIsInstance(provider, InlineProvider)
    self.assertTrue(name in provider._transform_factories.keys())

  def test_as_provider_list_with_provider(self):
    """Test that as_provider_list converts given provider to singleton list containing the provider."""
    inline_provider = InlineProvider({'MockTransform': lambda fn: ParDo(fn)})
    lst = as_provider_list('MockTransform', inline_provider)

    self.assertEqual(len(lst), 1)
    self.assertIs(lst[0], inline_provider)

  def test_as_provider_list_with_provider_list(self):
    """Test that as_provider_list returns given provider list."""
    inline_provider = InlineProvider({'MockTransform': lambda fn: ParDo(fn)})
    external_provider = ExternalProvider(
        {'transforms': 'beam:external:service:MockBeamJarTransform'}, None)
    lst = as_provider_list(
        'MockTransform', [inline_provider, external_provider])

    self.assertEqual(len(lst), 2)
    self.assertIs(lst[0], inline_provider)
    self.assertIs(lst[1], external_provider)

  def test_parse_external_providers(self):
    """Test that parse_external_providers returns a dict containing the expected ExternalProvider subclasses."""
    specs = [
        {
            'type': 'beamJar',
            'gradle_target': 'path:to:external:jar',
            'version': '1.0.0',
            'transforms': {
                'MockBeamJarTransform': 'beam:external:service:MockBeamJarTransform'
            }
        },
        {
            'type': 'beamJar',
            'gradle_target': 'path:to:external:jar',
            'version': '2.0.0',
            'transforms': {
                'MockBeamJarTransform': 'beam:external:service:MockBeamJarTransform'
            }
        },
        {
            'type': 'mavenJar',
            'group_id': 'org.apache.beam',
            'artifact_id': 'test-library',
            'version': '1.0.0',
            'transforms': {
                'MockMavenJarTransform': 'beam:external:service:MockMavenJarTransform'
            }
        },
        {
            'type': 'remote',
            'address': '127.0.0.1:0000',
            'transforms': {
                'MockRemoteTransform': 'beam:external:service:MockRemoteTransform'
            }
        }
    ]
    providers = parse_external_providers(specs)

    self.assertEqual([
        'MockBeamJarTransform', 'MockMavenJarTransform', 'MockRemoteTransform'
    ],
                     list(providers.keys()))

    self.assertEqual(len(providers.get('MockBeamJarTransform')), 2)
    self.assertEqual(len(providers.get('MockMavenJarTransform')), 1)
    self.assertEqual(len(providers.get('MockRemoteTransform')), 1)

    self.assertIsInstance(
        providers.get('MockBeamJarTransform')[0], ExternalJavaProvider)
    self.assertIsInstance(
        providers.get('MockBeamJarTransform')[1], ExternalJavaProvider)
    self.assertIsInstance(
        providers.get('MockMavenJarTransform')[0], ExternalJavaProvider)
    self.assertIsInstance(
        providers.get('MockRemoteTransform')[0], RemoteProvider)

  def test_merge_providers(self):
    """Test that merge_providers successfully merges the given providers and dict of providers."""
    spec = {
        'transforms': {
            'MockExternal': 'beam:external:service:MockExternalTransform'
        }
    }
    external_provider = collections.defaultdict(list)
    external_provider['ExternalProvider'].append(ExternalProvider(spec, None))
    inline_provider = InlineProvider({
        'InlineProvider': lambda fn: beam.ParDo(fn),
        'AnotherInlineProvider': lambda fn: beam.ParDo(fn)
    })
    providers = merge_providers(inline_provider, external_provider)

    self.assertTrue(providers.get('InlineProvider')[0] is inline_provider)
    self.assertTrue(
        providers.get('AnotherInlineProvider')[0] is inline_provider)
    self.assertTrue(
        providers.get('ExternalProvider')[0] is external_provider.get(
            'ExternalProvider')[0])


class PypiExpansionServiceTest(unittest.TestCase):
  """Test class for the PypiExpansionService service class."""
  def __init__(self, method_name="runPypiExpansionServiceTest"):
    super().__init__(method_name)

    self._python_path = '/mock/path/to/python'
    self._venv_path = '/mock/path/to/venv'
    self._packages = ['package1', 'package2']
    self._pypi_service = PypiExpansionService(
        self._packages, base_python=self._python_path)

  def test_get_key(self):
    """Test that the service key is formatted correctly."""
    self.assertEqual({
        'binary': self._python_path, 'packages': self._packages
    },
                     json.loads(self._pypi_service._key()))

  @mock.patch('subprocess.run')
  def test_get_venv(self, mock_subprocess):
    """Test that the service successfully creates a venv when it does not exist."""
    subprocess.run = mock_subprocess

    with mock.patch('builtins.open', mock.mock_open()) as mock_open:
      venv = self._pypi_service._venv()

    self.assertRegexpMatches(
        venv, "%s/[0-9a-z]{64}" % PypiExpansionService.VENV_CACHE)
    self.assertEqual(
        '\n'.join(self._packages),
        mock_open.return_value.write.call_args.args[0])

    mock_open.assert_called_with(venv + '-requirements.txt', 'w')
    mock_subprocess.assert_called_with(
        [venv + '/bin/python', '-m', 'pip', 'install', *self._packages],
        check=True)

  @mock.patch('apache_beam.utils.subprocess_server.SubprocessServer')
  @mock.patch('subprocess.run')
  @mock.patch('os.path.exists', return_value=True)
  def test_context_manager(
      self, os_path_exists, mock_subprocess, mock_subprocess_server):
    """Test that the __enter__ and __exit__ methods work properly, and test that _venv returns existing venv."""
    with self._pypi_service as s:
      self.assertTrue(self._pypi_service._service is s)
    self.assertTrue(self._pypi_service._service is None)

    mock_subprocess_server.assert_called_once()
    mock_subprocess.assert_not_called()
    os_path_exists.assert_called()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
