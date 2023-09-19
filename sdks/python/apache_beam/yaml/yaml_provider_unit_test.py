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
import logging
import subprocess
import unittest
from urllib.parse import ParseResult

import mock

import apache_beam as beam
from apache_beam import ExternalTransform
from apache_beam import JavaJarExpansionService
from apache_beam import ParDo
from apache_beam import SchemaAwareExternalTransform
from apache_beam import beam_expansion_api_pb2
from apache_beam.version import __version__ as beam_version
from apache_beam.yaml.yaml_provider import ExternalJavaProvider
from apache_beam.yaml.yaml_provider import ExternalProvider
from apache_beam.yaml.yaml_provider import InlineProvider
from apache_beam.yaml.yaml_provider import MetaInlineProvider
from apache_beam.yaml.yaml_provider import Provider
from apache_beam.yaml.yaml_provider import PypiExpansionService
from apache_beam.yaml.yaml_provider import as_provider
from apache_beam.yaml.yaml_provider import as_provider_list
from apache_beam.yaml.yaml_provider import merge_providers
from apache_beam.yaml.yaml_provider import parse_providers

# pytype: skip-file


class _SomeTransform(beam.PTransform):
  def __init__(self, arg):
    self.arg = arg


class ProviderTest(unittest.TestCase):
  """
  Base class for test cases that involve creating a transform Provider for
  YAML pipelines.
  """
  def __init__(self, method_name='runProviderTest'):
    super().__init__(method_name)
    self.provider = Provider()

  def test_available(self):
    """Assert that the base method is not implemented"""
    with self.assertRaises(NotImplementedError):
      self.provider.available()

  def test_provided_transforms(self):
    """Assert that the base method is not implemented"""
    with self.assertRaises(NotImplementedError):
      self.provider.provided_transforms()

  def test_create_transform(self):
    """Assert that the base method is not implemented"""
    with self.assertRaises(NotImplementedError):
      self.provider.create_transform("", {}, lambda: beam.PTransform())

  def test_underlying_provider_returns_self(self):
    provider = Provider()
    self.assertIsInstance(provider.underlying_provider(), Provider)

  def test_affinity(self):
    """Test the affinity score of various provider comparisons"""
    provider = Provider()

    class AlternativeProvider(Provider):
      """Mock provider class for testing affinity"""
      pass

    self.assertEqual(200, provider.affinity(provider))
    self.assertEqual(20, provider.affinity(copy.copy(provider)))
    self.assertEqual(0, provider.affinity(AlternativeProvider()))


class InlineProviderTest(unittest.TestCase):
  """Test class for InlineProvider tests."""
  def __init__(self, method_name='runInlineProviderTest'):
    super().__init__(method_name)

  def test_available(self):
    """Test that the provider is available (Inline providers should always
    be available)."""
    self.assertTrue(InlineProvider({}).available())

  def test_provided_transforms(self):
    """Test that the provided transforms include the given transforms."""
    self.assertEqual(['SomeTransform', 'SomeOtherTransform'],
                     list(
                         InlineProvider({
                             'SomeTransform': lambda x: None,
                             'SomeOtherTransform': lambda y: None
                         }).provided_transforms()))

  def test_create_transform(self):
    """Test that the create_transform method creates the correct transform."""
    ptransform = InlineProvider({
        'SomeTransform': _SomeTransform
    }).create_transform('SomeTransform', {'arg': 'some_arg'}, lambda x: x)
    self.assertIsInstance(ptransform, _SomeTransform)
    self.assertEqual(ptransform.arg, 'some_arg')

  def test_create_transform_fails_when_called_with_invalid_transform(self):
    """Test that the create_transform method fails when an invalid transform
    is given."""
    provider = InlineProvider({'SomeTransform': _SomeTransform})
    with self.assertRaises(KeyError) as e:
      provider.create_transform(
          'NotMyTransform', {'arg': 'some_arg'}, lambda x: x)
    self.assertRegex(str(e.exception), ".*Invalid transform specified.*")

  def test_create_transform_fails_when_called_with_invalid_args(self):
    """Test that the create_transform method fails when invalid arguments are
    given."""
    provider = InlineProvider({'SomeTransform': _SomeTransform})
    with self.assertRaises(TypeError) as e:
      provider.create_transform(
          'SomeTransform', {'invalid': 'some_arg'}, lambda x: x)
    self.assertRegex(str(e.exception), ".*Invalid transform arguments.*")

  def test_to_json(self):
    """Test that the to_json method return correctly formatted JSON."""
    self.assertEqual(InlineProvider({}).to_json(), {'type': "InlineProvider"})


class MetaInlineProviderTest(unittest.TestCase):
  """Test class for MetaInlineProvider tests."""
  def __init__(self, method_name="runMetaInlineProviderTest"):
    super().__init__(method_name)

  def test_create_transform(self):
    """Test that the created transform is expanded from the meta transform."""
    class MyOuterTransform(beam.PTransform):
      def __init__(self, yaml_create_transform, *, arg):
        super().__init__()
        self.yaml_create_transform = yaml_create_transform
        self.arg = arg

      def expand(self, input_or_inputs):
        return self.yaml_create_transform(self.arg.replace('_', '_other_'))

    class MyInnerTransform(beam.PTransform):
      def __init__(self, arg):
        super().__init__()
        self.arg = arg

    def create_ptransform(arg):
      return MyInnerTransform(arg)

    provider = MetaInlineProvider({'MyTransform': MyOuterTransform})
    meta_ptransform = provider.create_transform(
        'MyTransform', {'arg': 'some_arg'}, create_ptransform)
    inner_ptransform = meta_ptransform.expand(None)

    self.assertIsInstance(meta_ptransform, MyOuterTransform)
    self.assertIsInstance(inner_ptransform, MyInnerTransform)
    self.assertEqual(meta_ptransform.arg, 'some_arg')
    self.assertEqual(inner_ptransform.arg, 'some_other_arg')


class ExternalProviderTest(unittest.TestCase):
  """Test class for ExternalProvider tests."""
  def __init__(self, method_name="runExternalProviderTest"):
    super().__init__(method_name)

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

  def test_provided_transforms(self):
    """Test that provided transforms are the given transforms"""
    self.assertEqual(
        ['SomeTransform', 'SomeOtherTransform'],
        list(
            ExternalProvider({
                'SomeTransform': 'beam:external:service:SomeTransform',
                'SomeOtherTransform': 'beam:external:service:SomeOtherTransform'
            },
                             lambda x: x).provided_transforms()))

  def test_create_transform(self):
    """Test that creating the transform correctly constructs an
    ExternalTransform"""
    ptransform = ExternalProvider({
        'SomeTransform': 'beam:external:service:SomeTransform',
        'SomeOtherTransform': 'beam:external:service:SomeOtherTransform'
    },
                                  lambda: None).create_transform(
                                      'SomeTransform', {'arg': 'some_arg'},
                                      lambda y: y)

    self.assertIsInstance(ptransform.transform, ExternalTransform)
    self.assertEqual(
        ptransform.transform._urn, 'beam:external:service:SomeTransform')
    self.assertEqual(ptransform.label, 'SomeTransform')

  def test_schema_aware_transform(self):
    """
    Test that creating the transform creates a SchemaAwareExternalTransform
    when supplied a schema-aware transform using the given expansion service.
    """
    ptransform = ExternalProvider(
        {
            'SomeTransform': 'beam:external:service:SomeTransform',
            'SomeOtherTransform': 'beam:external:service:SomeOtherTransform'
        },
        lambda: self.MockSchemaExpansionService(
            'beam:external:service:SomeTransform')).create_transform(
                'SomeTransform', {'arg': 'some_arg'}, lambda y: y)

    self.assertIsInstance(ptransform, SchemaAwareExternalTransform)
    self.assertIsInstance(
        ptransform._expansion_service, self.MockSchemaExpansionService)
    self.assertEqual(
        ptransform._expansion_service._schema_urn,
        'beam:external:service:SomeTransform')
    self.assertEqual(ptransform._kwargs['arg'], 'some_arg')

  def test_provider_from_spec_populates_beam_version(self):
    """Test that the version in the spec file is replaced when set to
    BEAM_VERSION."""
    spec = {
        'type': 'fakeType',
        'transforms': {
            'SomeTransform': 'beam:external:service:SomeTransform',
            'SomeOtherTransform': 'beam:external:service:SomeOtherTransform'
        },
        'version': 'BEAM_VERSION'
    }
    with self.assertRaises(ValueError) as e:
      ExternalProvider.provider_from_spec(spec)
      self.assertEqual(beam_version, spec['version'])
    self.assertRegex(str(e.exception), ".*Unexpected parameters in provider.*")

  # TODO - this test will need to be updated when docker support is added.
  def test_docker_provider_from_spec_throws_error(self):
    """Test that the docker provider is not implemented."""
    spec = {
        'type': 'docker',
        'transforms': {
            'SomeTransform': 'beam:external:service:SomeTransform'
        }
    }
    with self.assertRaises(ValueError) as e:
      ExternalProvider.provider_from_spec(spec)
    self.assertRegex(str(e.exception), ".*Unable to instantiate provider.*")

  def test_unknown_provider_from_spec_throws_error(self):
    """Test that unknown providers throw error."""
    spec = {
        'type': 'unknown',
        'transforms': {
            'SomeTransform': 'beam:external:service:SomeTransform'
        }
    }
    with self.assertRaises(NotImplementedError) as e:
      ExternalProvider.provider_from_spec(spec)
    self.assertRegex(str(e.exception), ".*Unknown provider type.*")

  def test_provider_from_spec_fails_without_type(self):
    """Test that providing a spec without a type throws an error."""
    spec = {
        'transforms': {
            'SomeTransform': 'beam:external:service:SomeTransform'
        },
        'SomeOtherTransform': 'beam:external:service:SomeOtherTransform'
    }
    with self.assertRaises(ValueError) as e:
      ExternalProvider.provider_from_spec(spec)
    self.assertRegex(str(e.exception), ".*Missing type in provider.*")


class RemoteProviderTest(unittest.TestCase):
  """Test class for RemoteProvider tests."""
  def __init__(self, method_name="runRemoteProviderTest"):
    super().__init__(method_name)
    self._type = "remote"

  @mock.patch('apache_beam.transforms.external.ExternalTransform.service')
  def test_available(self, mock_service):
    """Test that the provider is available when the service is ready."""
    provider = ExternalProvider.provider_types[self._type]({}, "")
    self.assertTrue(provider.available())
    mock_service().__enter__().ready.assert_called_once()

  @mock.patch('apache_beam.transforms.external.ExternalTransform.service')
  def test_not_available(self, mock_service):
    """Test that the provider is not available when the service is not ready."""
    mock_service().__enter__().ready.side_effect = Exception
    provider = ExternalProvider.provider_types[self._type]({}, "")

    self.assertFalse(provider.available())
    mock_service().__enter__().ready.assert_called_once()

  @mock.patch('apache_beam.transforms.external.ExternalTransform.service')
  def test_expansion_service_fails_to_connect(self, mock_service):
    """Test that the provider is not available when the service fails to
    connect."""
    mock_service.side_effect = Exception
    provider = ExternalProvider.provider_types[self._type]({}, "")

    self.assertFalse(provider.available())

  def test_remote_provider_from_spec(self):
    """Test that a remote provider can be parsed from a spec."""
    spec = {
        'type': 'remote',
        'config': {
            'address': '127.0.0.1:0000'
        },
        'transforms': {
            'SomeTransform': 'beam:external:service:SomeTransform',
            'SomeOtherTransform': 'beam:external:service:SomeOtherTransform'
        }
    }
    provider = ExternalProvider.provider_from_spec(spec)
    self.assertIsInstance(provider, ExternalProvider.provider_types[self._type])
    self.assertEqual('127.0.0.1:0000', provider._service)
    self.assertEqual({
        'SomeTransform': 'beam:external:service:SomeTransform',
        'SomeOtherTransform': 'beam:external:service:SomeOtherTransform'
    },
                     provider._urns)
    self.assertEqual(None, provider._schema_transforms)

  def test_remote_provider_from_spec_fails_without_address(self):
    """Test that the spec must contain the address field."""
    spec = {
        'type': 'remote',
        'transforms': {
            'SomeTransform': 'beam:external:service:SomeTransform'
        }
    }
    with self.assertRaises(ValueError) as e:
      ExternalProvider.provider_from_spec(spec)
    self.assertRegex(
        str(e.exception),
        ".*Unable to instantiate provider.*"
        "missing.*address.*")


class ExternalJavaProviderTest(unittest.TestCase):
  """Test class for ExternalJavaProvider tests."""
  def __init__(self, method_name="runExternalJavaProviderTest"):
    super().__init__(method_name)
    self._urns = {
        'SomeTransform': 'beam:external:service:SomeTransform',
        'SomeOtherTransform': 'beam:external:service:SomeOtherTransform'
    }

  @mock.patch('subprocess.run', return_value=subprocess.CompletedProcess([], 0))
  def test_available(self, mock_subprocess):
    """Test that the provider is available when the java command is
    installed."""
    self.assertTrue(ExternalJavaProvider({}, "").available())
    mock_subprocess.assert_called_with(['which', 'java'], capture_output=True)

  @mock.patch('subprocess.run', return_value=subprocess.CompletedProcess([], 1))
  def test_not_available(self, mock_subprocess):
    """Test that the provider is not available when the java command is not
    installed."""
    self.assertFalse(ExternalJavaProvider({}, "").available())
    mock_subprocess.assert_called_with(['which', 'java'], capture_output=True)

  def java_provider_from_spec_asserts(self, provider):
    """Common asserts for testing parsing an ExternalJavaProvider from a spec
    file."""
    self.assertIsInstance(provider, ExternalJavaProvider)
    self.assertEqual(self._urns, provider._urns)
    self.assertEqual(None, provider._schema_transforms)
    self.assertIsInstance(provider._service(), JavaJarExpansionService)

  def test_java_jar_provider_from_spec_fails_without_jar_path(self):
    """Test that the spec must contain the jar path when parsing java jar."""
    spec = {'type': 'javaJar', 'transforms': self._urns}
    with self.assertRaises(ValueError) as e:
      ExternalProvider.provider_from_spec(spec)
    self.assertRegex(
        str(e.exception), ".*Unable to instantiate provider.*missing.*jar.*")

  @mock.patch('urllib.parse.urlparse', return_value=ParseResult)
  def test_java_jar_provider_from_spec(self, mock_url_parser):
    """Test that provider is successfully parsed when spec is valid."""
    spec = {
        'type': 'javaJar',
        'config': {
            'jar': 'path/to/external.jar'
        },
        'transforms': self._urns
    }
    provider = ExternalProvider.provider_from_spec(spec)
    self.java_provider_from_spec_asserts(provider)
    mock_url_parser.assert_called_once()

  def test_beam_jar_provider_from_spec_service_fails_without_gradle_target(
      self):
    """Test that the spec must contain the gradle target when parsing beam
    jar."""
    spec = {
        'type': 'beamJar',
        'config': {
            'version': '1.0.0'
        },
        'transforms': self._urns
    }
    with self.assertRaises(ValueError):
      ExternalProvider.provider_from_spec(spec)

  def test_beam_jar_provider_from_spec(self):
    """Test that provider is successfully parsed when spec is valid."""
    spec = {
        'type': 'beamJar',
        'config': {
            'gradle_target': 'path:to:external:jar', 'version': '1.0.0'
        },
        'transforms': self._urns
    }
    provider = ExternalProvider.provider_from_spec(spec)
    self.java_provider_from_spec_asserts(provider)

  def test_maven_jar_provider_from_spec_service_fails_without_required_args(
      self):
    """Test that the spec must contain the required args when parsing maven
    jar."""
    spec = {'type': 'mavenJar', 'transforms': self._urns}
    with self.assertRaises(ValueError) as e:
      ExternalProvider.provider_from_spec(spec)
    self.assertRegex(
        str(e.exception),
        ".*Unable to instantiate provider.*"
        "missing.*keyword-only arguments.*"
        "artifact_id.*group_id.*version.*")

  def test_maven_jar_provider_from_spec(self):
    """Test that provider is successfully parsed when spec is valid."""
    spec = {
        'type': 'mavenJar',
        'config': {
            'group_id': 'org.apache.beam',
            'artifact_id': 'test-library',
            'version': '1.0.0'
        },
        'transforms': self._urns
    }
    provider = ExternalProvider.provider_from_spec(spec)
    self.java_provider_from_spec_asserts(provider)


class ExternalPythonProviderTest(unittest.TestCase):
  """Test class for ExternalPythonProvider tests."""
  def __init__(self, method_name="runExternalPythonProviderTest"):
    super().__init__(method_name)

    self._type = 'pythonPackage'

  def test_available(self):
    """Test that the provider is always available since python must be
    available to run a yaml pipeline."""
    self.assertTrue(
        ExternalProvider.provider_types[self._type]({}, "").available())

  def test_provider_from_spec(self):
    """Test that provider is successfully parsed when spec is valid."""
    packages = ['package1', 'package2']
    spec = {
        'type': 'pythonPackage',
        "config": {
            'packages': packages
        },
        'transforms': {
            'SomeTransform': 'beam.external.service.SomeTransform',
            'SomeOtherTransform': 'beam.external.service.SomeOtherTransform'
        },
    }
    provider = ExternalProvider.provider_from_spec(spec)

    self.assertIsInstance(provider, ExternalProvider.provider_types[self._type])
    self.assertEqual({
        'SomeTransform': 'beam.external.service.SomeTransform',
        'SomeOtherTransform': 'beam.external.service.SomeOtherTransform'
    },
                     provider._urns)
    self.assertEqual(None, provider._schema_transforms)
    self.assertIsInstance(provider._service, PypiExpansionService)
    self.assertEqual(packages, provider._service._packages)

  def test_affinity(self):
    """Extend the parent test with a specific assert for python v. inline
    provider."""
    provider = ExternalProvider.provider_types[self._type]({}, "")
    self.assertEqual(50, provider.affinity(InlineProvider(None)))

  def test_python_provider_returns_external_without_packages(self):
    provider = ExternalProvider.provider_types['python']({}, ["package"])
    self.assertIsInstance(provider, ExternalProvider.provider_types[self._type])

  def test_python_provider_returns_inline_without_packages(self):
    self.assertIsInstance(
        ExternalProvider.provider_types['python']({}), InlineProvider)


class RenamingProviderTest(unittest.TestCase):
  """Test class for InlineProvider tests."""
  def __init__(self, method_name='runRenamingProviderTest'):
    super().__init__(method_name)
    self._type = 'renaming'

  def test_renaming_provider_throws_error_when_transform_not_in_mapping(self):
    """Test that creating a RenamingProvider fails when the transforms do not
    include mappings."""
    with self.assertRaises(ValueError) as e:
      ExternalProvider.provider_types[self._type]({
          'SomeTransform': 'SomeTransform',
          'SomeOtherTransform': 'SomeOtherTransform'
      }, {},
                                                  InlineProvider({}))
    self.assertRegex(str(e.exception), '.*Missing transforms.*in mappings.*')

  def test_available(self):
    """Test that the provider is available (Inline providers should always
    be available)."""
    provider = ExternalProvider.provider_types[self._type]({}, {},
                                                           InlineProvider({}))
    self.assertTrue(provider.available())

  def test_provided_transforms(self):
    """Test that provided transforms are the given transforms"""
    provider = ExternalProvider.provider_types[self._type](
        {
            'SomeTransform': 'SomeTransform',
            'SomeOtherTransform': 'SomeOtherTransform'
        },
        {
            'SomeTransform': {'arg1', 'arg3'},
            'SomeOtherTransform': {
                'arg2': 'arg4'
            }
        },
        InlineProvider({}))
    self.assertEqual(['SomeTransform', 'SomeOtherTransform'],
                     list(provider.provided_transforms()))

  def test_create_transform(self):
    """Test that the create_transform method creates the correct transform."""
    provider = ExternalProvider.provider_types[self._type](
        {
            'MyTransform': 'SomeTransform',
            'MyOtherTransform': 'SomeOtherTransform'
        }, {
            'MyTransform': {
                'arg1': 'arg'
            },
            'MyOtherTransform': {
                'arg2': 'arg4'
            }
        },
        InlineProvider({'SomeTransform': _SomeTransform}))
    ptransform = provider.create_transform(
        'MyTransform', {'arg1': 'some_arg'}, lambda x: x)
    self.assertIsInstance(ptransform, _SomeTransform)
    self.assertEqual(ptransform.arg, 'some_arg')

  def test_affinity_is_not_implemented(self):
    """Test that calling affinity() throws exception."""
    with self.assertRaises(NotImplementedError) as e:
      ExternalProvider.provider_types[self._type]({}, {},
                                                  InlineProvider({}))._affinity(
                                                      Provider())
    self.assertEqual(
        str(e.exception),
        "Should not be calling _affinity "
        "directly on this provider.")

  def test_underlying_provider_is_correct_type(self):
    provider = ExternalProvider.provider_types[self._type]({}, {},
                                                           InlineProvider({}))
    self.assertIsInstance(provider.underlying_provider(), InlineProvider)


class ProviderUtilsTest(unittest.TestCase):
  """Test class for testing the various utility functions in yaml_provider.py"""
  def __init__(self, method_name='runProviderUtilsTest'):
    super().__init__(method_name)

  def test_as_provider_returns_given_provider(self):
    """Test that as_provider returns given provider when instance of Provider
    class."""
    provider = Provider()
    self.assertIs(provider, as_provider('MockProvider', provider))

  def test_as_provider_converts_constructor_to_provider(self):
    """Test that as_provider creates an InlineProvider when supplied a
    callable."""
    name = 'MockProvider'
    provider = as_provider(name, lambda fn: ParDo(fn))
    self.assertIsInstance(provider, InlineProvider)
    self.assertTrue(name in provider._transform_factories.keys())

  def test_as_provider_list_with_provider(self):
    """Test that as_provider_list converts given provider to singleton list
    containing the provider."""
    inline_provider = InlineProvider({'MockTransform': lambda fn: ParDo(fn)})
    lst = as_provider_list('MockTransform', inline_provider)

    self.assertEqual(len(lst), 1)
    self.assertIs(lst[0], inline_provider)

  def test_as_provider_list_with_provider_list(self):
    """Test that as_provider_list returns given provider list."""
    inline_provider = InlineProvider({'MockTransform': lambda fn: ParDo(fn)})
    external_provider = ExternalProvider(
        {'transforms': 'beam:external:service:BeamJarTransform'}, None)
    lst = as_provider_list(
        'MockTransform', [inline_provider, external_provider])

    self.assertEqual(len(lst), 2)
    self.assertIs(lst[0], inline_provider)
    self.assertIs(lst[1], external_provider)

  def test_parse_providers(self):
    """Test that parse_providers returns a dict containing the expected
    ExternalProvider subclasses."""
    specs = [
        {
            'type': 'beamJar',
            'config': {
                'gradle_target': 'path:to:external:jar', 'version': '1.0.0'
            },
            'transforms': {
                'BeamJarTransform': 'beam:external:service:BeamJarTransform'
            }
        },
        {
            'type': 'beamJar',
            'config': {
                'gradle_target': 'path:to:external:jar', 'version': '2.0.0'
            },
            'transforms': {
                'BeamJarTransform': 'beam:external:service:BeamJarTransform'
            }
        },
        {
            'type': 'mavenJar',
            'config': {
                'group_id': 'org.apache.beam',
                'artifact_id': 'test-library',
                'version': '1.0.0'
            },
            'transforms': {
                'MavenJarTransform': 'beam:external:service:MavenJarTransform'
            }
        },
        {
            'type': 'remote',
            'config': {
                'address': '127.0.0.1:0000'
            },
            'transforms': {
                'RemoteTransform': 'beam:external:service:RemoteTransform'
            }
        }
    ]
    providers = parse_providers(specs)

    self.assertEqual(
        ['BeamJarTransform', 'MavenJarTransform', 'RemoteTransform'],
        list(providers.keys()))

    self.assertEqual(len(providers.get('BeamJarTransform')), 2)
    self.assertEqual(len(providers.get('MavenJarTransform')), 1)
    self.assertEqual(len(providers.get('RemoteTransform')), 1)

    self.assertIsInstance(
        providers.get('BeamJarTransform')[0], ExternalJavaProvider)
    self.assertIsInstance(
        providers.get('BeamJarTransform')[1], ExternalJavaProvider)
    self.assertIsInstance(
        providers.get('MavenJarTransform')[0], ExternalJavaProvider)
    self.assertIsInstance(
        providers.get('RemoteTransform')[0],
        ExternalProvider.provider_types["remote"])

  def test_merge_providers(self):
    """Test that merge_providers successfully merges the given providers and
    dict of providers."""
    external_provider = collections.defaultdict(list)
    external_provider['ExternalProvider'].append(ExternalProvider({}, ""))
    inline_provider = InlineProvider({
        'InlineProvider': lambda fn: beam.ParDo(fn),
        'AnotherInlineProvider': lambda fn: beam.ParDo(fn)
    })
    meta_provider = MetaInlineProvider(
        {'InlineProvider': lambda fn: beam.ParDo(fn)})

    providers = merge_providers(
        inline_provider, external_provider, meta_provider)

    self.assertTrue(providers.get('InlineProvider')[0] is inline_provider)
    self.assertTrue(providers.get('InlineProvider')[1] is meta_provider)
    self.assertTrue(
        providers.get('AnotherInlineProvider')[0] is inline_provider)
    self.assertTrue(
        providers.get('ExternalProvider')[0] is external_provider.get(
            'ExternalProvider')[0])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
