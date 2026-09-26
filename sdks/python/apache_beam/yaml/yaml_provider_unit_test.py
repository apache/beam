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

import logging
import os
import sys
import tempfile
import unittest

import mock
import yaml

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.yaml import yaml_provider
from apache_beam.yaml.yaml_provider import YamlProviders
from apache_beam.yaml.yaml_transform import SafeLineLoader
from apache_beam.yaml.yaml_transform import YamlTransform


class WindowIntoTest(unittest.TestCase):
  def __init__(self, methodName="runWindowIntoTest"):
    unittest.TestCase.__init__(self, methodName)
    self.parse_duration = YamlProviders.WindowInto._parse_duration

  def test_parse_duration_ms(self):
    value = self.parse_duration('1000ms', 'size')
    self.assertEqual(1, value)

  def test_parse_duration_sec(self):
    value = self.parse_duration('1s', 'size')
    self.assertEqual(1, value)

  def test_parse_duration_min(self):
    value = self.parse_duration('1m', 'size')
    self.assertEqual(60, value)

  def test_parse_duration_hour(self):
    value = self.parse_duration('1h', 'size')
    self.assertEqual(3600, value)

  def test_parse_duration_from_decimal(self):
    value = self.parse_duration('1.5m', 'size')
    self.assertEqual(90, value)

  def test_parse_duration_to_decimal(self):
    value = self.parse_duration('1ms', 'size')
    self.assertEqual(0.001, value)

  def test_parse_duration_with_missing_suffix(self):
    value = self.parse_duration('1', 'size')
    self.assertEqual(1, value)

  def test_parse_duration_with_invalid_suffix(self):
    with self.assertRaises(ValueError):
      self.parse_duration('1x', 'size')

  def test_parse_duration_with_missing_value(self):
    with self.assertRaises(ValueError):
      self.parse_duration('s', 'size')


class ProviderParsingTest(unittest.TestCase):

  INLINE_PROVIDER = {'type': 'TEST', 'name': 'INLINED'}
  INCLUDED_PROVIDER = {'type': 'TEST', 'name': 'INCLUDED'}
  EXTRA_PROVIDER = {'type': 'TEST', 'name': 'EXTRA'}

  @classmethod
  def setUpClass(cls):
    cls.tempdir = tempfile.TemporaryDirectory()
    cls.to_include = os.path.join(cls.tempdir.name, 'providers.yaml')
    with open(cls.to_include, 'w') as fout:
      yaml.dump([cls.INCLUDED_PROVIDER], fout)
    cls.to_include_nested = os.path.join(
        cls.tempdir.name, 'nested_providers.yaml')
    with open(cls.to_include_nested, 'w') as fout:
      yaml.dump([{'include': './providers.yaml'}, cls.EXTRA_PROVIDER], fout)

  @classmethod
  def tearDownClass(cls):
    cls.tempdir.cleanup()

  @mock.patch(
      'apache_beam.yaml.yaml_provider.ExternalProvider.provider_from_spec',
      lambda _, x: x)
  def test_include_file(self):
    flattened = [
        SafeLineLoader.strip_metadata(spec)
        for spec in yaml_provider.parse_providers(
            '', [
                self.INLINE_PROVIDER,
                {
                    'include': self.to_include
                }, ])
    ]

    self.assertEqual([
        self.INLINE_PROVIDER,
        self.INCLUDED_PROVIDER,
    ],
                     flattened)

  @mock.patch(
      'apache_beam.yaml.yaml_provider.ExternalProvider.provider_from_spec',
      lambda _, x: x)
  def test_include_url(self):
    flattened = [
        SafeLineLoader.strip_metadata(spec)
        for spec in yaml_provider.parse_providers(
            '', [
                self.INLINE_PROVIDER,
                {
                    'include': 'file:///' + self.to_include
                }, ])
    ]

    self.assertEqual([
        self.INLINE_PROVIDER,
        self.INCLUDED_PROVIDER,
    ],
                     flattened)

  @mock.patch(
      'apache_beam.yaml.yaml_provider.ExternalProvider.provider_from_spec',
      lambda _, x: x)
  def test_nested_include(self):
    flattened = [
        SafeLineLoader.strip_metadata(spec)
        for spec in yaml_provider.parse_providers(
            '', [
                self.INLINE_PROVIDER,
                {
                    'include': self.to_include_nested
                }, ])
    ]

    self.assertEqual([
        self.INLINE_PROVIDER,
        self.INCLUDED_PROVIDER,
        self.EXTRA_PROVIDER,
    ],
                     flattened)


class YamlDefinedProider(unittest.TestCase):
  def test_yaml_define_provider(self):
    providers = '''
    - type: yaml
      transforms:
        Range:
          config_schema:
            properties:
              end: {type: integer}
          requires_inputs: false
          body: |
            type: Create
            config:
              elements:
                {% for ix in range(end) %}
                - {{ix}}
                {% endfor %}
        Power:
          config_schema:
            properties:
              n: {type: integer}
          body:
            type: MapToFields
            config:
              language: python
              append: true
              fields:
                power: "element**{{n}}"
    '''

    pipeline = '''
    type: chain
    transforms:
      - type: Range
        config:
          end: 4
      - type: Power
        config:
          n: 2
    '''

    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          pipeline,
          providers=yaml_provider.parse_providers(
              '', yaml.load(providers, Loader=SafeLineLoader)))
      assert_that(
          result | beam.Map(lambda x: (x.element, x.power)),
          equal_to([(0, 0), (1, 1), (2, 4), (3, 9)]))

  def test_recursive(self):
    providers = '''
    - type: yaml
      transforms:
        Factorial:
          config_schema:
            properties:
              n: {type: integer}
          requires_inputs: false
          body: |
            {% if n <= 1 %}
              type: Create
              config:
                elements:
                  - {value: 1}
            {% else %}
              type: chain
              transforms:
                - type: Factorial
                  config:
                    n: {{n-1}}
                - type: MapToFields
                  name: Multiply
                  config:
                    language: python
                    fields:
                      value: value * {{n}}
            {% endif %}
    '''

    pipeline = '''
    type: Factorial
    config:
      n: 5
    '''

    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          pipeline,
          providers=yaml_provider.parse_providers(
              '', yaml.load(providers, Loader=SafeLineLoader)))
      assert_that(result | beam.Map(lambda x: x.value), equal_to([120]))


class PythonProviderDepsTest(unittest.TestCase):
  def test_env_package_sensitive(self):
    self.assertNotEqual(
        yaml_provider.PypiExpansionService._key('base', ['pkg1']),
        yaml_provider.PypiExpansionService._key('base', ['pkg2']))

  def test_env_base_sensitive(self):
    self.assertNotEqual(
        yaml_provider.PypiExpansionService._key('base1', ['pkg']),
        yaml_provider.PypiExpansionService._key('base2', ['pkg']))

  def test_env_order_invariant(self):
    self.assertEqual(
        yaml_provider.PypiExpansionService._key('base', ['pkg1', 'pkg2']),
        yaml_provider.PypiExpansionService._key('base', ['pkg2', 'pkg1']))

  def test_env_path_invariant(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      os.mkdir(os.path.join(tmpdir, 'a'))
      pkgA = os.path.join(tmpdir, 'a', 'pkg.tgz')
      os.mkdir(os.path.join(tmpdir, 'b'))
      pkgB = os.path.join(tmpdir, 'b', 'pkg.tgz')
      with open(pkgA, 'w') as fout:
        fout.write('content')
      with open(pkgB, 'w') as fout:
        fout.write('content')
      self.assertEqual(
          yaml_provider.PypiExpansionService._key('base', [pkgA]),
          yaml_provider.PypiExpansionService._key('base', [pkgB]))

  def test_env_content_sensitive(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      pkg = os.path.join(tmpdir, 'pkg.tgz')
      with open(pkg, 'w') as fout:
        fout.write('content')
      before = yaml_provider.PypiExpansionService._key('base', [pkg])
      with open(pkg, 'w') as fout:
        fout.write('new content')
      after = yaml_provider.PypiExpansionService._key('base', [pkg])
      self.assertNotEqual(before, after)


class JoinUrlOrFilepathTest(unittest.TestCase):
  def test_join_url_relative_path(self):
    self.assertEqual(
        yaml_provider._join_url_or_filepath('http://example.com/a', 'b/c.yaml'),
        'http://example.com/b/c.yaml')
    self.assertEqual(
        yaml_provider._join_url_or_filepath(
            'http://example.com/a/', 'b/c.yaml'),
        'http://example.com/a/b/c.yaml')

    # use os.path.join to mock gcs filesystem split and join.
    with mock.patch('apache_beam.io.filesystems.FileSystems.split',
                    new=lambda x:
                    ("gs://bucket", x.removeprefix("gs://bucket/"))):
      with mock.patch('apache_beam.io.filesystems.FileSystems.join',
                      new=lambda *args: '/'.join(args)):
        self.assertEqual(
            yaml_provider._join_url_or_filepath('gs://bucket', 'b/c.yaml'),
            'gs://bucket/b/c.yaml')
        self.assertEqual(
            yaml_provider._join_url_or_filepath('gs://bucket/', 'b/c.yaml'),
            'gs://bucket/b/c.yaml')
        self.assertEqual(
            yaml_provider._join_url_or_filepath('gs://bucket/a', 'b/c.yaml'),
            'gs://bucket/b/c.yaml')

  def test_join_filepath_relative_path(self):
    if sys.platform != 'win32':
      self.assertEqual(
          yaml_provider._join_url_or_filepath('/a/b/', 'c/d.yaml'),
          '/a/b/c/d.yaml')
      self.assertEqual(
          yaml_provider._join_url_or_filepath('/a/b', 'c/d.yaml'),
          '/a/c/d.yaml')
    else:
      self.assertEqual(
          yaml_provider._join_url_or_filepath('C:\\a\\b\\', 'c\\d.yaml'),
          'C:\\a\\b\\c\\d.yaml')
      self.assertEqual(
          yaml_provider._join_url_or_filepath('C:\\a\\b', 'c\\d.yaml'),
          'C:\\a\\c\\d.yaml')

  def test_absolute_path(self):
    self.assertEqual(
        yaml_provider._join_url_or_filepath(
            'gs://bucket/a', 'gs://bucket/b/c.yaml'),
        'gs://bucket/b/c.yaml')

    if sys.platform != 'win32':
      self.assertEqual(
          yaml_provider._join_url_or_filepath('/a/b', '/c/d.yaml'), '/c/d.yaml')

  def test_different_scheme(self):
    self.assertEqual(
        yaml_provider._join_url_or_filepath(
            'http://example.com/a', 'gs://bucket/b/c.yaml'),
        'gs://bucket/b/c.yaml')

  def test_empty_base(self):
    self.assertEqual(
        yaml_provider._join_url_or_filepath('', 'a/b.yaml'), 'a/b.yaml')
    self.assertEqual(
        yaml_provider._join_url_or_filepath(None, 'a/b.yaml'), 'a/b.yaml')


class YamlProvidersCreateTest(unittest.TestCase):
  def test_create_mixed_types(self):
    with beam.Pipeline() as p:
      # A mix of a primitive (Row(element=1)) and a dict (Row(a=2))
      result = p | YamlProviders.create([1, {"a": 2}])
      assert_that(
          result | beam.Map(lambda x: sorted(x._asdict().items())),
          equal_to([
              [('a', None), ('element', 1)],
              [('a', 2), ('element', None)],
          ]))


class YamlProvidersFlattenTest(unittest.TestCase):
  def test_flatten_schema_merging(self):
    with beam.Pipeline() as p:
      pcoll1 = p | 'C1' >> beam.Create([beam.Row(a=1)])
      pcoll2 = p | 'C2' >> beam.Create([beam.Row(b=2)])
      res = {
          'first': pcoll1, 'second': pcoll2
      } | yaml_provider.YamlProviders.Flatten()
      assert_that(
          res | beam.Map(lambda x: sorted(x._asdict().items())),
          equal_to([
              [('a', 1), ('b', None)],
              [('a', None), ('b', 2)],
          ]))

  def test_flatten_single_pcoll(self):
    with beam.Pipeline() as p:
      pcoll = p | beam.Create([beam.Row(a=1)])
      res = pcoll | yaml_provider.YamlProviders.Flatten()
      assert_that(res, equal_to([beam.Row(a=1)]))

  def test_flatten_empty(self):
    with beam.Pipeline() as p:
      res = p | yaml_provider.YamlProviders.Flatten()
      assert_that(res, equal_to([]))


class YamlProviderBaseAndHelpersTest(unittest.TestCase):
  def test_not_available_with_reason(self):
    n = yaml_provider.NotAvailableWithReason("reason")
    self.assertEqual(n.reason, "reason")
    self.assertFalse(bool(n))

  def test_provider_defaults(self):
    class MinimalProvider(yaml_provider.Provider):
      def available(self):
        return super().available()

      def cache_artifacts(self):
        return super().cache_artifacts()

      def provided_transforms(self):
        return super().provided_transforms()

      def create_transform(self, typ, args, spec):
        return super().create_transform(typ, args, spec)

      def _with_extra_dependencies(self, deps):
        return super()._with_extra_dependencies(deps)

    p = MinimalProvider()
    with self.assertRaises(NotImplementedError):
      p.available()
    with self.assertRaises(NotImplementedError):
      p.cache_artifacts()
    with self.assertRaises(NotImplementedError):
      p.provided_transforms()
    with self.assertRaises(NotImplementedError):
      p.create_transform("typ", {}, {})
    with self.assertRaises(ValueError):
      p._with_extra_dependencies(["dep"])

    self.assertIsNone(p.config_schema("typ"))
    self.assertIsNone(p.description("typ"))
    self.assertFalse(p.requires_inputs("ReadFromSource", {}))
    self.assertTrue(p.requires_inputs("typ", {}))
    self.assertIsNone(yaml_provider.InlineProvider({}).cache_artifacts())
    self.assertIsNone(
        yaml_provider.RemoteProvider({}, 'localhost:1234').cache_artifacts())

  def test_as_provider_list(self):
    p1 = yaml_provider.as_provider("t", lambda: None)
    self.assertIsInstance(p1, yaml_provider.InlineProvider)
    self.assertIs(yaml_provider.as_provider("t", p1), p1)

    lst1 = yaml_provider.as_provider_list("t", lambda: None)
    self.assertEqual(len(lst1), 1)
    lst2 = yaml_provider.as_provider_list("t", [p1])
    self.assertEqual(lst2, [p1])

  def test_merge_providers(self):
    p1 = yaml_provider.as_provider("t1", lambda: None)
    p2 = yaml_provider.as_provider("t2", lambda: None)
    res = yaml_provider.merge_providers(p1, [p2])
    self.assertIn("t1", res)
    self.assertIn("t2", res)

  def test_inline_provider_namespace(self):
    _ = yaml_provider.standard_inline_providers.Create
    with self.assertRaises(ValueError):
      _ = yaml_provider.standard_inline_providers.NonexistentTransform

  def test_renaming_provider(self):
    p = yaml_provider.InlineProvider({'T': lambda **kwargs: None})
    rp = yaml_provider.RenamingProvider(
        transforms={'MyT': 'T'},
        provider_base_path=None,
        mappings={'MyT': {
            'new_arg': 'x'
        }},
        underlying_provider=p,
        defaults={'MyT': {
            'def': 1
        }})
    self.assertTrue(rp.available())
    self.assertEqual(list(rp.provided_transforms()), ['MyT'])
    self.assertEqual(rp.underlying_provider(), p)
    self.assertIsNotNone(rp.config_schema('MyT'))
    self.assertIsNone(rp.description('MyT'))
    self.assertTrue(rp.requires_inputs('MyT', {}))
    _ = rp.create_transform('MyT', {'new_arg': 123}, lambda t, pcoll: None)

    # Verify that mappings must be a dictionary.
    with self.assertRaises(ValueError):
      yaml_provider.RenamingProvider({'MyT': 'T'}, None, 'not_dict', p)

    # Verify that string-based delegation mappings must point to a defined key.
    with self.assertRaises(ValueError):
      yaml_provider.RenamingProvider({'MyT': 'T'}, None, {'MyT': 'UnknownT'}, p)

    # Verify that every renamed transform must have an entry in the mappings dictionary.
    with self.assertRaises(ValueError):
      yaml_provider.RenamingProvider({'Missing': 'T'}, None, {}, p)


class WindowIntoTransformTest(unittest.TestCase):
  def test_window_into_types(self):
    from apache_beam.transforms import window

    p = yaml_provider.YamlProviders.WindowInto._parse_window_spec(
        {'type': 'global'})
    self.assertIsInstance(p.windowing.windowfn, window.GlobalWindows)

    p = yaml_provider.YamlProviders.WindowInto._parse_window_spec({
        'type': 'fixed', 'size': '10s'
    })
    self.assertIsInstance(p.windowing.windowfn, window.FixedWindows)

    p = yaml_provider.YamlProviders.WindowInto._parse_window_spec({
        'type': 'sliding', 'size': '10s', 'period': '5s'
    })
    self.assertIsInstance(p.windowing.windowfn, window.SlidingWindows)

    p = yaml_provider.YamlProviders.WindowInto._parse_window_spec({
        'type': 'sessions', 'gap': '10s'
    })
    self.assertIsInstance(p.windowing.windowfn, window.Sessions)

    with self.assertRaises(ValueError):
      yaml_provider.YamlProviders.WindowInto._parse_window_spec(
          {'type': 'unknown'})

    with self.assertRaises(ValueError):
      yaml_provider.YamlProviders.WindowInto._parse_window_spec({
          'type': 'global', 'extra': 123
      })


class LogForTestingTest(unittest.TestCase):
  def test_log_for_testing(self):
    with beam.Pipeline() as p:
      pcoll = p | beam.Create([1])
      output_pcoll = pcoll | yaml_provider.YamlProviders.log_for_testing(
          level='INFO', prefix='test:')

      # Extract the DoFn from the transform and run it directly in-process
      transform = output_pcoll.producer.transform
      dofn = transform.fn

      with self.assertLogs(level='INFO') as log:
        outputs = list(dofn.process(beam.Row(a=b'bytes_val', b=[1, 2])))

      self.assertEqual(outputs, [beam.Row(a=b'bytes_val', b=[1, 2])])
      self.assertIn(
          'INFO:root:test:{"a": "b\'bytes_val\'", "b": [1, 2]}', log.output)

  def test_log_for_testing_unknown_level(self):
    with beam.Pipeline() as p:
      pcoll = p | beam.Create([1])
      with self.assertRaises(ValueError):
        _ = pcoll | yaml_provider.YamlProviders.log_for_testing(level='UNKNOWN')


class PypiExpansionServiceTest(unittest.TestCase):
  @mock.patch('apache_beam.utils.subprocess_server.SubprocessServer')
  @mock.patch('subprocess.run')
  @mock.patch('os.path.exists')
  def test_pypi_expansion_service_venv_creation(
      self, mock_exists, mock_run, mock_server):
    mock_exists.return_value = False

    mock_clone = mock.MagicMock()
    mock_clone_module = mock.MagicMock()
    mock_clone_module.clone_virtualenv = mock_clone

    with mock.patch.dict('sys.modules', {'clonevirtualenv': mock_clone_module}):
      with mock.patch('builtins.open', mock.mock_open()):
        with yaml_provider.PypiExpansionService(['pkg1', 'pkg2']) as service:
          pass

    self.assertTrue(mock_clone.called)
    self.assertTrue(mock_run.called)

  @mock.patch('apache_beam.utils.subprocess_server.SubprocessServer')
  @mock.patch('os.environ.get')
  @mock.patch('os.path.exists')
  def test_pypi_expansion_service_dev_cloning(
      self, mock_exists, mock_env_get, mock_server):
    mock_exists.return_value = False
    mock_env_get.return_value = 'false'

    mock_clone = mock.MagicMock()
    mock_clone_module = mock.MagicMock()
    mock_clone_module.clone_virtualenv = mock_clone

    with mock.patch.dict('sys.modules', {'clonevirtualenv': mock_clone_module}):
      with mock.patch('builtins.open', mock.mock_open()):
        with mock.patch('subprocess.run') as mock_run:
          with mock.patch('apache_beam.yaml.yaml_provider.beam_version',
                          '2.50.0.dev'):
            with mock.patch('os.path.dirname') as mock_dirname:
              mock_dirname.return_value = '/path/to'
              with yaml_provider.PypiExpansionService(['pkg1']) as service:
                pass

    mock_clone.assert_called_once_with('/path/to', mock.ANY)


class ReshuffleTest(unittest.TestCase):
  def test_reshuffle(self):
    with beam.Pipeline() as p:
      res = p | beam.Create(
          [1, 2, 3]) | yaml_provider.YamlProviders.Reshuffle(num_buckets=1)
      assert_that(res, equal_to([1, 2, 3]))

  def test_python_provider_with_packages(self):
    p = yaml_provider.python(
        urns={}, provider_base_path='/tmp', packages=['pkg'])
    self.assertIsInstance(p, yaml_provider.ExternalPythonProvider)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
