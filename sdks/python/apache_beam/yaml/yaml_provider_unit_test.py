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


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
