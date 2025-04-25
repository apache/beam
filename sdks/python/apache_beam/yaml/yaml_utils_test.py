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
import unittest

import yaml

from apache_beam.yaml.yaml_utils import SafeLineLoader
from apache_beam.yaml.yaml_utils import patch_yaml


class SafeLineLoaderTest(unittest.TestCase):
  def test_get_line(self):
    pipeline_yaml = '''
          type: composite
          input:
              elements: input
          transforms:
            - type: PyMap
              name: Square
              input: elements
              config:
                fn: "lambda x: x * x"
            - type: PyMap
              name: Cube
              input: elements
              config:
                fn: "lambda x: x * x * x"
          output:
              Flatten
          '''
    spec = yaml.load(pipeline_yaml, Loader=SafeLineLoader)
    self.assertEqual(SafeLineLoader.get_line(spec['type']), 2)
    self.assertEqual(SafeLineLoader.get_line(spec['input']), 4)
    self.assertEqual(SafeLineLoader.get_line(spec['transforms'][0]), 6)
    self.assertEqual(SafeLineLoader.get_line(spec['transforms'][0]['type']), 6)
    self.assertEqual(SafeLineLoader.get_line(spec['transforms'][0]['name']), 7)
    self.assertEqual(SafeLineLoader.get_line(spec['transforms'][1]), 11)
    self.assertEqual(SafeLineLoader.get_line(spec['output']), 17)
    self.assertEqual(SafeLineLoader.get_line(spec['transforms']), "unknown")

  def test_strip_metadata(self):
    spec_yaml = '''
    transforms:
      - type: PyMap
        name: Square
    '''
    spec = yaml.load(spec_yaml, Loader=SafeLineLoader)
    stripped = SafeLineLoader.strip_metadata(spec['transforms'])

    self.assertFalse(hasattr(stripped[0], '__line__'))
    self.assertFalse(hasattr(stripped[0], '__uuid__'))

  def test_strip_metadata_nothing_to_strip(self):
    spec_yaml = 'prop: 123'
    spec = yaml.load(spec_yaml, Loader=SafeLineLoader)
    stripped = SafeLineLoader.strip_metadata(spec['prop'])

    self.assertFalse(hasattr(stripped, '__line__'))
    self.assertFalse(hasattr(stripped, '__uuid__'))


_BASIC_YAML = '''

a: [1, 2, 3]
# comment

b:
  - 1
  - 2
  - 3

outer:
  - k1: v1
    k2: v2
    k3: [1, 2, {x: y}]
    k0: out_of_order
  - item

'''


class PatchYamlTest(unittest.TestCase):
  def test_unchanged(self):
    obj = yaml.load(_BASIC_YAML, Loader=yaml.SafeLoader)
    self.assertEqual(patch_yaml(_BASIC_YAML, obj), _BASIC_YAML)

  def test_inline_list(self):
    obj = yaml.load(_BASIC_YAML, Loader=yaml.SafeLoader)
    obj['a'] = [100, 200]
    self.assertEqual(
        patch_yaml(_BASIC_YAML, obj),
        _BASIC_YAML.replace('[1, 2, 3]', '[100, 200]'))

  def test_bullet_list(self):
    obj = yaml.load(_BASIC_YAML, Loader=yaml.SafeLoader)
    obj['b'] = [100, 200]
    self.assertEqual(
        patch_yaml(_BASIC_YAML, obj),
        _BASIC_YAML.replace('''
  - 1
  - 2
  - 3
''', '''
  - 100
  - 200
'''))

  def test_inline_map(self):
    obj = yaml.load(_BASIC_YAML, Loader=yaml.SafeLoader)
    obj['outer'][0]['k3'][-1] = 500
    self.assertEqual(
        patch_yaml(_BASIC_YAML, obj), _BASIC_YAML.replace('{x: y}', '500'))

  def test_multiline_map(self):
    obj = yaml.load(_BASIC_YAML, Loader=yaml.SafeLoader)
    obj['outer'][0]['k4'] = 500
    self.assertEqual(
        patch_yaml(_BASIC_YAML, obj),
        _BASIC_YAML.replace(
            '''
  - k1: v1
    k2: v2
    k3: [1, 2, {x: y}]
    k0: out_of_order
'''.strip(),
            '''
  - k1: v1
    k2: v2
    k3:
    - 1
    - 2
    - x: y
    k0: out_of_order
    k4: 500
'''.strip()))

  def test_inline_list_shorten(self):
    obj = yaml.load(_BASIC_YAML, Loader=yaml.SafeLoader)
    obj['a'] = [100, 2]
    self.assertEqual(
        patch_yaml(_BASIC_YAML, obj),
        _BASIC_YAML.replace('[1, 2, 3]', '[100, 2]'))

  def test_bullet_list_shorten(self):
    obj = yaml.load(_BASIC_YAML, Loader=yaml.SafeLoader)
    obj['b'] = [100, 2]
    self.assertEqual(
        patch_yaml(_BASIC_YAML, obj),
        _BASIC_YAML.replace(
            '''
  - 1
  - 2
  - 3
        '''.strip(),
            '''
  - 100
  - 2
        '''.strip()))

  def test_inline_list_extend(self):
    obj = yaml.load(_BASIC_YAML, Loader=yaml.SafeLoader)
    obj['a'] = [100, 2, 3, 4]
    self.assertEqual(
        patch_yaml(_BASIC_YAML, obj),
        _BASIC_YAML.replace('[1, 2, 3]', '[100, 2, 3, 4]'))

  def test_bullet_list_extend(self):
    obj = yaml.load(_BASIC_YAML, Loader=yaml.SafeLoader)
    obj['b'] = [100, 2, 3, 4]
    self.assertEqual(
        patch_yaml(_BASIC_YAML, obj),
        _BASIC_YAML.replace(
            '''
  - 1
  - 2
  - 3
        '''.strip(),
            '''
  - 100
  - 2
  - 3
  - 4
        '''.strip()))

  def test_bullet_list_map_extend(self):
    obj = yaml.load(_BASIC_YAML, Loader=yaml.SafeLoader)
    obj['outer'].append({'a': 'A', 'b': 'B'})
    self.assertEqual(
        patch_yaml(_BASIC_YAML, obj),
        _BASIC_YAML.replace(
            '''
  - item
        '''.strip(),
            '''
  - item
  - a: A
    b: B
        '''.strip()))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
