import unittest

import yaml

from apache_beam.yaml.yaml_utils import SafeLineLoader


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
