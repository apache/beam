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
import copy
import logging
import unittest

import yaml

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.yaml import YamlTransform
from apache_beam.yaml import yaml_provider
from apache_beam.yaml.yaml_provider import InlineProvider
from apache_beam.yaml.yaml_transform import SafeLineLoader
from apache_beam.yaml.yaml_transform import Scope
from apache_beam.yaml.yaml_transform import chain_as_composite
from apache_beam.yaml.yaml_transform import ensure_errors_consumed
from apache_beam.yaml.yaml_transform import ensure_transforms_have_types
from apache_beam.yaml.yaml_transform import expand_composite_transform
from apache_beam.yaml.yaml_transform import expand_leaf_transform
from apache_beam.yaml.yaml_transform import expand_pipeline
from apache_beam.yaml.yaml_transform import extract_name
from apache_beam.yaml.yaml_transform import identify_object
from apache_beam.yaml.yaml_transform import normalize_inputs_outputs
from apache_beam.yaml.yaml_transform import normalize_source_sink
from apache_beam.yaml.yaml_transform import only_element
from apache_beam.yaml.yaml_transform import pipeline_as_composite
from apache_beam.yaml.yaml_transform import preprocess_flattened_inputs
from apache_beam.yaml.yaml_transform import preprocess_windowing
from apache_beam.yaml.yaml_transform import push_windowing_to_roots


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
              fn: "lambda x: x * x"
            - type: PyMap
              name: Cube
              input: elements
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
    self.assertEqual(SafeLineLoader.get_line(spec['transforms'][1]), 10)
    self.assertEqual(SafeLineLoader.get_line(spec['output']), 15)
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


def new_pipeline():
  return beam.Pipeline(
      options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle'))


class MainTest(unittest.TestCase):
  def get_transform_by_type(self, result, type):
    return ([t for t in result['transforms'] if t['type'] == type][0])

  def get_scope_by_spec(self, p, spec, inputs=None):
    if inputs is None:
      inputs = {}
    spec = yaml.load(spec, Loader=SafeLineLoader)

    scope = Scope(
        beam.pvalue.PBegin(p),
        inputs,
        spec['transforms'],
        yaml_provider.standard_providers(), {})
    return scope, spec

  def test_expand_leaf_transform_with_input(self):
    with new_pipeline() as p:
      spec = '''
          transforms:
          - type: Create
            elements: [0]
          - type: PyMap
            input: Create
            fn: "lambda x: x*x"
          '''
      scope, spec = self.get_scope_by_spec(p, spec)
      result = expand_leaf_transform(spec['transforms'][1], scope)
      self.assertTrue('out' in result)
      self.assertIsInstance(result['out'], beam.PCollection)
      self.assertRegex(
          str(result['out'].producer.inputs[0]), r"PCollection.*Create/Map.*")

  def test_expand_leaf_transform_without_input(self):
    with new_pipeline() as p:
      spec = '''
          transforms:
          - type: Create
            elements: [0]
          '''
      scope, spec = self.get_scope_by_spec(p, spec)
      result = expand_leaf_transform(spec['transforms'][0], scope)
      self.assertTrue('out' in result)
      self.assertIsInstance(result['out'], beam.PCollection)

  def test_pipeline_as_composite_with_type_transforms(self):
    spec = '''
      type: composite
      transforms:
      - type: Create
        elements: [0,1,2]
      - type: PyMap
        fn: 'lambda x: x*x'
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    result = pipeline_as_composite(spec)

    self.assertEqual(result['type'], 'composite')
    self.assertEqual(result['name'], None)

  def test_pipeline_as_composite_with_transforms(self):
    spec = '''
      transforms:
      - type: Create
        elements: [0,1,2]
      - type: PyMap
        fn: 'lambda x: x*x'
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    result = pipeline_as_composite(spec)

    self.assertEqual(result['type'], 'composite')
    self.assertEqual(result['name'], None)

  def test_pipeline_as_composite_list(self):
    spec = '''
        - type: Create
          elements: [0,1,2]
        - type: PyMap
          fn: 'lambda x: x*x'
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    result = pipeline_as_composite(spec)

    self.assertEqual(result['type'], 'composite')
    self.assertEqual(result['name'], None)
    self.assertEqual(result['transforms'], spec)
    self.assertTrue('__line__' in result)
    self.assertTrue('__uuid__' in result)

  def test_expand_composite_transform_with_name(self):
    with new_pipeline() as p:
      spec = '''
        type: composite
        name: Custom
        transforms:
          - type: Create
            elements: [0,1,2]
        output: 
          Create
              
        '''
      scope, spec = self.get_scope_by_spec(p, spec)
      self.assertRegex(
          str(expand_composite_transform(spec, scope)['output']),
          r"PCollection.*Custom/Create/Map.*")

  def test_expand_composite_transform_with_name_input(self):
    with new_pipeline() as p:
      spec = '''
        type: composite
        input: elements
        transforms:
          - type: PyMap
            input: input
            fn: 'lambda x: x*x'
        output: 
          PyMap
        '''
      elements = p | beam.Create(range(3))
      scope, spec = self.get_scope_by_spec(p, spec,
                                           inputs={'elements': elements})
      self.assertRegex(
          str(expand_composite_transform(spec, scope)['output']),
          r"PCollection.*Composite/Map.*")

  def test_expand_composite_transform_root(self):
    with new_pipeline() as p:
      spec = '''
        type: composite
        transforms:
          - type: Create
            elements: [0,1,2]
        output: 
          Create
              
        '''
      scope, spec = self.get_scope_by_spec(p, spec)
      self.assertRegex(
          str(expand_composite_transform(spec, scope)['output']),
          r"PCollection.*Create/Map.*")

  def test_chain_as_composite(self):
    spec = '''
        type: chain
        transforms:
        - type: Create
          elements: [0,1,2]
        - type: PyMap
          fn: 'lambda x: x*x'
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    result = chain_as_composite(spec)
    self.assertEqual(result['type'], "composite")
    self.assertEqual(result['name'], "Chain")
    self.assertEqual(len(result['transforms']), 2)
    self.assertEqual(result['transforms'][0]["input"], {})
    self.assertEqual(
        result['transforms'][1]["input"], spec['transforms'][0]['__uuid__'])
    self.assertEqual(result['output'], spec['transforms'][1]['__uuid__'])

  def test_chain_as_composite_with_wrong_output_type(self):
    spec = '''
        type: chain
        transforms:
        - type: Create
          elements: [0,1,2]
        - type: PyMap
          fn: 'lambda x: x*x'
        output:
          Create
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    with self.assertRaisesRegex(ValueError, r"Explicit output.*last transform"):
      chain_as_composite(spec)

  def test_chain_as_composite_with_wrong_output_name(self):
    spec = '''
        type: chain
        transforms:
        - type: Create
          name: elements
          elements: [0,1,2]
        - type: PyMap
          fn: 'lambda x: x*x'
        output:
          elements
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    with self.assertRaisesRegex(ValueError, r"Explicit output.*last transform"):
      chain_as_composite(spec)

  def test_chain_as_composite_with_outputs_override(self):
    spec = '''
        type: chain
        transforms:
        - type: Create
          elements: [0,1,2]
        - type: PyMap
          fn: 'lambda x: x*x'
        output:
          PyMap
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    self.assertEqual(
        chain_as_composite(spec)['output']['output'],
        f"{spec['transforms'][1]['__uuid__']}.PyMap")

  def test_chain_as_composite_with_input(self):
    spec = '''
        type: chain
        input: 
          elements
        transforms:
        - type: PyMap
          fn: 'lambda x: x*x'
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    self.assertEqual(
        chain_as_composite(spec)['transforms'][0]['input'], {"input": "input"})

  def test_normalize_source_sink(self):
    spec = '''
        source:
          type: Create
          elements: [0,1,2]
        transforms:
        - type: PyMap
          fn: 'lambda x: x*x'
        sink:
          type: PyMap
          fn: "lambda x: x + 41"
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    result = normalize_source_sink(spec)
    self.assertTrue('source' not in result)
    self.assertTrue('sink' not in result)
    self.assertEqual(len(result['transforms']), 3)
    self.assertEqual(result['transforms'][0], spec['source'])
    self.assertEqual(result['transforms'][2], spec['sink'])

  def test_normalize_source_sink_only_source(self):
    spec = '''
        source:
          type: Create
          elements: [0,1,2]
        transforms:
        - type: PyMap
          fn: 'lambda x: x*x'
       
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    result = normalize_source_sink(spec)
    self.assertTrue('source' not in result)
    self.assertTrue('sink' not in result)
    self.assertEqual(len(result['transforms']), 2)
    self.assertEqual(result['transforms'][0], spec['source'])

  def test_normalize_source_sink_only_sink(self):
    spec = '''
        transforms:
        - type: PyMap
          fn: 'lambda x: x*x'
        sink:
          type: PyMap
          fn: "lambda x: x + 41"
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    result = normalize_source_sink(spec)
    self.assertTrue('source' not in result)
    self.assertTrue('sink' not in result)
    self.assertEqual(len(result['transforms']), 2)
    self.assertEqual(result['transforms'][1], spec['sink'])

  def test_normalize_source_sink_no_source_no_sink(self):
    spec = '''
        transforms:
        - type: PyMap
          fn: 'lambda x: x*x'
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    result = normalize_source_sink(spec)
    self.assertTrue('source' not in result)
    self.assertTrue('sink' not in result)
    self.assertEqual(len(result['transforms']), 1)

  def test_preprocess_source_sink_composite(self):
    spec = '''
      type: composite
      source:
          type: Create
          elements: [0,1,2]
      transforms:
      - type: PyMap
        fn: 'lambda x: x*x'
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    result = normalize_source_sink(spec)
    self.assertTrue('source' not in result)
    self.assertEqual(len(result['transforms']), 2)

  def test_preprocess_source_sink_chain(self):
    spec = '''
      type: chain
      source:
          type: Create
          elements: [0,1,2]
      transforms:
      - type: PyMap
        fn: 'lambda x: x*x'
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    result = normalize_source_sink(spec)
    self.assertTrue('source' not in result)
    self.assertEqual(len(result['transforms']), 2)

  def test_preprocess_source_sink_other(self):
    spec = '''
      - type: PyMap
        fn: 'lambda x: x*x'
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    self.assertCountEqual(normalize_source_sink(spec), spec)

  def test_normalize_inputs_outputs(self):
    spec = '''
        type: PyMap
        input: [Create1, Create2]
        fn: 'lambda x: x*x'
        output: Squared
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    result = normalize_inputs_outputs(spec)
    self.assertCountEqual(result['input'], {"input": ["Create1", "Create2"]})
    self.assertCountEqual(result['output'], {"output": "Squared"})

  def test_normalize_inputs_outputs_dict(self):
    spec = '''
        type: PyMap
        input: [Create1, Create2]
        fn: 'lambda x: x*x'
        output: 
          out1: Squared1
          out2: Squared2
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    result = normalize_inputs_outputs(spec)
    self.assertCountEqual(result['input'], {"input": ["Create1", "Create2"]})
    self.assertCountEqual(
        result['output'], {
            "out1": "Squared1", "out2": "Squared2"
        })
    self.assertTrue("__uuid__" not in result['output'])
    self.assertTrue("__line_" not in result['output'])

  def test_identify_object_with_name(self):
    spec = '''
      type: PyMap
      fn: 'lambda x: x*x'
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    self.assertRegex(identify_object(spec), r"PyMap.*[0-9]")

  def test_identify_object(self):
    spec = '''
      argument: PyMap
      fn: 'lambda x: x*x'
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    self.assertRegex(identify_object(spec), r"at.*[0-9]")

  def test_extract_name_by_type(self):
    spec = '''
      type: PyMap
      fn: 'lambda x: x*x'
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    self.assertEqual(extract_name(spec), "PyMap")

  def test_extract_name_by_id(self):
    spec = '''
      type: PyMap
      id: PyMapId
      fn: 'lambda x: x*x'
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    self.assertEqual(extract_name(spec), "PyMapId")

  def test_extract_name_by_name(self):
    spec = '''
      type: PyMap
      name: PyMapName
      fn: 'lambda x: x*x'
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    self.assertEqual(extract_name(spec), "PyMapName")

  def test_extract_name_no_name(self):
    spec = '''
      transforms:
      - arg: PyMap
        fn: 'lambda x: x*x'
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    self.assertEqual(extract_name(spec), "")

  def test_push_windowing_to_roots(self):
    spec = '''
      type: composite
      transforms:
      - type: Create
        elements: [0,1,2]
      - type: PyMap
        fn: 'lambda x: x*x'
        input: Create
      windowing:
        type: fixed
        size: 2
        
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    spec = normalize_inputs_outputs(spec)
    spec['transforms'] = [
        normalize_inputs_outputs(t) for t in spec['transforms']
    ]
    result = push_windowing_to_roots(spec)
    self.assertCountEqual(
        result['transforms'][0]['windowing'], spec['windowing'])
    self.assertEqual(result['transforms'][0]['__consumed_outputs'], {None})
    self.assertTrue('windowing' not in result['transforms'][1])
    self.assertTrue('__consumed_outputs' not in result['transforms'][1])

  def test_preprocess_windowing_custom_type(self):
    spec = '''
      type: composite
      transforms:
        - type: CreateTimestamped
          name: Create1
          elements: [0, 2, 4]
        - type: SumGlobally
          input: Create1
          windowing:
            type: fixed
            size: 4
      output: SumGlobally
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    spec = normalize_inputs_outputs(spec)
    spec['transforms'] = [
        normalize_inputs_outputs(t) for t in spec['transforms']
    ]

    spec_sum = spec['transforms'][1]
    # Pass a copy of spec_sum, because preprocess_windowing modifies
    # the dict (spec.pop('windowing'))
    result = preprocess_windowing(dict(spec_sum))
    # Get resulting WindowInto transform
    result_window = ([
        t for t in result['transforms'] if t['type'] == "WindowInto"
    ][0])

    # Get resulting SumGlobally transform
    result_sum = ([
        t for t in result['transforms'] if t['type'] == "SumGlobally"
    ][0])

    self.assertEqual(result['type'], "composite")
    self.assertEqual(result['name'], "SumGlobally")
    self.assertEqual(len(result['transforms']), 2)
    self.assertCountEqual(result['input'], spec_sum['input'])
    self.assertEqual(result['__line__'], spec_sum['__line__'])
    self.assertEqual(result['__uuid__'], spec_sum['__uuid__'])

    # SumGlobally's input is the output of WindowInto
    self.assertEqual(result_sum['input']['input'], result_window["__uuid__"])

    self.assertEqual(result_sum['type'], "SumGlobally")
    self.assertNotEqual(result_sum['__uuid__'], spec_sum['__uuid__'])
    self.assertNotIn('windowing', result_sum)
    self.assertIn('__line__', result_sum)
    self.assertIn('__uuid__', result_sum)

    self.assertEqual(result_window['type'], "WindowInto")
    self.assertEqual(result_window['name'], "WindowInto[input]")
    self.assertCountEqual(result_window['windowing'], spec_sum['windowing'])
    self.assertEqual(result_window['input'], "input")
    self.assertIn('__line__', result_window)
    self.assertIn('__uuid__', result_window)

  def test_preprocess_windowing_composite_with_windowing_outer(self):
    spec = '''
      type: composite
      transforms:
        - type: CreateTimestamped
          name: Create1
          elements: [0, 2, 4]
        - type: SumGlobally
          input: Create1
      windowing:
        type: fixed
        size: 4
      output: SumGlobally
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    spec = normalize_inputs_outputs(spec)
    spec['transforms'] = [
        normalize_inputs_outputs(t) for t in spec['transforms']
    ]

    # Pass a copy of spec_sum, because preprocess_windowing modifies
    # the dict (spec.pop('windowing'))
    result = preprocess_windowing(copy.deepcopy(spec))
    self.assertNotIn('windowing', result)
    self.assertCountEqual(
        spec['windowing'], result['transforms'][0]['windowing'])

  def test_preprocess_windowing_composite_with_windowing_on_input(self):
    spec = '''
      type: composite
      transforms:
        - type: CreateTimestamped
          name: Create1
          elements: [0, 2, 4]
        - type: SumGlobally
          input: Create1
          windowing:
            type: fixed
            size: 4
      output: SumGlobally
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    spec = normalize_inputs_outputs(spec)
    spec['transforms'] = [
        normalize_inputs_outputs(t) for t in spec['transforms']
    ]

    # Pass a copy of spec_sum, because preprocess_windowing modifies
    # the dict (spec.pop('windowing'))
    result = preprocess_windowing(copy.deepcopy(spec))
    self.assertCountEqual(spec, result)
    self.assertEqual(spec['transforms'], result['transforms'])

  def test_preprocess_windowing_other_type_with_no_inputs(self):
    spec = '''
      type: composite
      transforms:
        - type: CreateTimestamped
          name: Create1
          elements: [0, 2, 4]
          windowing:
            type: fixed
            size: 4
      output: CreateTimestamped
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    spec = normalize_inputs_outputs(spec)
    spec['transforms'] = [
        normalize_inputs_outputs(t) for t in spec['transforms']
    ]

    # Pass a copy of spec_sum, because preprocess_windowing modifies
    # the dict (spec.pop('windowing'))
    spec = preprocess_windowing(copy.deepcopy(spec))
    spec = spec['transforms'][0]
    result = preprocess_windowing(copy.deepcopy(spec))

    # Get resulting WindowInto transform
    result_window = self.get_transform_by_type(result, "WindowInto")

    # Get resulting SumGlobally transform
    result_create = self.get_transform_by_type(result, "CreateTimestamped")

    self.assertEqual(result['type'], "composite")
    self.assertEqual(result['name'], "Create1")
    self.assertEqual(len(result['transforms']), 2)
    self.assertEqual(result['output'], result_window['__uuid__'])
    self.assertEqual(result['__line__'], spec['__line__'])
    self.assertEqual(result['__uuid__'], spec['__uuid__'])

    # WindowInto's input is the output of CreateTimestamped
    self.assertEqual(result_window['input'], result_create["__uuid__"])

    self.assertEqual(result_create['type'], "CreateTimestamped")
    self.assertNotEqual(result_create['__uuid__'], spec['__uuid__'])
    self.assertNotIn('windowing', result_create)
    self.assertIn('__line__', result_create)
    self.assertIn('__uuid__', result_create)

    self.assertEqual(result_window['type'], "WindowInto")
    self.assertEqual(result_window['name'], "WindowInto[None]")
    self.assertCountEqual(result_window['windowing'], spec['windowing'])
    self.assertIn('__line__', result_window)
    self.assertIn('__uuid__', result_window)

  def test_preprocess_flattened_inputs_implicit(self):
    spec = '''
      type: composite
      transforms:
        - type: PyMap
          fn: 'lambda x: x*x'
          input: [Create1, Create2]
      output: CreateTimestamped
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    spec['transforms'] = [
        normalize_inputs_outputs(t) for t in spec['transforms']
    ]
    result = preprocess_flattened_inputs(copy.deepcopy(spec))
    self.assertEqual(len(result['transforms']), 2)
    result_flatten = self.get_transform_by_type(result, "Flatten")
    result_pymap = self.get_transform_by_type(result, "PyMap")

    self.assertDictEqual(
        result_flatten["input"], {
            'input0': 'Create1', 'input1': 'Create2'
        })
    self.assertEqual(result_flatten['type'], "Flatten")
    self.assertIn("Flatten", result_flatten['name'])
    self.assertIn("__line__", result_flatten)
    self.assertIn("__uuid__", result_flatten)

    self.assertEqual(result_pymap['input']['input'], result_flatten["__uuid__"])
    self.assertEqual(result_pymap['type'], "PyMap")
    self.assertEqual(result_pymap['fn'], 'lambda x: x*x')
    self.assertIn("__line__", result_pymap)
    self.assertIn("__uuid__", result_pymap)

  def test_preprocess_flattened_inputs_explicit_flatten(self):
    spec = '''
      type: composite
      transforms:
        - type: Flatten
          input: [Create1, Create2]
        - type: PyMap
          fn: 'lambda x: x*x'
          input: Flatten
      output: CreateTimestamped
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    spec['transforms'] = [
        normalize_inputs_outputs(t) for t in spec['transforms']
    ]
    result = preprocess_flattened_inputs(copy.deepcopy(spec))
    self.assertEqual(len(result['transforms']), 2)
    result_flatten = self.get_transform_by_type(result, "Flatten")
    result_pymap = self.get_transform_by_type(result, "PyMap")

    self.assertDictEqual(
        result_flatten["input"], {
            'input0': 'Create1', 'input1': 'Create2'
        })
    self.assertEqual(result_flatten['type'], "Flatten")
    self.assertIn("__line__", result_flatten)
    self.assertIn("__uuid__", result_flatten)

    self.assertCountEqual(result_pymap, spec['transforms'][1])

  def test_ensure_transforms_have_types(self):
    spec = '''
      type: PyMap
      fn: 'lambda x: x*x'
      input: Flatten
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    result = ensure_transforms_have_types(copy.deepcopy(spec))
    self.assertCountEqual(result, spec)

  def test_ensure_transforms_have_types_error(self):
    spec = '''
      name: PyMap
      fn: 'lambda x: x*x'
      input: Flatten
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    with self.assertRaisesRegex(ValueError, r"Missing type .*"):
      ensure_transforms_have_types(copy.deepcopy(spec))

  def test_ensure_errors_consumed_unconsumed(self):
    spec = '''
      type: composite
      transforms:
        - type: Create
          elements: [1,2,3]
        - type: MyTransform
          input: Create
          error_handling:
            output: errors
      output:
        good: MyTransform
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    spec = normalize_inputs_outputs(spec)
    spec['transforms'] = [
        normalize_inputs_outputs(t) for t in spec['transforms']
    ]
    with self.assertRaisesRegex(ValueError, r"Unconsumed error.*"):
      ensure_errors_consumed(spec)

  def test_ensure_errors_consumed_in_transform(self):
    spec = '''
      type: composite
      transforms:
        - type: Create
          elements: [1,2,3]
        - type: MyTransform
          input: Create
          error_handling:
            output: errors
        - name: SaveToFile
          type: PyMap
          input: MyTransform.errors
      output:
        good: MyTransform
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    spec = normalize_inputs_outputs(spec)
    spec['transforms'] = [
        normalize_inputs_outputs(t) for t in spec['transforms']
    ]
    result = ensure_errors_consumed(copy.deepcopy(spec))
    self.assertCountEqual(result, spec)
    self.assertCountEqual(result['transforms'], spec['transforms'])

  def test_ensure_errors_consumed_no_output_in_error_handling(self):
    spec = '''
      type: composite
      transforms:
        - type: Create
          elements: [1,2,3]
        - type: MyTransform
          input: Create
          error_handling:
            arg: errors
        - name: SaveToFile
          type: PyMap
          input: MyTransform.errors
      output:
        good: MyTransform
    '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    spec = normalize_inputs_outputs(spec)
    spec['transforms'] = [
        normalize_inputs_outputs(t) for t in spec['transforms']
    ]
    with self.assertRaisesRegex(ValueError, r"Missing output.*"):
      ensure_errors_consumed(spec)

  def test_expand_pipeline_with_string_spec(self):
    with new_pipeline() as p:
      spec = '''
        pipeline:
          type: chain
          transforms:
            - type: Create
              elements: [1,2,3]
            - type: PyMap
              fn: 'lambda x: x*x'
      '''
      result = expand_pipeline(p, spec)

      self.assertIsInstance(result, PCollection)
      self.assertEqual(str(result), 'PCollection[Map(lambda x: x*x).None]')

  def test_expand_pipeline_with_spec(self):
    with new_pipeline() as p:
      spec = '''
        pipeline:
          type: chain
          transforms:
            - type: Create
              elements: [1,2,3]
            - type: PyMap
              fn: 'lambda x: x*x'
      '''
      spec = yaml.load(spec, Loader=SafeLineLoader)
      result = expand_pipeline(p, spec)

      self.assertIsInstance(result, PCollection)
      self.assertEqual(str(result), 'PCollection[Map(lambda x: x*x).None]')

  def test_only_element(self):
    self.assertEqual(only_element((1, )), 1)


class YamlTransformTest(unittest.TestCase):
  def test_init_with_string(self):
    provider1 = InlineProvider({"MyTransform1": lambda: beam.Map(lambda x: x)})
    provider2 = InlineProvider({"MyTransform2": lambda: beam.Map(lambda x: x)})

    providers_dict = {"p1": [provider1], "p2": [provider2]}

    spec = '''
        type: chain
        transforms:
          - type: Create
            elements: [1,2,3]
          - type: PyMap
            fn: 'lambda x: x*x'
      '''
    result = YamlTransform(spec, providers_dict)
    self.assertIn('p1', result._providers)  # check for custom providers
    self.assertIn('p2', result._providers)  # check for custom providers
    self.assertIn('PyMap', result._providers)  # check for standard provider
    self.assertEqual(result._spec['type'], "composite")  # preprocessed spec

  def test_init_with_dict(self):
    spec = '''
        type: chain
        transforms:
          - type: Create
            elements: [1,2,3]
          - type: PyMap
            fn: 'lambda x: x*x'
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    result = YamlTransform(spec)
    self.assertIn('PyMap', result._providers)  # check for standard provider
    self.assertEqual(result._spec['type'], "composite")  # preprocessed spec

  def test_expand_pcollection(self):
    with new_pipeline() as p:
      spec = '''
          type: chain
          input:
            elements: input
          transforms:
          - type: PyMap
            fn: 'lambda x: x*x'
        '''
      spec = yaml.load(spec, Loader=SafeLineLoader)
      create = p | beam.Create([1, 2, 3])
      result = YamlTransform(spec).expand(create)
      self.assertIsInstance(result, PCollection)
      self.assertEqual(
          str(result), 'PCollection[Chain/Map(lambda x: x*x).None]')

  def test_expand_pbegin(self):
    with new_pipeline() as p:
      spec = '''
          type: chain
          transforms:
          - type: Create
            elements: [1,2,3]
          - type: PyMap
            fn: 'lambda x: x*x'
        '''
      spec = yaml.load(spec, Loader=SafeLineLoader)
      result = YamlTransform(spec).expand(beam.pvalue.PBegin(p))
      self.assertIsInstance(result, PCollection)
      self.assertEqual(
          str(result), 'PCollection[Chain/Map(lambda x: x*x).None]')

  def test_expand_dict(self):
    with new_pipeline() as p:
      spec = '''
          type: chain
          input:
              elements: input
          transforms:
          - type: PyMap
            fn: 'lambda x: x*x'
        '''
      spec = yaml.load(spec, Loader=SafeLineLoader)
      create = p | beam.Create([1, 2, 3])
      result = YamlTransform(spec).expand({'input': create})
      self.assertIsInstance(result, PCollection)
      self.assertEqual(
          str(result), 'PCollection[Chain/Map(lambda x: x*x).None]')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
