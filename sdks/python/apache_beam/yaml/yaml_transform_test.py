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

import collections
import glob
import logging
import os
import tempfile
import unittest

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils import python_callable
from apache_beam.yaml import yaml_provider
from apache_beam.yaml.yaml_transform import YamlTransform


class CreateTimestamped(beam.PTransform):
  _yaml_requires_inputs = False

  def __init__(self, elements):
    self._elements = elements

  def expand(self, p):
    return (
        p
        | beam.Create(self._elements)
        | beam.Map(lambda x: beam.transforms.window.TimestampedValue(x, x)))


class CreateInts(beam.PTransform):
  _yaml_requires_inputs = False

  def __init__(self, elements):
    self._elements = elements

  def expand(self, p):
    return p | beam.Create(self._elements)


class SumGlobally(beam.PTransform):
  def expand(self, pcoll):
    return pcoll | beam.CombineGlobally(sum).without_defaults()


class SizeLimiter(beam.PTransform):
  def __init__(self, limit, error_handling):
    self._limit = limit
    self._error_handling = error_handling

  def expand(self, pcoll):
    def raise_on_big(row):
      if len(row.element) > self._limit:
        raise ValueError(row.element)
      else:
        return row.element

    good, bad = pcoll | beam.Map(raise_on_big).with_exception_handling()
    return {'small_elements': good, self._error_handling['output']: bad}


TEST_PROVIDERS = {
    'CreateInts': CreateInts,
    'CreateTimestamped': CreateTimestamped,
    'SumGlobally': SumGlobally,
    'SizeLimiter': SizeLimiter,
    'PyMap': lambda fn: beam.Map(python_callable.PythonCallableWithSource(fn)),
}


class YamlTransformE2ETest(unittest.TestCase):
  def test_composite(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create([1, 2, 3])
      # TODO(robertwb): Consider making the input implicit (and below).
      result = elements | YamlTransform(
          '''
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
            - type: Flatten
              input: [Square, Cube]
          output:
              Flatten
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([1, 4, 9, 1, 8, 27]))

  def test_chain_with_input(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(range(10))
      result = elements | YamlTransform(
          '''
          type: chain
          input:
              elements: input
          transforms:
            - type: PyMap
              config:
                  fn: "lambda x: x * x + x"
            - type: PyMap
              config:
                  fn: "lambda x: x + 41"
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([41, 43, 47, 53, 61, 71, 83, 97, 113, 131]))

  def test_chain_with_source_sink(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          source:
            type: CreateInts
            config:
                elements: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
          transforms:
            - type: PyMap
              config:
                  fn: "lambda x: x * x + x"
          sink:
            type: PyMap
            config:
                fn: "lambda x: x + 41"
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([41, 43, 47, 53, 61, 71, 83, 97, 113, 131]))

  def test_chain_with_root(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: CreateInts
              config:
                  elements: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            - type: PyMap
              config:
                  fn: "lambda x: x * x + x"
            - type: PyMap
              config:
                  fn: "lambda x: x + 41"
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([41, 43, 47, 53, 61, 71, 83, 97, 113, 131]))

  def create_has_schema(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: Create
              config:
                  elements: [{a: 1, b: 'x'}, {a: 2, b: 'y'}]
            - type: MapToFields
              config:
                  language: python
                  fields:
                      repeated: a * b
          ''') | beam.Map(lambda x: x.repeated)
      assert_that(result, equal_to(['x', 'yy']))

  def test_implicit_flatten(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: composite
          transforms:
            - type: Create
              name: CreateSmall
              config:
                  elements: [1, 2, 3]
            - type: Create
              name: CreateBig
              config:
                  elements: [100, 200]
            - type: PyMap
              input: [CreateBig, CreateSmall]
              config:
                  fn: "lambda x: x.element * x.element"
          output: PyMap
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([1, 4, 9, 10000, 40000]))

  def test_csv_to_json(self):
    try:
      import pandas as pd
    except ImportError:
      raise unittest.SkipTest('Pandas not available.')

    with tempfile.TemporaryDirectory() as tmpdir:
      data = pd.DataFrame([
          {
              'label': '11a', 'rank': 0
          },
          {
              'label': '37a', 'rank': 1
          },
          {
              'label': '389a', 'rank': 2
          },
      ])
      input = os.path.join(tmpdir, 'input.csv')
      output = os.path.join(tmpdir, 'output.json')
      data.to_csv(input, index=False)

      with beam.Pipeline() as p:
        result = p | YamlTransform(
            '''
            type: chain
            transforms:
              - type: ReadFromCsv
                config:
                    path: %s
              - type: WriteToJson
                config:
                    path: %s
                num_shards: 1
            ''' % (repr(input), repr(output)))

      output_shard = list(glob.glob(output + "*"))[0]
      result = pd.read_json(
          output_shard, orient='records',
          lines=True).sort_values('rank').reindex()
      pd.testing.assert_frame_equal(data, result)

  def test_circular_reference_validation(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      # pylint: disable=expression-not-assigned
      with self.assertRaisesRegex(ValueError, r'Circular reference detected.*'):
        p | YamlTransform(
            '''
                type: composite
                transforms:
                  - type: Create
                    name: Create
                    config:
                        elements: [0, 1, 3, 4]
                    input: Create
                  - type: PyMap
                    name: PyMap
                    config:
                        fn: "lambda row: row.element * row.element"
                    input: Create
                output: PyMap
                ''',
            providers=TEST_PROVIDERS)

  def test_circular_reference_multi_inputs_validation(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      # pylint: disable=expression-not-assigned
      with self.assertRaisesRegex(ValueError, r'Circular reference detected.*'):
        p | YamlTransform(
            '''
                  type: composite
                  transforms:
                    - type: Create
                      name: Create
                      config:
                        elements: [0, 1, 3, 4]
                    - type: PyMap
                      name: PyMap
                      config:
                        fn: "lambda row: row.element * row.element"
                      input: [Create, PyMap]
                  output: PyMap
                ''',
            providers=TEST_PROVIDERS)

  def test_name_is_not_ambiguous(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
            type: composite
            transforms:
              - type: Create
                name: Create
                config:
                    elements: [0, 1, 3, 4]
              - type: PyMap
                name: PyMap
                config:
                    fn: "lambda row: row.element * row.element"
                input: Create
            output: PyMap
            ''',
          providers=TEST_PROVIDERS)
      # No exception raised
      assert_that(result, equal_to([0, 1, 9, 16]))

  def test_name_is_ambiguous(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      # pylint: disable=expression-not-assigned
      with self.assertRaisesRegex(ValueError, r'Circular reference detected.*'):
        p | YamlTransform(
            '''
            type: composite
            transforms:
              - type: Create
                name: CreateData
                config:
                    elements: [0, 1, 3, 4]
              - type: PyMap
                name: PyMap
                config:
                    fn: "lambda elem: elem + 2"
                input: CreateData
              - type: PyMap
                name: AnotherMap
                config:
                    fn: "lambda elem: elem + 3"
                input: PyMap
            output: AnotherMap
            ''',
            providers=TEST_PROVIDERS)

  def test_empty_inputs_throws_error(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with self.assertRaisesRegex(ValueError,
                                  'Missing inputs for transform at '
                                  '"EmptyInputOkButYamlDoesntKnow" at line .*'):
        _ = p | YamlTransform(
            '''
            type: composite
            transforms:
              - type: PyTransform
                name: EmptyInputOkButYamlDoesntKnow
                config:
                  constructor: apache_beam.Impulse
            ''')

  def test_empty_inputs_ok_in_source(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      # Does not throw an error like it does above.
      _ = p | YamlTransform(
          '''
          type: composite
          source:
            type: PyTransform
            name: EmptyInputOkButYamlDoesntKnow
            config:
              constructor: apache_beam.Impulse
          ''')

  def test_empty_inputs_ok_if_explicit(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      # Does not throw an error like it does above.
      _ = p | YamlTransform(
          '''
          type: composite
          transforms:
            - type: PyTransform
              name: EmptyInputOkButYamlDoesntKnow
              input: {}
              config:
                constructor: apache_beam.Impulse
          ''')

  def test_annotations(self):
    t = LinearTransform(5, b=100)
    annotations = t.annotations()
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: Create
              config:
                elements: [0, 1, 2, 3]
            - type: %r
              config: %s
          ''' % (annotations['yaml_type'], annotations['yaml_args']))
      assert_that(result, equal_to([100, 105, 110, 115]))

  def test_resource_hints(self):
    t = LinearTransform(5, b=100)
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: Create
              config:
                elements: [0, 1, 2, 3]
            - type: MapToFields
              name: WithResourceHints
              config:
                language: python
                fields:
                  square: element * element
              resource_hints:
                min_ram: 1GB
          ''')
      assert_that(result | beam.Map(lambda x: x.square), equal_to([0, 1, 4, 9]))
    proto = p.to_runner_api()
    transform, = [
        t for t in proto.components.transforms.values()
        if t.unique_name == 'YamlTransform/Chain/WithResourceHints']
    self.assertEqual(
        proto.components.environments[transform.environment_id].
        resource_hints['beam:resources:min_ram_bytes:v1'],
        b'1000000000',
        proto)

  def test_composite_resource_hints(self):
    t = LinearTransform(5, b=100)
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: Create
              config:
                elements: [0, 1, 2, 3]
            - type: MapToFields
              name: WithInheritedResourceHints
              config:
                language: python
                fields:
                  square: element * element
          resource_hints:
            min_ram: 1GB
          ''')
      assert_that(result | beam.Map(lambda x: x.square), equal_to([0, 1, 4, 9]))
    proto = p.to_runner_api()
    transform, = [
        t for t in proto.components.transforms.values()
        if t.unique_name == 'YamlTransform/Chain/WithInheritedResourceHints']
    self.assertEqual(
        proto.components.environments[transform.environment_id].
        resource_hints['beam:resources:min_ram_bytes:v1'],
        b'1000000000',
        proto)

  def test_flatten_unifies_schemas(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      _ = p | YamlTransform(
          '''
            type: composite
            transforms:
              - type: Create
                name: Create1
                config:
                  elements:
                    - {ride_id: '1', passenger_count: 1}
                    - {ride_id: '2', passenger_count: 2}
              - type: Create
                name: Create2
                config:
                  elements:
                    - {ride_id: '3'}
                    - {ride_id: '4'}
              - type: Flatten
                input: [Create1, Create2]
              - type: AssertEqual
                input: Flatten
                config:
                  elements:
                    - {ride_id: '1', passenger_count: 1}
                    - {ride_id: '2', passenger_count: 2}
                    - {ride_id: '3'}
                    - {ride_id: '4'}
          ''')

  def test_flatten_unifies_optional_fields(self):
    """Test that Flatten correctly unifies schemas with optional fields."""
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      _ = p | YamlTransform(
          '''
            type: composite
            transforms:
              - type: Create
                name: Create1
                config:
                  elements:
                    - {id: '1', name: 'Alice', age: 30}
                    - {id: '2', name: 'Bob', age: 25}
              - type: Create
                name: Create2
                config:
                  elements:
                    - {id: '3', name: 'Charlie'}
                    - {id: '4', name: 'Diana'}
              - type: Flatten
                input: [Create1, Create2]
              - type: AssertEqual
                input: Flatten
                config:
                  elements:
                    - {id: '1', name: 'Alice', age: 30}
                    - {id: '2', name: 'Bob', age: 25}
                    - {id: '3', name: 'Charlie'}
                    - {id: '4', name: 'Diana'}
          ''')

  def test_flatten_unifies_different_types(self):
    """Test that Flatten correctly unifies schemas with different
    field types."""
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      _ = p | YamlTransform(
          '''
            type: composite
            transforms:
              - type: Create
                name: Create1
                config:
                  elements:
                    - {id: 1, value: 100}
                    - {id: 2, value: 200}
              - type: Create
                name: Create2
                config:
                  elements:
                    - {id: '3', value: 'text'}
                    - {id: '4', value: 'data'}
              - type: Flatten
                input: [Create1, Create2]
              - type: AssertEqual
                input: Flatten
                config:
                  elements:
                    - {id: 1, value: 100}
                    - {id: 2, value: 200}
                    - {id: '3', value: 'text'}
                    - {id: '4', value: 'data'}
          ''')

  def test_flatten_unifies_list_fields(self):
    """Test that Flatten correctly unifies schemas with list fields."""
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      _ = p | YamlTransform(
          '''
            type: composite
            transforms:
              - type: Create
                name: Create1
                config:
                  elements:
                    - {id: '1', tags: ['red', 'blue']}
                    - {id: '2', tags: ['green']}
              - type: Create
                name: Create2
                config:
                  elements:
                    - {id: '3', tags: ['yellow', 'purple', 'orange']}
                    - {id: '4', tags: []}
              - type: Flatten
                input: [Create1, Create2]
              - type: AssertEqual
                input: Flatten
                config:
                  elements:
                    - {id: '1', tags: ['red', 'blue']}
                    - {id: '2', tags: ['green']}
                    - {id: '3', tags: ['yellow', 'purple', 'orange']}
                    - {id: '4', tags: []}
          ''')

  def test_flatten_unifies_with_missing_fields(self):
    """Test that Flatten correctly unifies schemas when some inputs have
    missing fields."""
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      _ = p | YamlTransform(
          '''
            type: composite
            transforms:
              - type: Create
                name: Create1
                config:
                  elements:
                    - {id: '1', name: 'Alice', department: 'Engineering',
                       salary: 75000}
                    - {id: '2', name: 'Bob', department: 'Marketing',
                       salary: 65000}
              - type: Create
                name: Create2
                config:
                  elements:
                    - {id: '3', name: 'Charlie', department: 'Sales'}
                    - {id: '4', name: 'Diana'}
              - type: Flatten
                input: [Create1, Create2]
              - type: AssertEqual
                input: Flatten
                config:
                  elements:
                    - {id: '1', name: 'Alice', department: 'Engineering',
                       salary: 75000}
                    - {id: '2', name: 'Bob', department: 'Marketing',
                       salary: 65000}
                    - {id: '3', name: 'Charlie', department: 'Sales'}
                    - {id: '4', name: 'Diana'}
          ''')

  def test_flatten_unifies_complex_mixed_schemas(self):
    """Test that Flatten correctly unifies complex mixed
    schemas."""
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      _ = p | YamlTransform(
          '''
            type: composite
            transforms:
              - type: Create
                name: Create1
                config:
                  elements:
                    - {id: 1, name: 'Product A', price: 29.99,
                       categories: ['electronics', 'gadgets']}
                    - {id: 2, name: 'Product B', price: 15.50,
                       categories: ['books']}
              - type: Create
                name: Create2
                config:
                  elements:
                    - {id: 3, name: 'Product C', categories: ['clothing']}
                    - {id: 4, name: 'Product D', price: 99.99}
              - type: Create
                name: Create3
                config:
                  elements:
                    - {id: 5, name: 'Product E', price: 5.00,
                       categories: []}
              - type: Flatten
                input: [Create1, Create2, Create3]
              - type: AssertEqual
                input: Flatten
                config:
                  elements:
                    - {id: 1, name: 'Product A', price: 29.99,
                       categories: ['electronics', 'gadgets']}
                    - {id: 2, name: 'Product B', price: 15.50,
                       categories: ['books']}
                    - {id: 3, name: 'Product C', categories: ['clothing']}
                    - {id: 4, name: 'Product D', price: 99.99}
                    - {id: 5, name: 'Product E', price: 5.00,
                       categories: []}
          ''')

  def test_output_schema_success(self):
    """Test that optional output_schema works."""
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      _ = p | YamlTransform(
          '''
            type: composite
            transforms:
              - type: Create
                name: MyCreate
                config:
                  elements:
                    - {sdk: 'Beam', year: 2016}
                    - {sdk: 'Flink', year: 2015}
                  output_schema:
                    type: object
                    properties:
                      sdk: 
                        type: string
                      year: 
                        type: integer
              - type: AssertEqual
                name: CheckGood
                input: MyCreate
                config:
                  elements:
                    - {sdk: 'Beam', year: 2016}
                    - {sdk: 'Flink', year: 2015}
          ''')

  def test_output_schema_fails(self):
    """
    Test that optional output_schema works by failing the pipeline since main
    transform doesn't have error_handling config.
    """
    with self.assertRaises(Exception) as e:
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        _ = p | YamlTransform(
            '''
                type: composite
                transforms:
                  - type: Create
                    name: MyCreate
                    config:
                      elements:
                        - {sdk: 'Beam', year: 2016}
                        - {sdk: 'Spark', year: 'date'}
                        - {sdk: 'Flink', year: 2015}
                      output_schema:
                        type: object
                        properties:
                          sdk: 
                            type: string
                          year: 
                            type: integer
                  - type: AssertEqual
                    name: CheckGood
                    input: MyCreate
                    config:
                      elements:
                        - {sdk: 'Beam', year: 2016}
                        - {sdk: 'Flink', year: 2015}
              ''')
    self.assertIn("'date' is not of type 'integer'", str(e.exception))

  def test_output_schema_with_main_transform_error_handling_success(self):
    """Test that optional output_schema works in conjunction with main transform
    error handling."""
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      _ = p | YamlTransform(
          '''
            type: composite
            transforms:
              - type: Create
                name: CreateVisits
                config:
                  elements:
                    - {user: alice, timestamp: "not-valid"}
                    - {user: bob, timestamp: 3}
              - type: AssignTimestamps
                input: CreateVisits
                config:
                  timestamp: timestamp
                  error_handling:
                    output: invalid_rows
                  output_schema:
                    type: object
                    properties:
                      user:
                        type: string
                      timestamp:
                        type: boolean
              - type: MapToFields
                name: ExtractInvalidTimestamp
                input: AssignTimestamps.invalid_rows
                config:
                  language: python
                  fields:
                    user: "element.user"
                    timestamp: "element.timestamp"
              - type: AssertEqual
                input: ExtractInvalidTimestamp
                config:
                  elements:
                    - {user: "alice", timestamp: "not-valid"}
                    - {user: bob, timestamp: 3}
              - type: AssertEqual
                input: AssignTimestamps
                config:
                  elements: []
          ''')


class ErrorHandlingTest(unittest.TestCase):
  def test_error_handling_outputs(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: composite
          transforms:
            - type: Create
              config:
                  elements: ['a', 'b', 'biiiiig']
            - type: SizeLimiter
              input: Create
              config:
                  limit: 5
                  error_handling:
                    output: errors
            - name: TrimErrors
              type: PyMap
              input: SizeLimiter.errors
              config:
                  fn: "lambda x: x[1][1]"
          output:
            good: SizeLimiter
            bad: TrimErrors
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result['good'], equal_to(['a', 'b']), label="CheckGood")
      assert_that(result['bad'], equal_to(["ValueError('biiiiig')"]))

  def test_strip_error_metadata(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: composite
          transforms:
            - type: Create
              config:
                  elements: ['a', 'b', 'biiiiig']

            - type: SizeLimiter
              input: Create
              config:
                  limit: 5
                  error_handling:
                    output: errors
            - type: StripErrorMetadata
              name: StripErrorMetadata1
              input: SizeLimiter.errors

            - type: MapToFields
              input: Create
              config:
                  language: python
                  fields:
                    out: "1.0/(1-len(element))"
                  error_handling:
                    output: errors
            - type: StripErrorMetadata
              name: StripErrorMetadata2
              input: MapToFields.errors

          output:
            good: SizeLimiter
            bad1: StripErrorMetadata1
            bad2: StripErrorMetadata2
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result['good'], equal_to(['a', 'b']), label="CheckGood")
      assert_that(
          result['bad1'] | beam.Map(lambda x: x.element), equal_to(['biiiiig']))
      assert_that(
          result['bad2'] | beam.Map(lambda x: x.element), equal_to(['a', 'b']))

  def test_must_handle_error_output(self):
    with self.assertRaisesRegex(Exception, 'Unconsumed error output .*line 7'):
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        _ = p | YamlTransform(
            '''
            type: composite
            transforms:
              - type: Create
                config:
                    elements: ['a', 'b', 'biiiiig']
              - type: SizeLimiter
                input: Create
                config:
                    limit: 5
                    error_handling:
                      output: errors
            ''',
            providers=TEST_PROVIDERS)

  def test_mapping_errors(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: composite
          transforms:
            - type: Create
              config:
                  elements: [0, 1, 2, 4]
            - type: MapToFields
              name: ToRow
              input: Create
              config:
                  language: python
                  fields:
                      num: element
                      str: "'a' * element or 'bbb'"
            - type: Filter
              input: ToRow
              config:
                  language: python
                  keep:
                    str[1] >= 'a'
                  error_handling:
                    output: errors
            - type: MapToFields
              name: MapWithErrorHandling
              input: Filter
              config:
                  language: python
                  fields:
                    num: num
                    inverse: float(1 / num)
                  error_handling:
                    output: errors
            - type: PyMap
              name: TrimErrors
              input: [MapWithErrorHandling.errors, Filter.errors]
              config:
                  fn: "lambda x: x.msg"
            - type: MapToFields
              name: Sum
              input: MapWithErrorHandling
              config:
                  language: python
                  append: True
                  fields:
                    sum: num + inverse
          output:
            good: Sum
            bad: TrimErrors
          ''',
          providers=TEST_PROVIDERS)
      assert_that(
          result['good'],
          equal_to([
              beam.Row(num=2, inverse=.5, sum=2.5),
              beam.Row(num=4, inverse=.25, sum=4.25)
          ]),
          label="CheckGood")
      assert_that(
          result['bad'],
          equal_to([
              "IndexError('string index out of range')",  # from the filter
              "ZeroDivisionError('division by zero')",  # from the mapping
          ]),
          label='CheckErrors')


class YamlWindowingTest(unittest.TestCase):
  def test_explicit_window_into(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: CreateTimestamped
              config:
                  elements: [0, 1, 2, 3, 4, 5]
            - type: WindowInto
              windowing:
                type: fixed
                size: 4s
            - type: SumGlobally
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([6, 9]))

  def test_windowing_on_input(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: CreateTimestamped
              config:
                  elements: [0, 1, 2, 3, 4, 5]
            - type: SumGlobally
              windowing:
                type: fixed
                size: 4s
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([6, 9]))

  def test_windowing_multiple_inputs(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: composite
          transforms:
            - type: CreateTimestamped
              name: Create1
              config:
                  elements: [0, 2, 4]
            - type: CreateTimestamped
              name: Create2
              config:
                  elements: [1, 3, 5]
            - type: SumGlobally
              input: [Create1, Create2]
              windowing:
                type: fixed
                size: 4s
          output: SumGlobally
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([6, 9]))

  def test_windowing_on_output(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: CreateTimestamped
              config:
                  elements: [0, 1, 2, 3, 4, 5]
              windowing:
                type: fixed
                size: 4s
            - type: SumGlobally
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([6, 9]))

  def test_windowing_on_outer(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: CreateTimestamped
              config:
                  elements: [0, 1, 2, 3, 4, 5]
            - type: SumGlobally
          windowing:
            type: fixed
            size: 4s
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([6, 9]))

  def test_assign_timestamps(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: Create
              config:
                elements:
                  - {t: 1, v: 1}
                  - {t: 10, v: 2}
                  - {t: 11, v: 3}
            - type: AssignTimestamps
              config:
                timestamp: t
            - type: Combine
              config:
                group_by: []
                combine:
                  v: sum
              windowing:
                type: fixed
                size: 10s
          ''',
          providers=TEST_PROVIDERS)
      assert_that(
          result | beam.Map(lambda x: beam.Row(**x._asdict())),
          equal_to([
              beam.Row(v=1),
              beam.Row(v=5),
          ]))


class AnnotatingProvider(yaml_provider.InlineProvider):
  """A provider that vends transforms that do nothing but record that this
  provider (as identified by name) was used, along with any prior history
  of the given element.
  """
  def __init__(self, name, transform_names):
    super().__init__({
        transform_name: lambda: beam.Map(
            lambda x: (x if type(x) == tuple else ()) + (name, ))
        for transform_name in transform_names.strip().split()
    })
    self._name = name

  def __repr__(self):
    return 'AnnotatingProvider(%r)' % self._name


class AnotherAnnProvider(AnnotatingProvider):
  """A Provider that behaves exactly as AnnotatingProvider, but is not
  of the same type and so is considered "more distant" for matching purposes.
  """
  pass


class ProviderAffinityTest(unittest.TestCase):
  """These tests check that for a sequence of transforms, the "closest"
  proveders are chosen among multiple possible implementations.
  """
  provider1 = AnnotatingProvider("provider1", "P1 A B C  ")
  provider2 = AnnotatingProvider("provider2", "P2 A   C D")
  provider3 = AnotherAnnProvider("provider3", "P3 A B    ")
  provider4 = AnotherAnnProvider("provider4", "P4 A B   D")

  providers_dict = collections.defaultdict(list)
  for provider in [provider1, provider2, provider3, provider4]:
    for transform_type in provider.provided_transforms():
      providers_dict[transform_type].append(provider)

  def test_prefers_same_provider(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result1 = p | 'Yaml1' >> YamlTransform(
          '''
          type: chain
          transforms:
            - type: Create
              config:
                  elements: [0]
            - type: P1
            - type: A
            - type: C
          ''',
          providers=self.providers_dict)
      assert_that(
          result1,
          equal_to([(
              # provider1 was chosen, as it is the only one vending P1
              'provider1',
              # All of the providers vend A, but since the input was produced
              # by provider1, we prefer to use that again.
              'provider1',  # Similarly for C.
              'provider1')]),
          label='StartWith1')

      result2 = p | 'Yaml2' >> YamlTransform(
          '''
          type: chain
          transforms:
            - type: Create
              config:
                  elements: [0]
            - type: P2
            - type: A
            - type: C
          ''',
          providers=self.providers_dict)
      assert_that(
          result2,
          equal_to([(
              # provider2 was necessarily chosen for P2
              'provider2',  # Unlike above, we choose provider2 to implement A.
              'provider2',  # Likewise for C.
              'provider2')]),
          label='StartWith2')

  def test_prefers_same_provider_class(self):
    # Like test_prefers_same_provider, but as we cannot choose the same
    # exact provider, we go with the next closest (which is of the same type)
    # over an implementation from a Provider of a different type.
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result1 = p | 'Yaml1' >> YamlTransform(
          '''
          type: chain
          transforms:
            - type: Create
              config:
                  elements: [0]
            - type: P1
            - type: A
            - type: D
            - type: A
          ''',
          providers=self.providers_dict)
      assert_that(
          result1,
          equal_to([('provider1', 'provider1', 'provider2', 'provider2')]),
          label='StartWith1')

      result3 = p | 'Yaml2' >> YamlTransform(
          '''
          type: chain
          transforms:
            - type: Create
              config:
                  elements: [0]
            - type: P3
            - type: A
            - type: D
            - type: A
          ''',
          providers=self.providers_dict)
      assert_that(
          result3,
          equal_to([('provider3', 'provider3', 'provider4', 'provider4')]),
          label='StartWith3')


@beam.transforms.ptransform.annotate_yaml
class LinearTransform(beam.PTransform):
  """A transform used for testing annotate_yaml."""
  def __init__(self, a, b):
    self._a = a
    self._b = b

  def expand(self, pcoll):
    a = self._a
    b = self._b
    return pcoll | beam.Map(lambda x: a * x.element + b)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
