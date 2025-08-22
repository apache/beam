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
import functools
import json
import random
import unittest
import uuid
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import TypeVar
from typing import Union

import yaml

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.yaml import yaml_provider
from apache_beam.yaml import yaml_transform
from apache_beam.yaml import yaml_utils


class YamlTestCase(unittest.TestCase):
  def __init__(self, pipeline_spec, test_spec, options, fix_tests):
    super().__init__()
    self._pipeline_spec = pipeline_spec
    self._test_spec = test_spec
    self._options = options
    self._fix_tests = fix_tests

  def runTest(self):
    self._fixes = run_test(
        self._pipeline_spec, self._test_spec, self._options, self._fix_tests)

  def fixed_test(self):
    fixed_test_spec = yaml_transform.SafeLineLoader.strip_metadata(
        self._test_spec)
    if self._fixes:
      expectation_by_id = {(loc, expectation['name']): expectation
                           for loc in ('expected_inputs', 'expected_outputs')
                           for expectation in fixed_test_spec.get(loc, [])}
      for name_loc, values in self._fixes.items():
        expectation_by_id[name_loc]['elements'] = sorted(values, key=json.dumps)
    return fixed_test_spec

  def id(self):
    return (
        self._test_spec.get('name', 'unknown') +
        f' (line {yaml_transform.SafeLineLoader.get_line(self._test_spec)})')

  def __str__(self):
    return self.id()


def run_test(pipeline_spec, test_spec, options=None, fix_failures=False):
  if isinstance(pipeline_spec, str):
    pipeline_spec = yaml.load(pipeline_spec, Loader=yaml_utils.SafeLineLoader)

  pipeline_spec = _preprocess_for_testing(pipeline_spec)

  transform_spec, recording_ids = inject_test_tranforms(
      pipeline_spec,
      test_spec,
      fix_failures)

  allowed_sources = set(test_spec.get('allowed_sources', []) + ['Create'])
  for transform in transform_spec['transforms']:
    name_or_type = transform.get('name', transform['type'])
    if (not yaml_transform.empty_if_explicitly_empty(transform.get('input', []))
        and not transform.get('name') in allowed_sources and
        not transform['type'] in allowed_sources):
      raise ValueError(
          f'Non-mocked source {name_or_type} '
          f'at line {yaml_transform.SafeLineLoader.get_line(transform)}')

  if options is None:
    options = beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle',
        **yaml_transform.SafeLineLoader.strip_metadata(
            pipeline_spec.get('options', {})))

  with beam.Pipeline(options=options) as p:
    _ = p | yaml_transform.YamlTransform(
        transform_spec,
        providers={'AssertEqualAndRecord': AssertEqualAndRecord})

  if fix_failures:
    fixes = {}
    for recording_id in recording_ids:
      if AssertEqualAndRecord.has_recorded_result(recording_id):
        fixes[recording_id[1:]] = [
            _try_row_as_dict(row)
            for row in AssertEqualAndRecord.get_recorded_result(recording_id)
        ]
        AssertEqualAndRecord.remove_recorded_result(recording_id)
    return fixes


def _preprocess_for_testing(pipeline_spec):
  spec = yaml_transform.pipeline_as_composite(pipeline_spec['pipeline'])
  # These are idempotent, so it's OK to do them preemptively.
  for phase in [
      yaml_transform.ensure_transforms_have_types,
      yaml_transform.preprocess_source_sink,
      yaml_transform.preprocess_chain,
      yaml_transform.tag_explicit_inputs,
      yaml_transform.normalize_inputs_outputs,
  ]:
    spec = yaml_transform.apply_phase(phase, spec)

  return spec


def validate_test_spec(test_spec):
  if not isinstance(test_spec, dict):
    raise TypeError(
        f'Test specification must be an object, got {type(test_spec)}')
  identifier = (
      test_spec.get('name', 'unknown') +
      f' at line {yaml_transform.SafeLineLoader.get_line(test_spec)}')

  if not isinstance(test_spec.get('allowed_sources', []), list):
    raise TypeError(
        f'allowed_sources of test specification {identifier} '
        f'must be a list, got {type(test_spec["allowed_sources"])}')

  if (not test_spec.get('expected_outputs', []) and
      not test_spec.get('expected_inputs', [])):
    raise ValueError(
        f'test specification {identifier} '
        f'must have at least one expected_outputs or expected_inputs')

  unknown_attrs = set(
      yaml_transform.SafeLineLoader.strip_metadata(test_spec).keys()) - set([
          'name',
          'mock_inputs',
          'mock_outputs',
          'expected_outputs',
          'expected_inputs',
          'allowed_sources',
      ])
  if unknown_attrs:
    raise ValueError(
        f'test specification {identifier} '
        f'has unknown attributes {list(unknown_attrs)}')

  for attr_type in ('mock_inputs',
                    'mock_outputs',
                    'expected_outputs',
                    'expected_inputs'):
    attr = test_spec.get(attr_type, [])
    if not isinstance(attr, list):
      raise TypeError(
          f'{attr_type} of test specification {identifier} '
          f'must be a list, got {type(attr_type)}')
    for ix, attr_item in enumerate(attr):
      if not isinstance(attr_item, dict):
        raise TypeError(
            f'{attr_type} {ix} of test specification {identifier} '
            f'must be an object, got {type(attr_item)}')
      if 'name' not in attr_item:
        raise TypeError(
            f'{attr_type} {ix} of test specification {identifier} '
            f'missing a name')
      if 'elements' not in attr_item:
        raise TypeError(
            f'{attr_type} {ix} of test specification {identifier} '
            f'missing a elements')
      if not isinstance(attr_item['elements'], list):
        raise TypeError(
            f'{attr_type} {ix} of test specification {identifier} '
            f'must be a list, got {type(attr_item["elements"])}')


def inject_test_tranforms(spec, test_spec, fix_failures):
  validate_test_spec(test_spec)
  scope = yaml_transform.LightweightScope(spec['transforms'])

  mocked_inputs_by_id = {
      scope.get_transform_id(mock_input['name']): mock_input
      for mock_input in test_spec.get('mock_inputs', [])
  }

  mocked_outputs_by_id = _composite_key_to_nested({
      scope.get_transform_id_and_output_name(mock_output['name']): mock_output
      for mock_output in test_spec.get('mock_outputs', [])
  })

  recording_id_prefix = str(uuid.uuid4())
  recording_ids = []

  transforms = []

  @functools.cache
  def create_inputs(transform_id: str) -> InputsType:
    def require_output_or_outputs(name_or_names):
      if isinstance(name_or_names, str):
        return require_output(name_or_names)
      else:
        return [require_output(name) for name in name_or_names]

    if transform_id in mocked_inputs_by_id:
      return create_mocked_input(transform_id)
    else:
      input_spec = scope.get_transform_spec(transform_id)['input']
      return {
          tag: require_output_or_outputs(input_ref)
          for tag, input_ref in yaml_transform.empty_if_explicitly_empty(
              input_spec).items()
      }

  def require_output(name: str) -> str:
    # The same output may be referenced under different names.
    # Normalize before we cache.
    transform_id, tag = scope.get_transform_id_and_output_name(name)
    return _require_output(transform_id, tag) or name

  @functools.cache
  def _require_output(transform_id: str, tag: str) -> Optional[str]:
    if transform_id in mocked_outputs_by_id:
      if tag not in mocked_outputs_by_id[transform_id]:
        name = next(iter(
            mocked_outputs_by_id[transform_id].values()))['name'].split('.')[0]
        raise ValueError(
            f'Unmocked output {tag} of {name}.'
            'If any used output is mocked all used outputs must be mocked.')
      return create_mocked_output(transform_id, tag)
    else:
      _use_transform(transform_id)
      return None  # Use original name.

  @functools.cache
  def _use_transform(transform_id: str) -> None:
    transform_spec = dict(scope.get_transform_spec(transform_id))
    transform_spec['input'] = create_inputs(transform_id)
    transforms.append(transform_spec)

  @functools.cache
  def create_mocked_input(transform_id: str) -> str:
    transform = create_create(
        f'MockInput[{mocked_inputs_by_id[transform_id]["name"]}]',
        mocked_inputs_by_id[transform_id]['elements'],
        mocked_inputs_by_id[transform_id]['name'])
    transforms.append(transform)
    return transform['__uuid__']

  @functools.cache
  def create_mocked_output(transform_id: str, tag: str) -> str:
    transform = create_create(
        f'MockOutput[{mocked_outputs_by_id[transform_id][tag]["name"]}]',
        mocked_outputs_by_id[transform_id][tag]['elements'],
        mocked_outputs_by_id[transform_id][tag]['name'])
    transforms.append(transform)
    return transform['__uuid__']

  def create_create(name, elements, line_source):
    return {
        '__uuid__': yaml_utils.SafeLineLoader.create_uuid(),
        '__line__': yaml_utils.SafeLineLoader.get_line(line_source),
        'name': name,
        'type': 'Create',
        'config': {
            'elements': elements,
        },
    }

  def create_assertion(name, inputs, elements, recording_id, line_source):
    return {
        '__uuid__': yaml_utils.SafeLineLoader.create_uuid(),
        '__line__': yaml_utils.SafeLineLoader.get_line(line_source),
        'name': name,
        'input': inputs,
        'type': 'AssertEqualAndRecord',
        'config': {
            'elements': elements,
            'recording_id': recording_id,
        },
    }

  for expected_output in test_spec.get('expected_outputs', []):
    if fix_failures:
      recording_id = (
          recording_id_prefix, 'expected_outputs', expected_output['name'])
      recording_ids.append(recording_id)
    else:
      recording_id = None
    require_output(expected_output['name'])
    transforms.append(
        create_assertion(
            f'CheckExpectedOutput[{expected_output["name"]}]',
            expected_output['name'],
            expected_output['elements'],
            recording_id,
            expected_output['name']))

  for expected_input in test_spec.get('expected_inputs', []):
    if fix_failures:
      recording_id = (
          recording_id_prefix, 'expected_inputs', expected_input['name'])
      recording_ids.append(recording_id)
    else:
      recording_id = None
    transform_id = scope.get_transform_id(expected_input['name'])
    transforms.append(
        create_assertion(
            f'CheckExpectedInput[{expected_input["name"]}]',
            create_inputs(transform_id),
            expected_input['elements'],
            recording_id,
            expected_input['name']))

  return {
      '__uuid__': yaml_utils.SafeLineLoader.create_uuid(),
      '__line__': 0,
      'type': 'composite',
      'transforms': transforms,
  }, recording_ids


class AssertEqualAndRecord(beam.PTransform):
  _recorded_results = {}

  @classmethod
  def store_recorded_result(cls, recording_id, value):
    assert recording_id not in cls._recorded_results
    cls._recorded_results[recording_id] = value

  @classmethod
  def has_recorded_result(cls, recording_id):
    return recording_id in cls._recorded_results

  @classmethod
  def get_recorded_result(cls, recording_id):
    return cls._recorded_results[recording_id]

  @classmethod
  def remove_recorded_result(cls, recording_id):
    del cls._recorded_results[recording_id]

  def __init__(self, elements, recording_id):
    self._elements = elements
    self._recording_id = recording_id

  def expand(self, pcoll):
    # Convert elements to rows outside the matcher to avoid capturing
    # any grpc channels that might be created during the conversion
    expected_rows = yaml_provider.dicts_to_rows(self._elements)
    recording_id = self._recording_id

    # Create a serializable matcher function that doesn't capture
    # any external references that might contain grpc channels
    class SerializableMatcher:
      def __init__(self, expected_rows, recording_id):
        self.expected_rows = expected_rows
        self.recording_id = recording_id
        self.equal_to_matcher = equal_to(expected_rows)

      def __call__(self, actual):
        try:
          self.equal_to_matcher(actual)
        except Exception:
          if self.recording_id:
            AssertEqualAndRecord.store_recorded_result(
                tuple(self.recording_id), actual)
          else:
            raise

    matcher = SerializableMatcher(expected_rows, recording_id)
    return assert_that(
        pcoll | beam.Map(lambda row: beam.Row(**row._asdict())), matcher)


def create_test(
    pipeline_spec, options=None, max_num_inputs=40, min_num_outputs=3):
  if isinstance(pipeline_spec, str):
    pipeline_spec = yaml.load(pipeline_spec, Loader=yaml_utils.SafeLineLoader)

  transform_spec = _preprocess_for_testing(pipeline_spec)

  if options is None:
    options = beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle',
        **yaml_transform.SafeLineLoader.strip_metadata(
            pipeline_spec.get('options', {})))

  def get_name(transform):
    if 'name' in transform:
      return str(transform['name'])
    else:
      if sum(1 for t in transform_spec['transforms']
             if t['type'] == transform['type']) > 1:
        raise ValueError('Ambiguous unnamed transform {transform["type"]}')
      return str(transform['type'])

  input_transforms = [
      t for t in transform_spec['transforms'] if t['type'] != 'Create' and
      not yaml_transform.empty_if_explicitly_empty(t.get('input', []))
  ]

  mock_outputs = [{
      'name': get_name(t),
      'elements': [
          _try_row_as_dict(row) for row in _first_n(t, options, max_num_inputs)
      ],
  } for t in input_transforms]

  output_transforms = [
      t for t in transform_spec['transforms'] if t['type'] == 'LogForTesting' or
      yaml_transform.empty_if_explicitly_empty(t.get('output', [])) or
      t['type'].startswith('Write')
  ]

  expected_inputs = [{
      'name': get_name(t),
      'elements': [],
  } for t in output_transforms]

  if not expected_inputs:
    # TODO: Optionally take this as a parameter.
    raise ValueError('No output transforms detected.')

  num_inputs = min_num_outputs
  while True:
    test_spec = {
        'mock_outputs': [{
            'name': t['name'],
            'elements': random.sample(
                t['elements'], min(len(t['elements']), num_inputs)),
        } for t in mock_outputs],
        'expected_inputs': expected_inputs,
    }
    fixes = run_test(pipeline_spec, test_spec, options, fix_failures=True)
    if len(fixes) < len(output_transforms):
      actual_output_size = 0
    else:
      actual_output_size = min(len(e) for e in fixes.values())
    if actual_output_size >= min_num_outputs:
      break
    elif num_inputs == max_num_inputs:
      break
    else:
      num_inputs = min(2 * num_inputs, max_num_inputs)

  for expected_input in test_spec['expected_inputs']:
    if ('expected_inputs', expected_input['name']) in fixes:
      expected_input['elements'] = fixes['expected_inputs',
                                         expected_input['name']]

  return test_spec


class _DoneException(Exception):
  pass


class RecordElements(beam.PTransform):
  _recorded_results = collections.defaultdict(list)

  def __init__(self, n):
    self._n = n
    self._id = str(uuid.uuid4())

  def get_and_remove(self):
    listing = RecordElements._recorded_results[self._id]
    del RecordElements._recorded_results[self._id]
    return listing

  def expand(self, pcoll):
    def record(element):
      listing = RecordElements._recorded_results[self._id]
      if len(listing) < self._n:
        listing.append(element)
      else:
        raise _DoneException()

    return pcoll | beam.Map(record)


def _first_n(transform_spec, options, n):
  recorder = RecordElements(n)
  try:
    with beam.Pipeline(options=options) as p:
      _ = (
          p
          | yaml_transform.YamlTransform(
              transform_spec,
              providers={'AssertEqualAndRecord': AssertEqualAndRecord})
          | recorder)
  except _DoneException:
    pass
  except Exception as exn:
    # Runners don't always raise a faithful exception type.
    if not '_DoneException' in str(exn):
      raise
  return recorder.get_and_remove()


K1 = TypeVar('K1')
K2 = TypeVar('K2')
V = TypeVar('V')

InputsType = Dict[str, Union[str, List[str]]]


def _composite_key_to_nested(
    d: Mapping[Tuple[K1, K2], V]) -> Mapping[K1, Mapping[K2, V]]:
  nested = collections.defaultdict(dict)
  for (k1, k2), v in d.items():
    nested[k1][k2] = v
  return nested


def _try_row_as_dict(row):
  try:
    return row._asdict()
  except AttributeError:
    return row


# Linter: No need for unittest.main here.
