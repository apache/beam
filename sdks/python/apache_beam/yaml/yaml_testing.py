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
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple
from typing import TypeVar
from typing import Union

import yaml

import apache_beam as beam
from apache_beam.yaml import yaml_transform
from apache_beam.yaml import yaml_utils


def run_test(pipeline_spec, test_spec, options=None):
  if isinstance(pipeline_spec, str):
    pipeline_spec = yaml.load(pipeline_spec, Loader=yaml_utils.SafeLineLoader)

  transform_spec = inject_test_tranforms(
      yaml_transform.pipeline_as_composite(pipeline_spec['pipeline']),
      test_spec)

  allowed_sources = set(test_spec.get('allowed_sources', []) + ['Create'])
  for transform in transform_spec['transforms']:
    name_or_type = transform.get('name', transform['type'])
    if (not yaml_transform.empty_if_explicitly_empty(transform.get('input', []))
        and not transform.get('name') in allowed_sources and
        not transform['type'] in allowed_sources):
      raise ValueError(
          f'Non-mocked source {name_or_type} '
          f'at line {yaml_transform.SafeLineLoader.get_line(transform)}')

  options = beam.options.pipeline_options.PipelineOptions(
      pickle_library='cloudpickle',
      **yaml_transform.SafeLineLoader.strip_metadata(
          pipeline_spec.get('options', {})))

  with beam.Pipeline(options=options) as p:
    _ = p | yaml_transform.YamlTransform(transform_spec)


def inject_test_tranforms(spec, test_spec):
  # These are idempotent, so it's OK to do them preemptively.
  for phase in [
      yaml_transform.ensure_transforms_have_types,
      yaml_transform.preprocess_source_sink,
      yaml_transform.preprocess_chain,
      yaml_transform.tag_explicit_inputs,
      yaml_transform.normalize_inputs_outputs,
  ]:
    spec = yaml_transform.apply_phase(phase, spec)

  scope = yaml_transform.LightweightScope(spec['transforms'])

  mocked_inputs_by_id = {
      scope.get_transform_id(mock_input['name']): mock_input
      for mock_input in test_spec.get('mock_inputs', [])
  }

  mocked_outputs_by_id = _composite_key_to_nested({
      scope.get_transform_id_and_output_name(mock_output['name']): mock_output
      for mock_output in test_spec.get('mock_outputs', [])
  })

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
          for tag,
          input_ref in yaml_transform.empty_if_explicitly_empty(
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
        mocked_inputs_by_id[transform_id]['elements'])
    transforms.append(transform)
    return transform['__uuid__']

  @functools.cache
  def create_mocked_output(transform_id: str, tag: str) -> str:
    transform = create_create(
        f'MockOutput[{mocked_outputs_by_id[transform_id][tag]["name"]}]',
        mocked_outputs_by_id[transform_id][tag]['elements'])
    transforms.append(transform)
    return transform['__uuid__']

  def create_create(name, elements):
    return {
        '__uuid__': yaml_utils.SafeLineLoader.create_uuid(),
        'name': name,
        'type': 'Create',
        'config': {
            'elements': elements,
        },
    }

  def create_assertion(name, inputs, elements):
    return {
        '__uuid__': yaml_utils.SafeLineLoader.create_uuid(),
        'name': name,
        'input': inputs,
        'type': 'AssertEqual',
        'config': {
            'elements': elements,
        },
    }

  for ix, expected_output in enumerate(test_spec.get('expected_outputs', [])):
    if 'name' not in expected_output:
      raise ValueError(f'Expected output spec {ix} missing a name.')
    if 'elements' not in expected_output:
      raise ValueError(
          f'Expected output {expected_output["name"]} missing elements.')
    require_output(expected_output['name'])
    transforms.append(
        create_assertion(
            f'CheckExpectedOutput[{expected_output["name"]}]',
            expected_output['name'],
            expected_output['elements']))

  for ix, expected_input in enumerate(test_spec.get('expected_inputs', [])):
    if 'name' not in expected_input:
      raise ValueError(f'Expected input spec {ix} missing a name.')
    transform_id = scope.get_transform_id(expected_input['name'])
    transforms.append(
        create_assertion(
            f'CheckExpectedInput[{expected_input["name"]}]',
            create_inputs(transform_id),
            expected_input['elements']))

  return {
      '__uuid__': yaml_utils.SafeLineLoader.create_uuid(),
      'type': 'composite',
      'transforms': transforms,
  }


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
