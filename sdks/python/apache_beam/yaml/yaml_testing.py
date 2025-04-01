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

import yaml

import apache_beam as beam
from apache_beam.yaml import yaml_transform
from apache_beam.yaml import yaml_utils


def run_test(pipeline_spec, test_spec):
  if isinstance(pipeline_spec, str):
    pipeline_spec = yaml.load(pipeline_spec, Loader=yaml_utils.SafeLineLoader)

  transform_spec = inject_test_tranforms(
      yaml_transform.pipeline_as_composite(pipeline_spec['pipeline']),
      test_spec)

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

  included_outputs = set()
  transforms = []
  scope = yaml_transform.LightweightScope(spec['transforms'])

  def all_inputs(transform_spec):
    for input_ref in yaml_transform.empty_if_explicitly_empty(
        transform_spec['input']).values():
      if isinstance(input_ref, str):
        yield input_ref
      else:
        yield from input_ref
        require_output(input)

  def require_output(name):
    transform_id, tag = scope.get_transform_id_and_output_name(name)
    if (transform_id, tag) in included_outputs:
      return
    included_outputs.add((transform_id, tag))

    transform_spec = scope.get_transform_spec(transform_id)
    transforms.append(transform_spec)
    for input_ref in all_inputs(transform_spec):
      require_output(input_ref)

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
    transform_spec = scope.get_transform_spec(expected_input['name'])
    for input_ref in all_inputs(transform_spec):
      require_output(input_ref)
    transforms.append(
        create_assertion(
            f'CheckExpectedInput[{expected_input["name"]}]',
            transform_spec['input'],
            expected_input['elements']))

  return {
      '__uuid__': yaml_utils.SafeLineLoader.create_uuid(),
      'type': 'composite',
      'transforms': transforms,
  }
