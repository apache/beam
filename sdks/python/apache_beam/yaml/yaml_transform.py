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
import json
import logging
import pprint
import re
import uuid
from typing import Iterable
from typing import Mapping

import yaml
from yaml.loader import SafeLoader

import apache_beam as beam
from apache_beam.transforms.fully_qualified_named_transform import FullyQualifiedNamedTransform
from apache_beam.yaml import yaml_provider

__all__ = ["YamlTransform"]

_LOGGER = logging.getLogger(__name__)
yaml_provider.fix_pycallable()


def memoize_method(func):
  def wrapper(self, *args):
    if not hasattr(self, '_cache'):
      self._cache = {}
    key = func.__name__, args
    if key not in self._cache:
      self._cache[key] = func(self, *args)
    return self._cache[key]

  return wrapper


def only_element(xs):
  x, = xs
  return x


class SafeLineLoader(SafeLoader):
  """A yaml loader that attaches line information to mappings and strings."""
  class TaggedString(str):
    """A string class to which we can attach metadata.

    This is primarily used to trace a string's origin back to its place in a
    yaml file.
    """
    def __reduce__(self):
      # Pickle as an ordinary string.
      return str, (str(self), )

  def construct_scalar(self, node):
    value = super().construct_scalar(node)
    if isinstance(value, str):
      value = SafeLineLoader.TaggedString(value)
      value._line_ = node.start_mark.line + 1
    return value

  def construct_mapping(self, node, deep=False):
    mapping = super().construct_mapping(node, deep=deep)
    mapping['__line__'] = node.start_mark.line + 1
    mapping['__uuid__'] = self.create_uuid()
    return mapping

  @classmethod
  def create_uuid(cls):
    return str(uuid.uuid4())

  @classmethod
  def strip_metadata(cls, spec, tagged_str=True):
    if isinstance(spec, Mapping):
      return {
          key: cls.strip_metadata(value, tagged_str)
          for key,
          value in spec.items() if key not in ('__line__', '__uuid__')
      }
    elif isinstance(spec, Iterable) and not isinstance(spec, (str, bytes)):
      return [cls.strip_metadata(value, tagged_str) for value in spec]
    elif isinstance(spec, SafeLineLoader.TaggedString) and tagged_str:
      return str(spec)
    else:
      return spec

  @staticmethod
  def get_line(obj):
    if isinstance(obj, dict):
      return obj.get('__line__', 'unknown')
    else:
      return getattr(obj, '_line_', 'unknown')


class LightweightScope(object):
  def __init__(self, transforms):
    self._transforms = transforms
    self._transforms_by_uuid = {t['__uuid__']: t for t in self._transforms}
    self._uuid_by_name = collections.defaultdict(list)
    for spec in self._transforms:
      if 'name' in spec:
        self._uuid_by_name[spec['name']].append(spec['__uuid__'])
      if 'type' in spec:
        self._uuid_by_name[spec['type']].append(spec['__uuid__'])

  def get_transform_id_and_output_name(self, name):
    if '.' in name:
      transform_name, output = name.rsplit('.', 1)
    else:
      transform_name, output = name, None
    return self.get_transform_id(transform_name), output

  def get_transform_id(self, transform_name):
    if transform_name in self._transforms_by_uuid:
      return transform_name
    else:
      candidates = self._uuid_by_name[transform_name]
      if not candidates:
        raise ValueError(
            f'Unknown transform at line '
            f'{SafeLineLoader.get_line(transform_name)}: {transform_name}')
      elif len(candidates) > 1:
        raise ValueError(
            f'Ambiguous transform at line '
            f'{SafeLineLoader.get_line(transform_name)}: {transform_name}')
      else:
        return only_element(candidates)


class Scope(LightweightScope):
  """To look up PCollections (typically outputs of prior transforms) by name."""
  def __init__(self, root, inputs, transforms, providers):
    super().__init__(transforms)
    self.root = root
    self._inputs = inputs
    self.providers = providers
    self._seen_names = set()

  def compute_all(self):
    for transform_id in self._transforms_by_uuid.keys():
      self.compute_outputs(transform_id)

  def get_pcollection(self, name):
    if name in self._inputs:
      return self._inputs[name]
    elif '.' in name:
      transform, output = name.rsplit('.', 1)
      outputs = self.get_outputs(transform)
      if output in outputs:
        return outputs[output]
      else:
        raise ValueError(
            f'Unknown output {repr(output)} '
            f'at line {SafeLineLoader.get_line(name)}: '
            f'{transform} only has outputs {list(outputs.keys())}')
    else:
      outputs = self.get_outputs(name)
      if len(outputs) == 1:
        return only_element(outputs.values())
      else:
        raise ValueError(
            f'Ambiguous output at line {SafeLineLoader.get_line(name)}: '
            f'{name} has outputs {list(outputs.keys())}')

  def get_outputs(self, transform_name):
    return self.compute_outputs(self.get_transform_id(transform_name))

  @memoize_method
  def compute_outputs(self, transform_id):
    return expand_transform(self._transforms_by_uuid[transform_id], self)

  # A method on scope as providers may be scoped...
  def create_ptransform(self, spec):
    if 'type' not in spec:
      raise ValueError(f'Missing transform type: {identify_object(spec)}')

    if spec['type'] not in self.providers:
      raise ValueError(
          'Unknown transform type %r at %s' %
          (spec['type'], identify_object(spec)))

    for provider in self.providers.get(spec['type']):
      if provider.available():
        break
    else:
      raise ValueError(
          'No available provider for type %r at %s' %
          (spec['type'], identify_object(spec)))

    if 'args' in spec:
      args = spec['args']
      if not isinstance(args, dict):
        raise ValueError(
            'Arguments for transform at %s must be a mapping.' %
            identify_object(spec))
    else:
      args = {
          key: value
          for (key, value) in spec.items()
          if key not in ('type', 'name', 'input', 'output')
      }
    real_args = SafeLineLoader.strip_metadata(args)
    try:
      # pylint: disable=undefined-loop-variable
      ptransform = provider.create_transform(spec['type'], real_args)
      # TODO(robertwb): Should we have a better API for adding annotations
      # than this?
      annotations = dict(
          yaml_type=spec['type'],
          yaml_args=json.dumps(real_args),
          yaml_provider=json.dumps(provider.to_json()),
          **ptransform.annotations())
      ptransform.annotations = lambda: annotations
      return ptransform
    except Exception as exn:
      if isinstance(exn, TypeError):
        # Create a slightly more generic error message for argument errors.
        msg = str(exn).replace('positional', '').replace('keyword', '')
        msg = re.sub(r'\S+lambda\S+', '', msg)
        msg = re.sub('  +', ' ', msg).strip()
      else:
        msg = str(exn)
      raise ValueError(
          f'Invalid transform specification at {identify_object(spec)}: {msg}'
      ) from exn

  def unique_name(self, spec, ptransform, strictness=0):
    if 'name' in spec:
      name = spec['name']
      strictness += 1
    else:
      name = ptransform.label
    if name in self._seen_names:
      if strictness >= 2:
        raise ValueError(f'Duplicate name at {identify_object(spec)}: {name}')
      else:
        name = f'{name}@{SafeLineLoader.get_line(spec)}'
    self._seen_names.add(name)
    return name


def expand_transform(spec, scope):
  if 'type' not in spec:
    raise TypeError(
        f'Missing type parameter for transform at {identify_object(spec)}')
  type = spec['type']
  if type == 'composite':
    return expand_composite_transform(spec, scope)
  else:
    return expand_leaf_transform(spec, scope)


def expand_leaf_transform(spec, scope):
  spec = normalize_inputs_outputs(spec)
  inputs_dict = {
      key: scope.get_pcollection(value)
      for (key, value) in spec['input'].items()
  }
  input_type = spec.get('input_type', 'default')
  if input_type == 'list':
    inputs = tuple(inputs_dict.values())
  elif input_type == 'map':
    inputs = inputs_dict
  else:
    if len(inputs_dict) == 0:
      inputs = scope.root
    elif len(inputs_dict) == 1:
      inputs = next(iter(inputs_dict.values()))
    else:
      inputs = inputs_dict
  _LOGGER.info("Expanding %s ", identify_object(spec))
  ptransform = scope.create_ptransform(spec)
  try:
    # TODO: Move validation to construction?
    with FullyQualifiedNamedTransform.with_filter('*'):
      outputs = inputs | scope.unique_name(spec, ptransform) >> ptransform
  except Exception as exn:
    raise ValueError(
        f"Error apply transform {identify_object(spec)}: {exn}") from exn
  if isinstance(outputs, dict):
    # TODO: Handle (or at least reject) nested case.
    return outputs
  elif isinstance(outputs, (tuple, list)):
    return {'out{ix}': pcoll for (ix, pcoll) in enumerate(outputs)}
  elif isinstance(outputs, beam.PCollection):
    return {'out': outputs}
  else:
    raise ValueError(
        f'Transform {identify_object(spec)} returned an unexpected type '
        f'{type(outputs)}')


def expand_composite_transform(spec, scope):
  spec = normalize_inputs_outputs(normalize_source_sink(spec))

  inner_scope = Scope(
      scope.root, {
          key: scope.get_pcollection(value)
          for key,
          value in spec['input'].items()
      },
      spec['transforms'],
      yaml_provider.merge_providers(
          yaml_provider.parse_providers(spec.get('providers', [])),
          scope.providers))

  class CompositePTransform(beam.PTransform):
    @staticmethod
    def expand(inputs):
      inner_scope.compute_all()
      return {
          key: inner_scope.get_pcollection(value)
          for (key, value) in spec['output'].items()
      }

  if 'name' not in spec:
    spec['name'] = 'Composite'
  if spec['name'] is None:  # top-level pipeline, don't nest
    return CompositePTransform.expand(None)
  else:
    _LOGGER.info("Expanding %s ", identify_object(spec))
    return ({
        key: scope.get_pcollection(value)
        for key,
        value in spec['input'].items()
    } or scope.root) | scope.unique_name(spec, None) >> CompositePTransform()


def expand_chain_transform(spec, scope):
  return expand_composite_transform(chain_as_composite(spec), scope)


def chain_as_composite(spec):
  # A chain is simply a composite transform where all inputs and outputs
  # are implicit.
  spec = normalize_source_sink(spec)
  if 'transforms' not in spec:
    raise TypeError(
        f"Chain at {identify_object(spec)} missing transforms property.")
  has_explicit_outputs = 'output' in spec
  composite_spec = normalize_inputs_outputs(spec)
  new_transforms = []
  for ix, transform in enumerate(composite_spec['transforms']):
    if any(io in transform for io in ('input', 'output', 'input', 'output')):
      raise ValueError(
          f'Transform {identify_object(transform)} is part of a chain, '
          'must have implicit inputs and outputs.')
    if ix == 0:
      transform['input'] = {key: key for key in composite_spec['input'].keys()}
    else:
      transform['input'] = new_transforms[-1]['__uuid__']
    new_transforms.append(transform)
  composite_spec['transforms'] = new_transforms

  last_transform = new_transforms[-1]['__uuid__']
  if has_explicit_outputs:
    composite_spec['output'] = {
        key: f'{last_transform}.{value}'
        for (key, value) in composite_spec['output'].items()
    }
  else:
    composite_spec['output'] = last_transform
  if 'name' not in composite_spec:
    composite_spec['name'] = 'Chain'
  composite_spec['type'] = 'composite'
  return composite_spec


def preprocess_chain(spec):
  if spec['type'] == 'chain':
    return chain_as_composite(spec)
  else:
    return spec


def pipeline_as_composite(spec):
  if isinstance(spec, list):
    return {
        'type': 'composite',
        'name': None,
        'transforms': spec,
        '__line__': spec[0]['__line__'],
        '__uuid__': SafeLineLoader.create_uuid(),
    }
  else:
    return dict(spec, name=None, type=spec.get('type', 'composite'))


def normalize_source_sink(spec):
  if 'source' not in spec and 'sink' not in spec:
    return spec
  spec = dict(spec)
  spec['transforms'] = list(spec.get('transforms', []))
  if 'source' in spec:
    spec['transforms'].insert(0, spec.pop('source'))
  if 'sink' in spec:
    spec['transforms'].append(spec.pop('sink'))
  return spec


def preprocess_source_sink(spec):
  if spec['type'] in ('chain', 'composite'):
    return normalize_source_sink(spec)
  else:
    return spec


def normalize_inputs_outputs(spec):
  spec = dict(spec)

  def normalize_io(tag):
    io = spec.get(tag, {})
    if isinstance(io, (str, list)):
      return {tag: io}
    else:
      return SafeLineLoader.strip_metadata(io, tagged_str=False)

  return dict(spec, input=normalize_io('input'), output=normalize_io('output'))


def identify_object(spec):
  line = SafeLineLoader.get_line(spec)
  name = extract_name(spec)
  if name:
    return f'"{name}" at line {line}'
  else:
    return f'at line {line}'


def extract_name(spec):
  if 'name' in spec:
    return spec['name']
  elif 'id' in spec:
    return spec['id']
  elif 'type' in spec:
    return spec['type']
  elif len(spec) == 1:
    return extract_name(next(iter(spec.values())))
  else:
    return ''


def push_windowing_to_roots(spec):
  scope = LightweightScope(spec['transforms'])
  consumed_outputs_by_transform = collections.defaultdict(set)
  for transform in spec['transforms']:
    for _, input_ref in transform['input'].items():
      try:
        transform_id, output = scope.get_transform_id_and_output_name(input_ref)
        consumed_outputs_by_transform[transform_id].add(output)
      except ValueError:
        # Could be an input or an ambiguity we'll raise later.
        pass

  for transform in spec['transforms']:
    if not transform['input'] and 'windowing' not in transform:
      transform['windowing'] = spec['windowing']
      transform['__consumed_outputs'] = consumed_outputs_by_transform[
          transform['__uuid__']]

  return spec


def preprocess_windowing(spec):
  if spec['type'] == 'WindowInto':
    # This is the transform where it is actually applied.
    return spec
  elif 'windowing' not in spec:
    # Nothing to do.
    return spec

  if spec['type'] == 'composite':
    # Apply the windowing to any reads, creates, etc. in this transform
    # TODO(robertwb): Better handle the case where a read is followed by a
    # setting of the timestamps. We should be careful of sliding windows
    # in particular.
    spec = push_windowing_to_roots(spec)

  windowing = spec.pop('windowing')
  if spec['input']:
    # Apply the windowing to all inputs by wrapping it in a trasnform that
    # first applies windowing and then applies the original transform.
    original_inputs = spec['input']
    windowing_transforms = [{
        'type': 'WindowInto',
        'name': f'WindowInto[{key}]',
        'windowing': windowing,
        'input': key,
        '__line__': spec['__line__'],
        '__uuid__': SafeLineLoader.create_uuid(),
    } for key in original_inputs.keys()]
    windowed_inputs = {
        key: t['__uuid__']
        for (key, t) in zip(original_inputs.keys(), windowing_transforms)
    }
    modified_spec = dict(
        spec, input=windowed_inputs, __uuid__=SafeLineLoader.create_uuid())
    return {
        'type': 'composite',
        'name': spec.get('name', None) or spec['type'],
        'transforms': [modified_spec] + windowing_transforms,
        'input': spec['input'],
        'output': modified_spec['__uuid__'],
        '__line__': spec['__line__'],
        '__uuid__': spec['__uuid__'],
    }

  elif spec['type'] == 'composite':
    # Pushing the windowing down was sufficient.
    return spec

  else:
    # No inputs, apply the windowing to all outputs.
    consumed_outputs = list(spec.pop('__consumed_outputs', {None}))
    modified_spec = dict(spec, __uuid__=SafeLineLoader.create_uuid())
    windowing_transforms = [{
        'type': 'WindowInto',
        'name': f'WindowInto[{out}]',
        'windowing': windowing,
        'input': modified_spec['__uuid__'] + ('.' + out if out else ''),
        '__line__': spec['__line__'],
        '__uuid__': SafeLineLoader.create_uuid(),
    } for out in consumed_outputs]
    if consumed_outputs == [None]:
      windowed_outputs = only_element(windowing_transforms)['__uuid__']
    else:
      windowed_outputs = {
          out: t['__uuid__']
          for (out, t) in zip(consumed_outputs, windowing_transforms)
      }
    return {
        'type': 'composite',
        'name': spec.get('name', None) or spec['type'],
        'transforms': [modified_spec] + windowing_transforms,
        'output': windowed_outputs,
        '__line__': spec['__line__'],
        '__uuid__': spec['__uuid__'],
    }


def preprocess_flattened_inputs(spec):
  if spec['type'] != 'composite':
    return spec

  # Prefer to add the flattens as sibling operations rather than nesting
  # to keep graph shape consistent when the number of inputs goes from
  # one to multiple.
  new_transforms = []
  for t in spec['transforms']:
    if t['type'] == 'Flatten':
      # Don't flatten before explicit flatten.
      # But we do have to expand list inputs into singleton inputs.
      def all_inputs(t):
        for key, values in t.get('input', {}).items():
          if isinstance(values, list):
            for ix, values in enumerate(values):
              yield f'{key}{ix}', values
          else:
            yield key, values

      inputs_dict = {}
      for key, value in all_inputs(t):
        while key in inputs_dict:
          key += '_'
        inputs_dict[key] = value
      t = dict(t, input=inputs_dict)
    else:
      replaced_inputs = {}
      for key, values in t.get('input', {}).items():
        if isinstance(values, list):
          flatten_id = SafeLineLoader.create_uuid()
          new_transforms.append({
              'type': 'Flatten',
              'name': '%s-Flatten[%s]' % (t.get('name', t['type']), key),
              'input': {
                  f'input{ix}': value
                  for (ix, value) in enumerate(values)
              },
              '__line__': spec['__line__'],
              '__uuid__': flatten_id,
          })
          replaced_inputs[key] = flatten_id
      if replaced_inputs:
        t = dict(t, input={**t['input'], **replaced_inputs})
    new_transforms.append(t)
  return dict(spec, transforms=new_transforms)


def preprocess(spec, verbose=False):
  if verbose:
    pprint.pprint(spec)

  def apply(phase, spec):
    spec = phase(spec)
    if spec['type'] in {'composite', 'chain'}:
      spec = dict(
          spec, transforms=[apply(phase, t) for t in spec['transforms']])
    return spec

  for phase in [preprocess_source_sink,
                preprocess_chain,
                normalize_inputs_outputs,
                preprocess_flattened_inputs,
                preprocess_windowing]:
    spec = apply(phase, spec)
    if verbose:
      print('=' * 20, phase, '=' * 20)
      pprint.pprint(spec)
  return spec


class YamlTransform(beam.PTransform):
  def __init__(self, spec, providers={}):  # pylint: disable=dangerous-default-value
    if isinstance(spec, str):
      spec = yaml.load(spec, Loader=SafeLineLoader)
    self._spec = preprocess(spec)
    self._providers = yaml_provider.merge_providers(
        {
            key: yaml_provider.as_provider_list(key, value)
            for (key, value) in providers.items()
        },
        yaml_provider.standard_providers())

  def expand(self, pcolls):
    if isinstance(pcolls, beam.pvalue.PBegin):
      root = pcolls
      pcolls = {}
    elif isinstance(pcolls, beam.PCollection):
      root = pcolls.pipeline
      pcolls = {'input': pcolls}
    else:
      root = next(iter(pcolls.values())).pipeline
    result = expand_transform(
        self._spec,
        Scope(root, pcolls, transforms=[], providers=self._providers))
    if len(result) == 1:
      return only_element(result.values())
    else:
      return result


def expand_pipeline(pipeline, pipeline_spec, providers=None):
  if isinstance(pipeline_spec, str):
    pipeline_spec = yaml.load(pipeline_spec, Loader=SafeLineLoader)
  # Calling expand directly to avoid outer layer of nesting.
  return YamlTransform(
      pipeline_as_composite(pipeline_spec['pipeline']),
      {
          **yaml_provider.parse_providers(pipeline_spec.get('providers', [])),
          **{
              key: yaml_provider.as_provider_list(key, value)
              for (key, value) in (providers or {}).items()
          }
      }).expand(beam.pvalue.PBegin(pipeline))
