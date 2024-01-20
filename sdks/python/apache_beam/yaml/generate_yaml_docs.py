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

import argparse
import contextlib
import re

import yaml

from apache_beam.portability.api import schema_pb2
from apache_beam.utils import subprocess_server
from apache_beam.yaml import json_utils
from apache_beam.yaml import yaml_provider


def _fake_value(name, beam_type):
  type_info = beam_type.WhichOneof("type_info")
  if type_info == "atomic_type":
    if beam_type.atomic_type == schema_pb2.STRING:
      return f'"{name}"'
    elif beam_type.atomic_type == schema_pb2.BOOLEAN:
      return "true|false"
    else:
      return name
  elif type_info == "array_type":
    return [_fake_value(name, beam_type.array_type.element_type), '...']
  elif type_info == "iterable_type":
    return [_fake_value(name, beam_type.iterable_type.element_type), '...']
  elif type_info == "map_type":
    if beam_type.map_type.key_type.atomic_type == schema_pb2.STRING:
      return {
          'a': _fake_value(name + '_value_a', beam_type.map_type.value_type),
          'b': _fake_value(name + '_value_b', beam_type.map_type.value_type),
          'c': '...',
      }
    else:
      return {
          _fake_value(name + '_key', beam_type.map_type.key_type): _fake_value(
              name + '_value', beam_type.map_type.value_type)
      }
  elif type_info == "row_type":
    return _fake_row(beam_type.row_type.schema)
  elif type_info == "logical_type":
    return name
  else:
    raise ValueError(f"Unrecognized type_info: {type_info!r}")


def _fake_row(schema):
  if schema is None:
    return '...'
  return {f.name: _fake_value(f.name, f.type) for f in schema.fields}


def pretty_example(provider, t):
  spec = {'type': t}
  try:
    requires_inputs = provider.requires_inputs(t, {})
  except Exception:
    requires_inputs = False
  if requires_inputs:
    spec['input'] = '...'
  config_schema = provider.config_schema(t)
  if config_schema is None or config_schema.fields:
    spec['config'] = _fake_row(config_schema)
  s = yaml.dump(spec, sort_keys=False)
  return s.replace("'", "")


def config_docs(schema):
  if schema is None:
    return ''
  elif not schema.fields:
    return 'No configuration parameters.'

  def pretty_type(beam_type):
    type_info = beam_type.WhichOneof("type_info")
    if type_info == "atomic_type":
      return schema_pb2.AtomicType.Name(beam_type.atomic_type).lower()
    elif type_info == "array_type":
      return f'Array[{pretty_type(beam_type.array_type.element_type)}]'
    elif type_info == "iterable_type":
      return f'Iterable[{pretty_type(beam_type.iterable_type.element_type)}]'
    elif type_info == "map_type":
      return (
          f'Map[{pretty_type(beam_type.map_type.key_type)}, '
          f'{pretty_type(beam_type.map_type.value_type)}]')
    elif type_info == "row_type":
      return 'Row'
    else:
      return '?'

  def maybe_row_parameters(t):
    if t.WhichOneof("type_info") == "row_type":
      return indent('\n\nRow fields:\n\n' + config_docs(t.row_type.schema), 4)
    else:
      return ''

  def maybe_optional(t):
    return " (Optional)" if t.nullable else ""

  def lines():
    for f in schema.fields:
      yield ''.join([
          f'**{f.name}** `{pretty_type(f.type)}`',
          maybe_optional(f.type),
          indent(': ' + f.description if f.description else '', 2),
          maybe_row_parameters(f.type),
      ])

  return '\n\n'.join('*' + indent(line, 2) for line in lines()).strip()


def indent(lines, size):
  return '\n'.join(' ' * size + line for line in lines.split('\n'))


def longest(func, xs):
  return max([func(x) or '' for x in xs], key=len)


def io_grouping_key(transform_name):
  """Place reads and writes next to each other, after all other transforms."""
  if transform_name.startswith('ReadFrom'):
    return 1, transform_name[8:], 0
  elif transform_name.startswith('WriteTo'):
    return 1, transform_name[7:], 1
  else:
    return 0, transform_name


SKIP = [
    'Combine',
    'Filter',
    'MapToFields',
]


def transform_docs(t, providers):
  return '\n'.join([
      f'## {t}',
      '',
      longest(lambda p: p.description(t), providers),
      '',
      '### Configuration',
      '',
      longest(lambda p: config_docs(p.config_schema(t)), providers),
      '',
      '### Usage',
      '',
      indent(longest(lambda p: pretty_example(p, t), providers), 4),
  ])


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--markdown_file')
  parser.add_argument('--schema_file')
  parser.add_argument('--include', default='.*')
  parser.add_argument(
      '--exclude', default='(Combine)|(Filter)|(MapToFields)-.*')
  options = parser.parse_args()
  include = re.compile(options.include).match
  exclude = re.compile(options.exclude).match

  with subprocess_server.SubprocessServer.cache_subprocesses():
    json_config_schemas = []
    with contextlib.ExitStack() as stack:
      if options.markdown_file:
        markdown_out = stack.enter_context(open(options.markdown_file, 'w'))
      providers = yaml_provider.standard_providers()
      for transform in sorted(providers.keys(), key=io_grouping_key):
        if include(transform) and not exclude(transform):
          print(transform)
          if options.markdown_file:
            markdown_out.write(transform_docs(transform, providers[transform]))
            markdown_out.write('\n\n')
          if options.schema_file:
            schema = providers[transform][0].config_schema(transform)
            if schema:
              json_config_schemas.append({
                  'if': {
                      'properties': {
                          'type': {
                              'const': transform
                          }
                      }
                  },
                  'then': {
                      'properties': {
                          'config': {
                              'type': 'object',
                              'properties': {
                                  '__line__': {
                                      'type': 'integer'
                                  },
                                  '__uuid__': {},
                                  **{
                                      f.name: json_utils.beam_type_to_json_type(
                                          f.type)
                                      for f in schema.fields
                                  }
                              },
                              'additionalProperties': False,
                          }
                      }
                  }
              })

    if options.schema_file:
      with open(options.schema_file, 'w') as fout:
        yaml.dump(json_config_schemas, fout, sort_keys=False)


if __name__ == '__main__':
  main()
