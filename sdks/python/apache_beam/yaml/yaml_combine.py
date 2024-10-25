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

"""This module defines the basic Combine operation."""

from typing import Any
from typing import Iterable
from typing import Mapping
from typing import Optional

import apache_beam as beam
from apache_beam import typehints
from apache_beam.typehints import row_type
from apache_beam.typehints import trivial_inference
from apache_beam.typehints.decorators import get_type_hints
from apache_beam.typehints.schemas import named_fields_from_element_type
from apache_beam.utils import python_callable
from apache_beam.yaml import yaml_mapping
from apache_beam.yaml import yaml_provider

BUILTIN_COMBINE_FNS = {
    'sum': sum,
    'max': max,
    'min': min,
    'all': all,
    'any': any,
    'mean': beam.transforms.combiners.MeanCombineFn(),
    'count': beam.transforms.combiners.CountCombineFn(),
    'group': beam.transforms.combiners.ToListCombineFn(),
    'concat': beam.transforms.combiners.ConcatListCombineFn(),
}


def normalize_combine(spec):
  """Expands various shorthand specs for combine (which can otherwise be quite
  verbose for simple cases.)  We do this here so that it doesn't need to be done
  per language.  The following are all equivalent::

      dest: fn_type

      dest:
        value: dest
        fn: fn_type

      dest:
        value: dest
        fn:
          type: fn_type
  """
  from apache_beam.yaml.yaml_transform import SafeLineLoader
  if spec['type'] == 'Combine':
    config = spec.get('config')
    if isinstance(config.get('group_by'), str):
      config['group_by'] = [config['group_by']]

    def normalize_agg(dest, agg):
      if isinstance(agg, str):
        agg = {'fn': agg}
      if 'value' not in agg and config.get('language') != 'sql':
        agg['value'] = dest
      if isinstance(agg['fn'], str):
        agg['fn'] = {'type': agg['fn']}
      return agg

    if 'combine' not in config:
      raise ValueError('Missing combine parameter in Combine config.')
    config['combine'] = {
        dest: normalize_agg(dest, agg)
        for (dest,
             agg) in SafeLineLoader.strip_metadata(config['combine']).items()
    }
  return spec


class PyJsYamlCombine(beam.PTransform):
  """Groups and combines records sharing common fields.

  Built-in combine functions are BUILTIN_COMBINE_FNS
  but custom aggregation functions can be used as well.

  See also the documentation on
  [YAML Aggregation](https://beam.apache.org/documentation/sdks/yaml-combine/).
  """
  def __init__(
      self,
      group_by: Iterable[str],
      combine: Mapping[str, Mapping[str, Any]],
      language: Optional[str] = None):
    self._group_by = group_by
    self._combine = combine
    self._language = language

  def expand(self, pcoll):
    input_types = dict(named_fields_from_element_type(pcoll.element_type))
    all_fields = list(input_types.keys())
    unknown_keys = set(self._group_by) - set(all_fields)
    if unknown_keys:
      raise ValueError(f'Unknown grouping columns: {list(unknown_keys)}')

    def create_combine_fn(fn_spec):
      if 'type' not in fn_spec:
        raise ValueError(f'CombineFn spec missing type: {fn_spec}')
      elif fn_spec['type'] in BUILTIN_COMBINE_FNS:
        return BUILTIN_COMBINE_FNS[fn_spec['type']]
      elif self._language == 'python':
        # TODO(yaml): Support output_type here as well.
        fn = python_callable.PythonCallableWithSource.load_from_source(
            fn_spec['type'])
        if 'config' in fn_spec:
          fn = fn(**fn_spec['config'])
        return fn
      else:
        raise TypeError('Unknown CombineFn: {fn_spec}')

    def extract_return_type(expr):
      if isinstance(expr, str) and expr in input_types:
        return input_types[expr]
      expr_hints = get_type_hints(expr)
      if (expr_hints and expr_hints.has_simple_output_type() and
          expr_hints.simple_output_type(None) != typehints.Any):
        return expr_hints.simple_output_type(None)
      elif callable(expr):
        return trivial_inference.infer_return_type(expr, [pcoll.element_type])
      else:
        return Any

    # TODO(yaml): Support error handling.
    transform = beam.GroupBy(*self._group_by)
    output_types = [(k, input_types[k]) for k in self._group_by]

    for output, agg in self._combine.items():
      expr = yaml_mapping._as_callable(
          all_fields, agg['value'], 'Combine', self._language, input_types)
      fn = create_combine_fn(agg['fn'])
      transform = transform.aggregate_field(expr, fn, output)

      # TODO(yaml): See if this logic can be pushed into GroupBy itself.
      expr_type = extract_return_type(expr)
      if isinstance(fn, beam.CombineFn):
        # TODO(yaml): Better inference on CombineFns whose outputs types are
        # functions of their input types
        combined_type = extract_return_type(fn)
      elif fn in (sum, min, max):
        combined_type = expr_type
      elif fn in (any, all):
        combined_type = bool
      else:
        combined_type = Any
      output_types.append((output, combined_type))

    return pcoll | transform.with_output_types(
        row_type.RowTypeConstraint.from_fields(output_types))


if PyJsYamlCombine.__doc__:  # make mypy happy
  PyJsYamlCombine.__doc__ = PyJsYamlCombine.__doc__.replace(
      'BUILTIN_COMBINE_FNS',
      ', '.join('`%s`' % k for k in BUILTIN_COMBINE_FNS.keys()))


@beam.ptransform.ptransform_fn
def _SqlCombineTransform(
    pcoll, sql_transform_constructor, group_by, combine, language=None):
  all_fields = [
      x for x, _ in named_fields_from_element_type(pcoll.element_type)
  ]
  unknown_keys = set(group_by) - set(all_fields)
  if unknown_keys:
    raise ValueError(f'Unknown grouping columns: {list(unknown_keys)}')

  def combine_col(dest, fn_spec):
    if 'value' in fn_spec or 'config' in fn_spec['fn']:
      expr = '%s(%s)' % (
          fn_spec['fn']['type'],
          ', '.join([fn_spec['value']] +
                    list(fn_spec['fn'].get('config', {}).values())))
    else:
      expr = fn_spec['fn']['type']
    return f'{expr} as {dest}'

  return pcoll | sql_transform_constructor(
      'SELECT %s FROM PCOLLECTION GROUP BY %s' % (
          ', '.join(
              list(group_by) +
              [combine_col(dest, fn_spec)
               for dest, fn_spec in combine.items()]),
          ', '.join(group_by),
      ))


def create_combine_providers():
  return [
      yaml_provider.InlineProvider({
          'Combine-generic': PyJsYamlCombine,
          'Combine-python': PyJsYamlCombine,
          'Combine-javascript': PyJsYamlCombine,
      }),
      yaml_provider.SqlBackedProvider({
          'Combine-generic': _SqlCombineTransform,
          'Combine-sql': _SqlCombineTransform,
          'Combine-calcite': _SqlCombineTransform,
      }),
  ]
