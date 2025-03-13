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

"""This module defines the Join operation."""
from typing import Any
from typing import Optional
from typing import Union

import apache_beam as beam
from apache_beam.yaml import yaml_provider


def _validate_input(pcolls):
  error_prefix = f'Invalid input {pcolls} specified.'
  if not isinstance(pcolls, dict):
    raise ValueError(f'{error_prefix} It must be a dict.')
  if len(pcolls) < 2:
    raise ValueError(
        f'{error_prefix} There should be at least 2 inputs to join.')


def _validate_type(type, pcolls):
  error_prefix = f'Invalid value "{type}" for "type".'
  if not isinstance(type, dict) and not isinstance(type, str):
    raise ValueError(f'{error_prefix} It must be a dict or a str.')
  if isinstance(type, dict):
    error = ValueError(
        f'{error_prefix} When specifying a dict for type, '
        f'it must follow this format: '
        f'{{"outer": [list of inputs to outer join]}}. '
        f'Example: {{"outer": ["input1", "input2"]}}')
    if (len(type) != 1 or next(iter(type)) != 'outer' or
        not isinstance(type['outer'], list)):
      raise error
    for input in type['outer']:
      if input not in list(pcolls.keys()):
        raise ValueError(
            f'{error_prefix} An invalid input "{input}" was specified.')
  if isinstance(type, str) and type not in ('inner', 'outer', 'left', 'right'):
    raise ValueError(
        f'{error_prefix} When specifying the value for type as a str, '
        f'it must be one of the following: "inner", "outer", "left", "right"')


def _validate_equalities(equalities, pcolls):
  error_prefix = f'Invalid value "{equalities}" for "equalities".'

  valid_cols = {
      name: set(
          dict(fields).keys() if fields and all(
              isinstance(field, tuple) for field in fields) else fields)
      for (name, pcoll) in pcolls.items()
      for fields in [getattr(pcoll.element_type, '_fields', [])]
  }

  if isinstance(equalities, str):
    for cols in valid_cols.values():
      if equalities not in cols:
        raise ValueError(
            f'{error_prefix} When "equalities" is a str, '
            f'it must be a field name that exists in all the specified inputs.')
    equality = {pcoll_tag: equalities for pcoll_tag in pcolls}
    return [equality]

  if not isinstance(equalities, list):
    raise ValueError(f'{error_prefix} It should be a str or a list.')

  input_edge_list = []
  for equality in equalities:
    invalid_dict_error = ValueError(
        f'{error_prefix} {equality} '
        f'should be a dict[str, str] containing at least 2 items.')
    if not isinstance(equality, dict):
      raise invalid_dict_error
    if len(equality) < 2:
      raise invalid_dict_error

    for pcoll_tag, col in equality.items():
      if pcoll_tag not in pcolls:
        raise ValueError(
            f'{error_prefix} "{pcoll_tag}" is not a specified alias in "input"')
      if col not in valid_cols[pcoll_tag]:
        raise ValueError(
            f'{error_prefix} "{col}" is not a valid field in "{pcoll_tag}".')

    input_edge_list.append(tuple(equality.keys()))

  if not _is_connected(input_edge_list, len(pcolls)):
    raise ValueError(
        f'{error_prefix} '
        f'The provided equalities do not connect all of {list(pcolls.keys())}.')

  return equalities


def _parse_fields(tables, fields):
  error_prefix = f'Invalid value "{fields}" for "fields".'
  if not isinstance(fields, dict):
    raise ValueError(f'{error_prefix} Fields must be a dict.')
  output_fields = []
  named_columns = set()
  for input, cols in fields.items():
    if input not in tables:
      raise ValueError(f'An invalid input "{input}" was specified in "fields".')
    if isinstance(cols, list):
      for col in cols:
        if not isinstance(col, str):
          raise ValueError(
              f'Invalid column "{col}" in "fields". Column name must be a str.')
        if col in named_columns:
          raise ValueError(
              f'The field name "{col}" was specified more than once.')
        output_fields.append(f'{input}.{col} AS {col}')
        named_columns.add(col)
    elif isinstance(cols, dict):
      for k, v in cols.items():
        if k in named_columns:
          raise ValueError(
              f'The field name "{k}" was specified more than once.')
        if not isinstance(v, str):
          raise ValueError(
              f'Invalid column "{v}" in "fields". Column name must be a str.')
        output_fields.append(f'{input}.{v} AS {k}')
        named_columns.add(k)
    else:
      raise ValueError(
          f'{error_prefix} '
          f'For every input key in fields, '
          f'the value must either be a list or dict.')
  for table in tables:
    if table not in fields.keys():
      output_fields.append(f'{table}.*')
  return output_fields


def _is_connected(edge_list, expected_node_count):
  graph = {}
  for edge_set in edge_list:
    for u in edge_set:
      if u not in graph:
        graph[u] = set()
      for v in edge_set:
        if u != v:
          graph[u].add(v)

  visited = set()
  stack = [next(iter(graph))]
  while stack:
    node = stack.pop()
    visited.add(node)
    for neighbor in graph[node]:
      if neighbor not in visited:
        stack.append(neighbor)

  return len(visited) == len(graph) == expected_node_count


@beam.ptransform.ptransform_fn
def _SqlJoinTransform(
    pcolls,
    sql_transform_constructor,
    *,
    equalities: Union[str, list[dict[str, str]]],
    type: Union[str, dict[str, list]] = 'inner',
    fields: Optional[dict[str, Any]] = None):
  """Joins two or more inputs using a specified condition.

  For example::

      type: Join
      input:
        input1: SomeTransform
        input2: AnotherTransform
        input3: YetAnotherTransform
      config:
        type: inner
        equalities:
          - input1: colA
            input2: colB
          - input2: colX
            input3: colY
        fields:
          input1: [colA, colB, colC]
          input2: {new_name: colB}

  would perform an inner join on the three inputs satisfying the constraints
  that `input1.colA = input2.colB` and `input2.colX = input3.colY`
  emitting rows with `colA`, `colB` and `colC` from `input1`, the values of
  `input2.colB` as a field called `new_name`, and all the fields from `input3`.

  Args:
    type: The type of join. Could be a string value in
        ["inner", "left", "right", "outer"] that specifies the type of join to
        be performed. For scenarios with multiple inputs to join where different
        join types are desired, specify the inputs to be outer joined. For
        example, ``{outer: [input1, input2]}`` means that `input1` and `input2`
        will be outer joined using the conditions specified, while other inputs
        will be inner joined.
    equalities: The condition to join on. A list of sets of columns that should
        be equal to fulfill the join condition. For the simple scenario of
        joining on the same column across all inputs where the column name is
        the same, one can specify the column name as the equality rather than
        having to list it for every input.
    fields: The fields to be outputted. A mapping with the input alias as the
        key and the list of fields in the input to be outputted.
        The value in the map
        can either be a dictionary with the new field name as the key and the
        original field name as the value (e.g new_field_name: field_name), or a
        list of the fields to be outputted with their original names
        (e.g ``[col1, col2, col3]``), or an '*' indicating all fields in the
        input will be outputted. If not specified, all fields from all inputs
        will be outputted.
  """

  _validate_input(pcolls)
  _validate_type(type, pcolls)
  validate_equalities = _validate_equalities(equalities, pcolls)

  equalities_in_pairs = []
  for equality in validate_equalities:
    inputs = list(equality.keys())
    first_input = inputs[0]
    for input in inputs[1:]:
      equalities_in_pairs.append({
          first_input: equality[first_input], input: equality[input]
      })

  tables = list(pcolls.keys())
  if isinstance(type, dict):
    outer = type['outer']
  elif type == 'outer':
    outer = tables
  else:
    outer = []
  first_table = tables[0]
  conditioned = [first_table]

  def generate_join_type(left, right):
    if left in outer and right in outer:
      return 'FULL'
    if left in outer:
      return 'LEFT'
    if right in outer:
      return 'RIGHT'
    if not outer:
      return type.upper()
    return 'INNER'

  prev_table = tables[0]
  join_conditions = {}
  for i in range(1, len(tables)):
    curr_table = tables[i]
    join_type = generate_join_type(prev_table, curr_table)
    join_conditions[curr_table] = f' {join_type} JOIN {curr_table}'
    prev_table = curr_table

  for equality in equalities_in_pairs:
    left, right = equality.keys()
    if left in conditioned and right in conditioned:
      t = tables[max(tables.index(left), tables.index(right))]
      join_conditions[t] = (
          f'{join_conditions[t]} '
          f'AND {left}.{equality[left]} = {right}.{equality[right]}')
    elif left in conditioned:
      join_conditions[right] = (
          f'{join_conditions[right]} '
          f'ON {left}.{equality[left]} = {right}.{equality[right]}')
      conditioned.append(right)
    elif right in conditioned:
      join_conditions[left] = (
          f'{join_conditions[left]} '
          f'ON {left}.{equality[left]} = {right}.{equality[right]}')
      conditioned.append(left)
    else:
      t = tables[max(tables.index(left), tables.index(right))]
      join_conditions[t] = (
          f'{join_conditions[t]} '
          f'ON {left}.{equality[left]} = {right}.{equality[right]}')
      conditioned.append(t)

  if fields:
    selects = ', '.join(_parse_fields(tables, fields))
  else:
    selects = '*'
  query = f'SELECT {selects} FROM {first_table}'
  query += ' '.join(condition for condition in join_conditions.values())
  return pcolls | sql_transform_constructor(query)


def create_join_providers():
  return [
      yaml_provider.SqlBackedProvider({'Join': _SqlJoinTransform}),
  ]
