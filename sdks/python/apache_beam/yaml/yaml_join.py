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
import networkx as nx
import apache_beam as beam
from apache_beam.yaml import yaml_provider


def _validate_type(type, str):
  error = ValueError('Invalid value for \'type\' in Join transform')
  print(type)
  if not isinstance(type, dict) and isinstance(type, str) != str:
    return error
  if isinstance(type, dict) and len(type) != 1 and next(
      iter(type)) != 'outer' and not isinstance(type['outer'],
                                                list):
    return error
  if isinstance(type, str) and type not in (
      'inner', 'outer', 'left', 'right'):
    return error


def _validate_equalities(equalities, pcoll):
  error = ValueError(
    'Invalid value for \'equalities\' in Join transform')
  if not isinstance(equalities, list):
    return error
  input_edge_list = []
  for equality in equalities:
    if len(equality) != 2 and not isinstance(equality, dict):
      return error

    for input, col in equality.items():
      # TODO: look for an easier way to get field names in a Pcollection obj
      possible_cols = set()
      for field in pcoll['f'].element_type._fields:
        possible_cols.add(field[0])
      if input not in pcoll.keys() or col not in possible_cols:
        return ValueError(
          'Invalid input alias or column name doesn\'t exist in the input')

    input_edge_list.append(tuple(equality.keys()))

  if not nx.is_connected(nx.Graph(input_edge_list)):
    return ValueError(
      'Inputs in equalities are not all connected'
    )


def _parse_fields(tables, fields):
  # TODO(titodo) - consider taking all validations to a preprocessing fn
  output_fields = []
  named_columns = set()
  for input, cols in fields.items():
    if input not in tables:
      return ValueError(f'invalid input {input}')
    if isinstance(cols, list):
      for col in cols:
        if col in named_columns:
          return ValueError(
            f'same field name {col} specified more than once')
        output_fields.append(f'{input}.{col} AS {col}')
        named_columns.add(col)
    elif isinstance(cols, dict):
      for k, v in cols.items():
        if k in named_columns:
          return ValueError(
            f'same field name {k} specified more than once')
        output_fields.append(f'{input}.{v} AS {k}')
        named_columns.add(k)
    else:
      return ValueError(f'invalid entry for fields')
  for table in tables:
    if table not in fields.keys():
      output_fields.append(f'{table}.*')
  return output_fields


@beam.ptransform.ptransform_fn
def _SqlJoinTransform(pcoll, sql_transform_constructor, type,
                      equalities, fields=None):
  _validate_type(type, str)
  _validate_equalities(equalities, pcoll)

  tables = list(pcoll)
  outer = []
  if isinstance(type, dict):
    outer = type['outer']
  base_table = tables[0]
  conditioned = {base_table}

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

  for equality in equalities:
    keys = list(equality)
    left, right = keys[0], keys[1]
    # TODO(titodo) - shorten this and reduce repitition
    if left in conditioned and right in conditioned:
      t = tables[max(tables.index(left), tables.index(right))]
      join_conditions[
        t] = f'{join_conditions[t]} AND {left}.{equality[left]} = {right}.{equality[right]}'
    elif left in conditioned:
      join_conditions[
        right] = f'{join_conditions[right]} ON {left}.{equality[left]} = {right}.{equality[right]}'
      conditioned.add(right)
    elif right in conditioned:
      join_conditions[
        left] = f'{join_conditions[left]} ON {left}.{equality[left]} = {right}.{equality[right]}'
      conditioned.add(left)
    else:
      t = tables[max(tables.index(left), tables.index(right))]
      join_conditions[
        t] = f'{join_conditions[t]} ON {left}.{equality[left]} = {right}.{equality[right]}'
      conditioned.add(t)

  selects = '*'
  if fields:
    selects = ', '.join(_parse_fields(tables, fields))
  query = f'SELECT {selects} FROM {base_table}'
  for condition in join_conditions.values():
    query += condition
  print(query)
  return pcoll | sql_transform_constructor(query)


def create_join_providers():
  return [
    yaml_provider.SqlBackedProvider({'Join': _SqlJoinTransform}),
  ]
