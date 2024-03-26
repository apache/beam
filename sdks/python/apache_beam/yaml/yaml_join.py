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
import apache_beam as beam
from apache_beam.yaml import yaml_provider


@beam.ptransform.ptransform_fn
def _SqlJoinTransform(pcoll, sql_transform_constructor, type, equalities):
    tables = list(pcoll)
    outer = [] # TODO(titodo) conditionally check for when a dict is passed for type and fill in the fields with outer
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
    for i in range(1,len(tables)):
        curr_table = tables[i]
        join_type = generate_join_type(prev_table, curr_table)
        join_conditions[curr_table] = f'{join_type} JOIN {curr_table}'
        prev_table = curr_table

    for equality in equalities:
        keys = list(equality)
        left, right = keys[0], keys[1]

        # TODO(titodo) - shorten this and reduce repitition
        if left in conditioned and right in conditioned:
            t = tables[max(tables.index(left), tables.index(right))]
            join_conditions[t] = f'{join_conditions[t]} AND {left}.{equality[left]} = {right}.{equality[right]}'
        elif left in conditioned:
            join_conditions[right] = f'{join_conditions[right]} ON {left}.{equality[left]} = {right}.{equality[right]}'
            conditioned.add(right)
        elif right in conditioned:
            join_conditions[left] = f'{join_conditions[left]} ON {left}.{equality[left]} = {right}.{equality[right]}'
            conditioned.add(left)
        else:
            t = tables[max(tables.index(left), tables.index(right))]
            join_conditions[t] = f'{join_conditions[t]} ON {left}.{equality[left]} = {right}.{equality[right]}'
            conditioned.add(t)

    query = f'SELECT * FROM {base_table} '
    for condition in join_conditions.values():
        query += condition
    print(query)
    return pcoll | sql_transform_constructor(query)


def create_join_providers():
    return [
        yaml_provider.SqlBackedProvider({
            'Join': _SqlJoinTransform
        }),
    ]