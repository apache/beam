#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Utilities to be used in  Interactive Beam.
"""

from __future__ import absolute_import

import pandas as pd

from apache_beam.typehints import typehints as th
from apache_beam.utils.windowed_value import WindowedValue

COLUMN_PREFIX = 'el'


def parse_row_(el, element_type, depth):
  elements = []
  columns = []

  # Recurse if there are a known length of columns to parse into.
  if isinstance(element_type, (th.TupleHint.TupleConstraint)):
    for index, t in enumerate(element_type._inner_types()):
      underlying_columns, underlying_elements = parse_row_(el[index], t,
                                                           depth + 1)
      column = '[{}]'.format(index)
      if underlying_columns:
        columns += [column + c for c in underlying_columns]
      else:
        columns += [column]
      elements += underlying_elements

  # Don't make new columns for variable length types.
  elif isinstance(
      element_type,
      (th.ListHint.ListConstraint, th.TupleHint.TupleSequenceConstraint)):
    elements = [pd.array(el)]

  # For any other types, try to parse as a namedtuple, otherwise pass element
  # through.
  else:
    fields = getattr(el, '_fields', None)
    if fields:
      columns = list(fields)
      if depth > 0:
        columns = ['[{}]'.format(f) for f in fields]
      elements = [el._asdict()[f] for f in fields]
    else:
      elements = [el]
  return columns, elements


def parse_row(el, element_type, include_window_info=True, prefix=COLUMN_PREFIX):
  # Reify the WindowedValue data to the Dataframe if asked.
  windowed = None
  if isinstance(el, WindowedValue):
    if include_window_info:
      windowed = el
    el = el.value

  # Parse the elements with the given type.
  columns, elements = parse_row_(el, element_type, 0)

  # If there are no columns returned, there is only a single column of a
  # primitive data type.
  if not columns:
    columns = ['']

  # Add the prefix to the columns that have an index.
  for i in range(len(columns)):
    if columns[i] == '' or columns[i][0] == '[':
      columns[i] = prefix + columns[i]

  # Reify the windowed columns and do a best-effort casting into Pandas DTypes.
  if windowed:
    columns += ['event_time', 'windows', 'pane_info']
    elements += [
        windowed.timestamp.micros, windowed.windows, windowed.pane_info
    ]
  return columns, elements


def pcoll_to_df(
    elements, element_type, include_window_info=False, prefix=COLUMN_PREFIX):
  """Parses the given elements into a Dataframe.

  Each column name will be prefixed with `prefix` concatenated with the nested
  index, e.g. for a Tuple[Tuple[int, str], int], the column names will be:
  [prefix[0][0], prefix[0][1], prefix[0]]. This is subject to change.
  """
  rows = []
  columns = []

  for e in elements:
    columns, row = parse_row(e, element_type, include_window_info, prefix)
    rows.append(row)

  return pd.DataFrame(rows, columns=columns)
