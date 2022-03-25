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
# pytype: skip-file

import array
from collections import OrderedDict

import numpy as np
from fastavro import parse_schema

from apache_beam.typehints import trivial_inference
from apache_beam.typehints import typehints

try:
  import pyarrow as pa
except ImportError:
  pa = None


def infer_element_type(elements):
  """For internal use only; no backwards-compatibility guarantees.

  Infer a Beam type for a list of elements.

  Args:
    elements (List[Any]): A list of elements for which the type should be
        inferred.

  Returns:
    A Beam type encompassing all elements.
  """
  element_type = typehints.Union[[
      trivial_inference.instance_to_type(e) for e in elements
  ]]
  return element_type


def infer_typehints_schema(data):
  """For internal use only; no backwards-compatibility guarantees.

  Infer Beam types for tabular data.

  Args:
    data (List[dict]): A list of dictionaries representing rows in a table.

  Returns:
    An OrderedDict mapping column names to Beam types.
  """
  column_data = OrderedDict()
  for row in data:
    for key, value in row.items():
      column_data.setdefault(key, []).append(value)
  column_types = OrderedDict([
      (key, infer_element_type(values)) for key, values in column_data.items()
  ])
  return column_types


def infer_avro_schema(data):
  """For internal use only; no backwards-compatibility guarantees.

  Infer avro schema for tabular data.

  Args:
    data (List[dict]): A list of dictionaries representing rows in a table.

  Returns:
    An avro schema object.
  """
  _typehint_to_avro_type = {
      type(None): "null",
      int: "int",
      float: "double",
      str: "string",
      bytes: "bytes",
      np.ndarray: "bytes",
      array.array: "bytes",
  }

  def typehint_to_avro_type(value):
    if isinstance(value, typehints.UnionConstraint):
      return sorted(
          typehint_to_avro_type(union_type) for union_type in value.union_types)
    else:
      return _typehint_to_avro_type[value]

  column_types = infer_typehints_schema(data)
  avro_fields = [{
      "name": str(key), "type": typehint_to_avro_type(value)
  } for key,
                 value in column_types.items()]
  schema_dict = {
      "namespace": "example.avro",
      "name": "User",
      "type": "record",
      "fields": avro_fields
  }
  return parse_schema(schema_dict)


def infer_pyarrow_schema(data):
  """For internal use only; no backwards-compatibility guarantees.

  Infer PyArrow schema for tabular data.

  Args:
    data (List[dict]): A list of dictionaries representing rows in a table.

  Returns:
    A PyArrow schema object.
  """
  column_data = OrderedDict()
  for row in data:
    for key, value in row.items():
      column_data.setdefault(key, []).append(value)
  column_types = OrderedDict([
      (key, pa.array(value).type) for key, value in column_data.items()
  ])
  return pa.schema(list(column_types.items()))
