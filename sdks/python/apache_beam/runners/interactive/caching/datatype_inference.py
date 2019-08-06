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
from __future__ import absolute_import

import array
import json
from collections import OrderedDict

import numpy as np
import pyarrow as pa
from past.builtins import unicode

from apache_beam.typehints import trivial_inference
from apache_beam.typehints import typehints

# pylint: disable=wrong-import-order, wrong-import-position
try:
  from avro.schema import Parse  # avro-python3 library for python3
except ImportError:
  from avro.schema import parse as Parse  # avro library for python2
# pylint: enable=wrong-import-order, wrong-import-position


def infer_element_type(elements):
  element_type = typehints.Union[[
      trivial_inference.instance_to_type(e) for e in elements
  ]]
  return element_type


def infer_typehints_schema(data):
  """
  TODO: For internal use only.
  """
  column_data = OrderedDict()
  for row in data:
    for key, value in row.items():
      column_data.setdefault(key, []).append(value)
  column_types = OrderedDict([
      (key, infer_element_type(values)) for key, values in column_data.items()
  ])
  return column_types


def infer_avro_schema(data, use_fastavro=False):
  """
  TODO: For internal use only.
  """
  _typehint_to_avro_type = {
      typehints.Union[[int]]: "int",
      typehints.Union[[int, type(None)]]: ["int", "null"],
      typehints.Union[[float]]: "double",
      typehints.Union[[float, type(None)]]: ["double", "null"],
      typehints.Union[[str]]: "string",
      typehints.Union[[str, type(None)]]: ["string", "null"],
      typehints.Union[[unicode]]: "string",
      typehints.Union[[unicode, type(None)]]: ["string", "null"],
      typehints.Union[[bytes]]: "bytes",
      typehints.Union[[bytes, type(None)]]: ["bytes", "null"],
      typehints.Union[[np.ndarray]]: "bytes",
      typehints.Union[[np.ndarray, type(None)]]: ["bytes", "null"],
      typehints.Union[[array.array]]: "bytes",
      typehints.Union[[array.array, type(None)]]: ["bytes", "null"],
  }

  column_types = infer_typehints_schema(data)
  avro_fields = [{
      "name": str(key),
      "type": _typehint_to_avro_type[value]
  } for key, value in column_types.items()]
  schema_dict = {
      "namespace": "example.avro",
      "name": "User",
      "type": "record",
      "fields": avro_fields
  }
  if use_fastavro:
    from fastavro import parse_schema
    return parse_schema(schema_dict)
  else:
    return Parse(json.dumps(schema_dict))


def infer_pyarrow_schema(data):
  """
  TODO: For internal use only.
  """
  column_data = OrderedDict()
  for row in data:
    for key, value in row.items():
      column_data.setdefault(key, []).append(value)
  column_types = OrderedDict([
      (key, pa.array(value).type) for key, value in column_data.items()
  ])
  return pa.schema(list(column_types.items()))
