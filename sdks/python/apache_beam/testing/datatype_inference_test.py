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

from __future__ import absolute_import

import logging
import sys
import unittest
from collections import OrderedDict

import numpy as np
from parameterized import parameterized
from past.builtins import unicode

from apache_beam.testing import datatype_inference
from apache_beam.typehints import typehints

try:
  import pyarrow as pa
except ImportError:
  pa = None

TEST_DATA = [
    {
        "name": "empty",
        "data": [],
        "type_schema": OrderedDict([]),
        "pyarrow_schema": pa.schema([]) if pa is not None else None,
        "avro_schema": {
            "namespace": "example.avro",
            "name": "User",
            "type": "record",
            "fields": [],
        },
    },
    {
        "name": "main",
        "data": [
            OrderedDict([
                ("a", 1),
                ("b", 0.12345),
                ("c", u"Hello World!!"),
                ("d", np.array([1, 2, 3])),
                ("e", b"some bytes"),
            ]),
            OrderedDict([
                ("a", -5),
                ("b", 1234.567),
                ("e", b"more bytes"),
            ]),
            OrderedDict([
                ("a", 100000),
                ("c", u"XoXoX"),
                ("d", np.array([4, 5, 6])),
                ("e", b""),
            ]),
        ],
        "type_schema": OrderedDict([
            ("a", int),
            ("b", float),
            ("c", unicode),
            ("d", np.ndarray),
            ("e", bytes),
        ]),
        "pyarrow_schema": pa.schema([
            ("a", pa.int64()),
            ("b", pa.float64()),
            ("c", pa.string()),
            ("d", pa.list_(pa.int64())),
            ("e", pa.binary()),
        ]) if pa is not None else None,
        "avro_schema": {
            "namespace": "example.avro",
            "name": "User",
            "type": "record",
            "fields": [
                {
                    "name": "a", "type": "int"
                },
                {
                    "name": "b", "type": "double"
                },
                {
                    "name": "c", "type": "string"
                },
                {
                    "name": "d", "type": "bytes"
                },
                {
                    "name": "e", "type": "bytes"
                },
            ],
        },
    },
]


def nullify_data_and_schemas(test_data):
  """Add a row with all columns set to None and adjust the schemas accordingly.
  """
  def nullify_avro_schema(schema):
    """Add a 'null' type to every field."""
    schema = schema.copy()
    new_fields = []
    for field in schema["fields"]:
      if isinstance(field["type"], str):
        new_fields.append({
            "name": field["name"], "type": sorted([field["type"], "null"])
        })
      else:
        new_fields.append({
            "name": field["name"], "type": sorted(field["type"] + ["null"])
        })
    schema["fields"] = new_fields
    return schema

  def get_collumns_in_order(test_data):
    """Get a list of columns while trying to maintain original order.

    .. note::
      Columns which do not apear until later rows are added to the end,
      even if they preceed some columns which have already been added.
    """
    _seen = set()
    columns = [
        c for test_case in test_data for row in test_case["data"] for c in row
        if c not in _seen and not _seen.add(c)
    ]
    return columns

  nullified_test_data = []
  columns = get_collumns_in_order(test_data)
  for test_case in test_data:
    if not test_case["data"]:
      continue
    test_case = test_case.copy()
    test_case["name"] = test_case["name"] + "_nullified"
    test_case["data"] = test_case["data"] + [
        OrderedDict([(c, None) for c in columns])
    ]
    test_case["type_schema"] = OrderedDict([
        (k, typehints.Union[v, type(None)]) for k,
        v in test_case["type_schema"].items()
    ])
    test_case["avro_schema"] = nullify_avro_schema(test_case["avro_schema"])
    nullified_test_data.append(test_case)
  return nullified_test_data


TEST_DATA += nullify_data_and_schemas(TEST_DATA)


class DatatypeInferenceTest(unittest.TestCase):
  @parameterized.expand([(d["name"], d["data"], d["type_schema"])
                         for d in TEST_DATA])
  def test_infer_typehints_schema(self, _, data, schema):
    typehints_schema = datatype_inference.infer_typehints_schema(data)
    self.assertEqual(typehints_schema, schema)

  @parameterized.expand([(d["name"], d["data"], d["pyarrow_schema"])
                         for d in TEST_DATA])
  @unittest.skipIf(pa is None, "PyArrow is not installed")
  @unittest.skipIf(
      sys.platform == "win32",
      "numpy array dtype is coming as int32 by default in a windows 64 bit")
  def test_infer_pyarrow_schema(self, _, data, schema):
    pyarrow_schema = datatype_inference.infer_pyarrow_schema(data)
    self.assertEqual(pyarrow_schema, schema)

  @parameterized.expand([(d["name"], d["data"], d["avro_schema"])
                         for d in TEST_DATA])
  def test_infer_avro_schema(self, _, data, schema):
    schema = schema.copy()  # Otherwise, it would be mutated by `.pop()`
    avro_schema = datatype_inference.infer_avro_schema(data, use_fastavro=False)
    avro_schema = avro_schema.to_json()
    fields1 = avro_schema.pop("fields")
    fields2 = schema.pop("fields")
    self.assertDictEqual(avro_schema, schema)
    for field1, field2 in zip(fields1, fields2):
      self.assertDictEqual(field1, field2)

  @parameterized.expand([(d["name"], d["data"], d["avro_schema"])
                         for d in TEST_DATA])
  def test_infer_fastavro_schema(self, _, data, schema):
    from fastavro import parse_schema
    schema = parse_schema(schema)
    avro_schema = datatype_inference.infer_avro_schema(data, use_fastavro=True)
    fields1 = avro_schema.pop("fields")
    fields2 = schema.pop("fields")
    self.assertDictEqual(avro_schema, schema)
    for field1, field2 in zip(fields1, fields2):
      self.assertDictEqual(field1, field2)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
