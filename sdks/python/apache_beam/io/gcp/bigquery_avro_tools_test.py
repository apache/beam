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
from __future__ import division

import json
import logging
import unittest

import avro.schema
import fastavro

from apache_beam.io.gcp import bigquery_avro_tools
from apache_beam.io.gcp import bigquery_tools
from apache_beam.io.gcp.bigquery_test import HttpError
from apache_beam.io.gcp.internal.clients import bigquery


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestBigQueryToAvroSchema(unittest.TestCase):
  def test_convert_bigquery_schema_to_avro_schema(self):
    subfields = [
        bigquery.TableFieldSchema(
            name="species", type="STRING", mode="NULLABLE"),
    ]

    fields = [
      bigquery.TableFieldSchema(
        name="number", type="INTEGER", mode="REQUIRED"),
      bigquery.TableFieldSchema(
        name="species", type="STRING", mode="NULLABLE"),
      bigquery.TableFieldSchema(
        name="quality", type="FLOAT"),  # default to NULLABLE
      bigquery.TableFieldSchema(
        name="quantity", type="INTEGER"),  # default to NULLABLE
      bigquery.TableFieldSchema(
        name="birthday", type="TIMESTAMP", mode="NULLABLE"),
      bigquery.TableFieldSchema(
        name="birthdayMoney", type="NUMERIC", mode="NULLABLE"),
      bigquery.TableFieldSchema(
        name="flighted", type="BOOL", mode="NULLABLE"),
      bigquery.TableFieldSchema(
        name="flighted2", type="BOOLEAN", mode="NULLABLE"),
      bigquery.TableFieldSchema(
        name="sound", type="BYTES", mode="NULLABLE"),
      bigquery.TableFieldSchema(
        name="anniversaryDate", type="DATE", mode="NULLABLE"),
      bigquery.TableFieldSchema(
        name="anniversaryDatetime", type="DATETIME", mode="NULLABLE"),
      bigquery.TableFieldSchema(
        name="anniversaryTime", type="TIME", mode="NULLABLE"),
      bigquery.TableFieldSchema(
        name="scion", type="RECORD", mode="NULLABLE", fields=subfields),
      bigquery.TableFieldSchema(
        name="associates", type="RECORD", mode="REPEATED", fields=subfields),
      bigquery.TableFieldSchema(
        name="geoPositions", type="GEOGRAPHY", mode="NULLABLE"),
    ]

    table_schema = bigquery.TableSchema(fields=fields)
    avro_schema = bigquery_avro_tools.get_record_schema_from_dict_table_schema(
        "root", bigquery_tools.get_dict_table_schema(table_schema))

    # Test that schema can be parsed correctly by fastavro
    fastavro.parse_schema(avro_schema)

    # Test that schema can be parsed correctly by avro
    parsed_schema = avro.schema.parse(json.dumps(avro_schema))
    # Avro RecordSchema provides field_map in py3 and fields_dict in py2
    field_map = getattr(parsed_schema, "field_map", None) or \
      getattr(parsed_schema, "fields_dict", None)

    self.assertEqual(
        field_map["number"].type, avro.schema.parse(json.dumps("long")))
    self.assertEqual(
        field_map["species"].type,
        avro.schema.parse(json.dumps(["null", "string"])))
    self.assertEqual(
        field_map["quality"].type,
        avro.schema.parse(json.dumps(["null", "double"])))
    self.assertEqual(
        field_map["quantity"].type,
        avro.schema.parse(json.dumps(["null", "long"])))
    self.assertEqual(
        field_map["birthday"].type,
        avro.schema.parse(
            json.dumps(
                ["null", {
                    "type": "long", "logicalType": "timestamp-micros"
                }])))
    self.assertEqual(
        field_map["birthdayMoney"].type,
        avro.schema.parse(
            json.dumps([
                "null",
                {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 38,
                    "scale": 9
                }
            ])))
    self.assertEqual(
        field_map["flighted"].type,
        avro.schema.parse(json.dumps(["null", "boolean"])))
    self.assertEqual(
        field_map["flighted2"].type,
        avro.schema.parse(json.dumps(["null", "boolean"])))
    self.assertEqual(
        field_map["sound"].type,
        avro.schema.parse(json.dumps(["null", "bytes"])))
    self.assertEqual(
        field_map["anniversaryDate"].type,
        avro.schema.parse(
            json.dumps(["null", {
                "type": "int", "logicalType": "date"
            }])))
    self.assertEqual(
        field_map["anniversaryDatetime"].type,
        avro.schema.parse(json.dumps(["null", "string"])))
    self.assertEqual(
        field_map["anniversaryTime"].type,
        avro.schema.parse(
            json.dumps(["null", {
                "type": "long", "logicalType": "time-micros"
            }])))
    self.assertEqual(
        field_map["geoPositions"].type,
        avro.schema.parse(json.dumps(["null", "string"])))

    self.assertEqual(
        field_map["scion"].type,
        avro.schema.parse(
            json.dumps([
                "null",
                {
                    "type": "record",
                    "name": "scion",
                    "fields": [
                        {
                            "type": ["null", "string"],
                            "name": "species",
                        },
                    ],
                    "doc": "Translated Avro Schema for scion",
                    "namespace": "apache_beam.io.gcp.bigquery.root.scion",
                },
            ])))

    self.assertEqual(
        field_map["associates"].type,
        avro.schema.parse(
            json.dumps({
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "associates",
                    "fields": [
                        {
                            "type": ["null", "string"],
                            "name": "species",
                        },
                    ],
                    "doc": "Translated Avro Schema for associates",
                    "namespace": "apache_beam.io.gcp.bigquery.root.associates",
                }
            })))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
