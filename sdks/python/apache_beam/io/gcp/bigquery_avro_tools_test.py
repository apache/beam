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

import logging
import unittest

from fastavro.schema import parse_schema

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
        bigquery.TableFieldSchema(name="quality",
                                  type="FLOAT"),  # default to NULLABLE
        bigquery.TableFieldSchema(name="grade",
                                  type="FLOAT64"),  # default to NULLABLE
        bigquery.TableFieldSchema(name="quantity",
                                  type="INTEGER"),  # default to NULLABLE
        bigquery.TableFieldSchema(name="dependents",
                                  type="INT64"),  # default to NULLABLE
        bigquery.TableFieldSchema(
            name="birthday", type="TIMESTAMP", mode="NULLABLE"),
        bigquery.TableFieldSchema(
            name="birthdayMoney", type="NUMERIC", mode="NULLABLE"),
        bigquery.TableFieldSchema(
            name="flighted", type="BOOL", mode="NULLABLE"),
        bigquery.TableFieldSchema(
            name="flighted2", type="BOOLEAN", mode="NULLABLE"),
        bigquery.TableFieldSchema(name="sound", type="BYTES", mode="NULLABLE"),
        bigquery.TableFieldSchema(
            name="anniversaryDate", type="DATE", mode="NULLABLE"),
        bigquery.TableFieldSchema(
            name="anniversaryDatetime", type="DATETIME", mode="NULLABLE"),
        bigquery.TableFieldSchema(
            name="anniversaryTime", type="TIME", mode="NULLABLE"),
        bigquery.TableFieldSchema(
            name="scion", type="RECORD", mode="NULLABLE", fields=subfields),
        bigquery.TableFieldSchema(
            name="family", type="STRUCT", mode="NULLABLE", fields=subfields),
        bigquery.TableFieldSchema(
            name="associates", type="RECORD", mode="REPEATED",
            fields=subfields),
        bigquery.TableFieldSchema(
            name="geoPositions", type="GEOGRAPHY", mode="NULLABLE"),
    ]

    table_schema = bigquery.TableSchema(fields=fields)
    avro_schema = bigquery_avro_tools.get_record_schema_from_dict_table_schema(
        "root", bigquery_tools.get_dict_table_schema(table_schema))

    parsed_schema = parse_schema(avro_schema)
    self.assertEqual(type(parsed_schema), dict)
    # names: key -> name, value ->  different types allowed
    names = {
        "number": 4,
        "species": 2,
        "quality": 2,
        "grade": 2,
        "quantity": 2,
        "dependents": 2,
        "birthday": 2,
        "birthdayMoney": 2,
        "flighted": 2,
        "flighted2": 2,
        "sound": 2,
        "anniversaryDate": 2,
        "anniversaryDatetime": 2,
        "anniversaryTime": 2,
        "scion": 2,
        "family": 2,
        "associates": 2,
        "geoPositions": 2,
    }
    # simple test case to check if the schema is parsed right.
    fields = parsed_schema["fields"]
    for i in range(len(fields)):
      field_ = fields[i]
      assert 'name' in field_ and field_['name'] in names
      self.assertEqual(len(field_['type']), names[field_['name']])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
