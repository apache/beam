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

try:
  from google.cloud import bigquery as gcp_bigquery
except ImportError:
  raise unittest.SkipTest('GCP dependencies are not installed')


class TestBigQueryToAvroSchema(unittest.TestCase):
  def test_convert_bigquery_schema_to_avro_schema(self):
    subfields = [
        gcp_bigquery.SchemaField(
            name="species", field_type="STRING", mode="NULLABLE"),
    ]

    fields = [
        gcp_bigquery.SchemaField(
            name="number", field_type="INTEGER", mode="REQUIRED"),
        gcp_bigquery.SchemaField(
            name="species", field_type="STRING", mode="NULLABLE"),
        gcp_bigquery.SchemaField(name="quality",
                                 field_type="FLOAT"),  # default to NULLABLE
        gcp_bigquery.SchemaField(name="grade",
                                 field_type="FLOAT64"),  # default to NULLABLE
        gcp_bigquery.SchemaField(name="quantity",
                                 field_type="INTEGER"),  # default to NULLABLE
        gcp_bigquery.SchemaField(name="dependents",
                                 field_type="INT64"),  # default to NULLABLE
        gcp_bigquery.SchemaField(
            name="birthday", field_type="TIMESTAMP", mode="NULLABLE"),
        gcp_bigquery.SchemaField(
            name="birthdayMoney", field_type="NUMERIC", mode="NULLABLE"),
        gcp_bigquery.SchemaField(
            name="flighted", field_type="BOOL", mode="NULLABLE"),
        gcp_bigquery.SchemaField(
            name="flighted2", field_type="BOOLEAN", mode="NULLABLE"),
        gcp_bigquery.SchemaField(
            name="sound", field_type="BYTES", mode="NULLABLE"),
        gcp_bigquery.SchemaField(
            name="anniversaryDate", field_type="DATE", mode="NULLABLE"),
        gcp_bigquery.SchemaField(
            name="anniversaryDatetime", field_type="DATETIME", mode="NULLABLE"),
        gcp_bigquery.SchemaField(
            name="anniversaryTime", field_type="TIME", mode="NULLABLE"),
        gcp_bigquery.SchemaField(
            name="scion",
            field_type="RECORD",
            mode="NULLABLE",
            fields=subfields),
        gcp_bigquery.SchemaField(
            name="family",
            field_type="STRUCT",
            mode="NULLABLE",
            fields=subfields),
        gcp_bigquery.SchemaField(
            name="associates",
            field_type="RECORD",
            mode="REPEATED",
            fields=subfields),
        gcp_bigquery.SchemaField(
            name="geoPositions", field_type="GEOGRAPHY", mode="NULLABLE"),
    ]

    table_schema = tuple(fields)
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
