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

"""
Integration tests for BigQuery's JSON data type
"""

import json
import logging
import secrets
import time
import unittest

import pytest

import apache_beam as beam
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position

try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  HttpError = None

_LOGGER = logging.getLogger(__name__)

PROJECT = 'apache-beam-testing'
# this dataset holds the canonical rows consisting json data types.
DATASET_ID = 'bq_jsontype_test_nodelete'
JSON_TABLE_NAME = 'json_data'

JSON_TABLE_DESTINATION = f"{PROJECT}:{DATASET_ID}.{JSON_TABLE_NAME}"


class BigQueryJsonIT(unittest.TestCase):
  BIGQUERY_DATASET = 'python_bigquery_json_type'

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.args = self.test_pipeline.get_full_options_as_args()
    self.project = self.test_pipeline.get_option('project')
    _LOGGER.info("mmy project: %s", self.project)

    self.bigquery_client = BigQueryWrapper()
    self.dataset_id = '%s%s%s' % (
        self.BIGQUERY_DATASET, str(int(time.time())), secrets.token_hex(3))
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    _LOGGER.info(
        "Created dataset %s in project %s", self.dataset_id, self.project)

  def tearDown(self):
    request = bigquery.BigqueryDatasetsDeleteRequest(
        projectId=self.project, datasetId=self.dataset_id, deleteContents=True)
    try:
      _LOGGER.info(
          "Deleting dataset %s in project %s", self.dataset_id, self.project)
      self.bigquery_client.client.datasets.Delete(request)
    except HttpError:
      _LOGGER.debug(
          'Failed to clean up dataset %s in project %s',
          self.dataset_id,
          self.project)

  def run_test_write(self, write_method, table):
    json_table_schema = self.generate_schema()
    rows = []
    json_data = self.generate_data()
    for country_code, country in json_data.items():
      cities_to_write = []
      for city_name, city in country["cities"].items():
        cities_to_write.append({'city_name': city_name, 'city': city})

      rows.append({
          'country_code': country_code,
          'country': country["country"],
          'stats': country["stats"],
          'cities': cities_to_write,
          'landmarks': country["landmarks"]
      })

    with beam.Pipeline(argv=self.args) as p:
      _ = (
          p
          | "Create rows with JSON data" >> beam.Create(rows)
          | "Write to BigQuery" >> WriteToBigQuery(
              method=write_method,
              table=table,
              schema=json_table_schema,
              temp_file_format="AVRO",
              custom_gcs_temp_location="gs://bigqueryio-json-it-temp",
              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
          ))

    self.read_and_validate_rows(
        read_method=ReadFromBigQuery.Method.EXPORT, table=table)

  def read_and_validate_rows(
      self, read_method, query=None, table=JSON_TABLE_DESTINATION):
    json_data = self.generate_data()

    class CompareJson(beam.DoFn, unittest.TestCase):
      def get_json(self, data):
        obj = json.loads(data)
        while not isinstance(obj, dict):
          obj = json.loads(obj)
        return obj

      def process(self, row):
        country_code = row["country_code"]
        expected = json_data[country_code]

        # Test country (JSON String)
        country_actual = self.get_json(row["country"])
        country_expected = json.loads(expected["country"])
        self.assertTrue(country_expected == country_actual)

        # Test stats (JSON String in BigQuery struct)
        for stat, value in row["stats"].items():
          stats_actual = self.get_json(value)
          stats_expected = json.loads(expected["stats"][stat])
          self.assertTrue(stats_expected == stats_actual)

        # Test cities (JSON String in BigQuery array of structs)
        for city_row in row["cities"]:
          city = city_row["city"]
          city_name = city_row["city_name"]

          city_actual = self.get_json(city)
          city_expected = json.loads(expected["cities"][city_name])
          self.assertTrue(city_expected == city_actual)

        # Test landmarks (JSON String in BigQuery array)
        landmarks_actual = row["landmarks"]
        landmarks_expected = expected["landmarks"]
        for i in range(len(landmarks_actual)):
          l_actual = self.get_json(landmarks_actual[i])
          l_expected = json.loads(landmarks_expected[i])
          self.assertTrue(l_expected == l_actual)

    if query:
      json_query_data = self.generate_query_data()
      with beam.Pipeline(argv=self.args) as p:
        data = p | 'Read rows' >> ReadFromBigQuery(
            query=query,
            method=read_method,
            gcs_location="gs://bigqueryio-json-it-temp",
            use_standard_sql=True)
        _ = data | beam.Map(_LOGGER.info)
        assert_that(data, equal_to(json_query_data))
    else:
      with beam.Pipeline(argv=self.args) as p:
        _ = p | 'Read rows' >> ReadFromBigQuery(
            table=table,
            method=read_method,
            gcs_location="gs://bigqueryio-json-it-temp"
        ) | 'Validate rows' >> beam.ParDo(CompareJson())

  @pytest.mark.it_postcommit
  def test_direct_read(self):
    self.read_and_validate_rows(read_method=ReadFromBigQuery.Method.DIRECT_READ)

  @pytest.mark.it_postcommit
  def test_export_read(self):
    self.read_and_validate_rows(read_method=ReadFromBigQuery.Method.EXPORT)

  @pytest.mark.it_postcommit
  def test_query_read(self):
    query = (
        "SELECT "
        "country_code, "
        "country.past_leaders[2] AS past_leader, "
        "stats.gdp_per_capita[\"gdp_per_capita\"] AS gdp, "
        "cities[OFFSET(1)].city.name AS city_name, "
        "landmarks[OFFSET(1)][\"name\"] AS landmark_name "
        f"FROM `{PROJECT}.{DATASET_ID}.{JSON_TABLE_NAME}`")

    self.read_and_validate_rows(
        read_method=ReadFromBigQuery.Method.EXPORT, query=query)

  @pytest.mark.it_postcommit
  def test_streaming_inserts(self):
    streaming_table = f"{PROJECT}:{DATASET_ID}.streaming_inserts"
    self.run_test_write(
        WriteToBigQuery.Method.STREAMING_INSERTS, streaming_table)

  @pytest.mark.it_postcommit
  def test_file_loads_write(self):
    file_loads_table = f"{PROJECT}:{DATASET_ID}.file_loads"
    self.run_test_write(WriteToBigQuery.Method.FILE_LOADS, file_loads_table)

  # Schema for writing to BigQuery
  def generate_schema(self):
    from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
    from apache_beam.io.gcp.internal.clients.bigquery import TableSchema
    json_fields = [
        TableFieldSchema(name='country_code', type='STRING', mode='NULLABLE'),
        TableFieldSchema(name='country', type='JSON', mode='NULLABLE'),
        TableFieldSchema(
            name='stats',
            type='STRUCT',
            mode='NULLABLE',
            fields=[
                TableFieldSchema(
                    name="gdp_per_capita", type='JSON', mode='NULLABLE'),
                TableFieldSchema(
                    name="co2_emissions", type='JSON', mode='NULLABLE'),
            ]),
        TableFieldSchema(
            name='cities',
            type='STRUCT',
            mode='REPEATED',
            fields=[
                TableFieldSchema(
                    name="city_name", type='STRING', mode='NULLABLE'),
                TableFieldSchema(name="city", type='JSON', mode='NULLABLE'),
            ]),
        TableFieldSchema(name='landmarks', type='JSON', mode='REPEATED'),
    ]

    schema = TableSchema(fields=json_fields)

    return schema

  # Expected data for query test
  def generate_query_data(self):
    query_data = [{
        'country_code': 'usa',
        'past_leader': '\"George W. Bush\"',
        'gdp': '58559.675',
        'city_name': '\"Los Angeles\"',
        'landmark_name': '\"Golden Gate Bridge\"'
    },
                  {
                      'country_code': 'aus',
                      'past_leader': '\"Kevin Rudd\"',
                      'gdp': '58043.581',
                      'city_name': '\"Melbourne\"',
                      'landmark_name': '\"Great Barrier Reef\"'
                  },
                  {
                      'country_code': 'special',
                      'past_leader': '\"!@#$%^&*()_+\"',
                      'gdp': '421.7',
                      'city_name': '\"Bikini Bottom\"',
                      'landmark_name': "\"Willy Wonka's Factory\""
                  }]
    return query_data

  def generate_data(self):
    # Raw country data
    usa = {
        "name": "United States of America",
        "population": 329484123,
        "cities": {
            "nyc": {
                "name": "New York City", "state": "NY", "population": 8622357
            },
            "la": {
                "name": "Los Angeles", "state": "CA", "population": 4085014
            },
            "chicago": {
                "name": "Chicago", "state": "IL", "population": 2670406
            },
        },
        "past_leaders": [
            "Donald Trump", "Barack Obama", "George W. Bush", "Bill Clinton"
        ],
        "in_northern_hemisphere": True
    }

    aus = {
        "name": "Australia",
        "population": 25687041,
        "cities": {
            "sydney": {
                "name": "Sydney",
                "state": "New South Wales",
                "population": 5367206
            },
            "melbourne": {
                "name": "Melbourne", "state": "Victoria", "population": 5159211
            },
            "brisbane": {
                "name": "Brisbane",
                "state": "Queensland",
                "population": 2560720
            }
        },
        "past_leaders": [
            "Malcolm Turnbull",
            "Tony Abbot",
            "Kevin Rudd",
        ],
        "in_northern_hemisphere": False
    }

    special = {
        "name": "newline\n, form\f, tab\t, \"quotes\", "
        "\\backslash\\, backspace\b, \u0000_hex_\u0f0f",
        "population": -123456789,
        "cities": {
            "basingse": {
                "name": "Ba Sing Se",
                "state": "The Earth Kingdom",
                "population": 200000
            },
            "bikinibottom": {
                "name": "Bikini Bottom",
                "state": "The Pacific Ocean",
                "population": 50000
            }
        },
        "past_leaders": [
            "1",
            "2",
            "!@#$%^&*()_+",
        ],
        "in_northern_hemisphere": True
    }

    landmarks = {
        "usa_0": {
            "name": "Statue of Liberty", "cool rating": None
        },
        "usa_1": {
            "name": "Golden Gate Bridge", "cool rating": "very cool"
        },
        "usa_2": {
            "name": "Grand Canyon", "cool rating": "very very cool"
        },
        "aus_0": {
            "name": "Sydney Opera House", "cool rating": "amazing"
        },
        "aus_1": {
            "name": "Great Barrier Reef", "cool rating": None
        },
        "special_0": {
            "name": "Hogwarts School of WitchCraft and Wizardry",
            "cool rating": "magical"
        },
        "special_1": {
            "name": "Willy Wonka's Factory", "cool rating": None
        },
        "special_2": {
            "name": "Rivendell", "cool rating": "precious"
        },
    }
    stats = {
        "usa_gdp_per_capita": {
            "gdp_per_capita": 58559.675, "currency": "constant 2015 US$"
        },
        "usa_co2_emissions": {
            "co2 emissions": 15.241,
            "measurement": "metric tons per capita",
            "year": 2018
        },
        "aus_gdp_per_capita": {
            "gdp_per_capita": 58043.581, "currency": "constant 2015 US$"
        },
        "aus_co2_emissions": {
            "co2 emissions": 15.476,
            "measurement": "metric tons per capita",
            "year": 2018
        },
        "special_gdp_per_capita": {
            "gdp_per_capita": 421.70, "currency": "constant 200 BC gold"
        },
        "special_co2_emissions": {
            "co2 emissions": -10.79,
            "measurement": "metric tons per capita",
            "year": 2018
        }
    }

    data = {
        "usa": {
            "country": json.dumps(usa),
            "cities": {
                "nyc": json.dumps(usa["cities"]["nyc"]),
                "la": json.dumps(usa["cities"]["la"]),
                "chicago": json.dumps(usa["cities"]["chicago"])
            },
            "landmarks": [
                json.dumps(landmarks["usa_0"]),
                json.dumps(landmarks["usa_1"]),
                json.dumps(landmarks["usa_2"])
            ],
            "stats": {
                "gdp_per_capita": json.dumps(stats["usa_gdp_per_capita"]),
                "co2_emissions": json.dumps(stats["usa_co2_emissions"])
            }
        },
        "aus": {
            "country": json.dumps(aus),
            "cities": {
                "sydney": json.dumps(aus["cities"]["sydney"]),
                "melbourne": json.dumps(aus["cities"]["melbourne"]),
                "brisbane": json.dumps(aus["cities"]["brisbane"])
            },
            "landmarks": [
                json.dumps(landmarks["aus_0"]), json.dumps(landmarks["aus_1"])
            ],
            "stats": {
                "gdp_per_capita": json.dumps(stats["aus_gdp_per_capita"]),
                "co2_emissions": json.dumps(stats["aus_co2_emissions"])
            }
        },
        "special": {
            "country": json.dumps(special),
            "cities": {
                "basingse": json.dumps(special["cities"]["basingse"]),
                "bikinibottom": json.dumps(special["cities"]["bikinibottom"])
            },
            "landmarks": [
                json.dumps(landmarks["special_0"]),
                json.dumps(landmarks["special_1"]),
                json.dumps(landmarks["special_2"])
            ],
            "stats": {
                "gdp_per_capita": json.dumps(stats["special_gdp_per_capita"]),
                "co2_emissions": json.dumps(stats["special_co2_emissions"])
            }
        }
    }
    return data


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
