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

import argparse
import json
import logging
import time
import unittest
from random import randint

import pytest

import apache_beam as beam
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.api_core import exceptions as gexc
  from google.cloud import bigquery
except ImportError:
  gexc = None
  bigquery = None
# pylint: enable=wrong-import-order, wrong-import-position

_LOGGER = logging.getLogger(__name__)

PROJECT = 'apache-beam-testing'
DATASET_ID = 'bq_jsontype_test_nodelete'
JSON_TABLE_NAME = 'json_data'

JSON_TABLE_DESTINATION = f"{PROJECT}:{DATASET_ID}.{JSON_TABLE_NAME}"

STREAMING_TEST_TABLE = "py_streaming_test" \
                       f"{time.time_ns() // 1000}_{randint(0,32)}"
FILE_LOAD_TABLE = "py_fileload_test" \
                       f"{time.time_ns() // 1000}_{randint(0,32)}"


class BigQueryJsonIT(unittest.TestCase):
  created_tables = set()

  @classmethod
  def setUpClass(cls):
    cls.test_pipeline = TestPipeline(is_integration_test=True)

  @classmethod
  def tearDownClass(cls):
    if cls.created_tables:
      client = bigquery.Client(project=PROJECT)
      for ref in cls.created_tables:
        try:
          client.delete_table(ref[len(PROJECT) + 1:])  # need dataset:table
        except gexc.NotFound:
          pass  # just skip

  def run_test_write(self, options):
    json_table_schema = self.generate_schema()
    rows_to_write = []
    json_data = self.generate_data()
    for country_code, country in json_data.items():
      cities_to_write = []
      for city_name, city in country["cities"].items():
        cities_to_write.append({'city_name': city_name, 'city': city})

      rows_to_write.append({
          'country_code': country_code,
          'country': country["country"],
          'stats': country["stats"],
          'cities': cities_to_write,
          'landmarks': country["landmarks"]
      })

    parser = argparse.ArgumentParser()
    parser.add_argument('--write_method')
    parser.add_argument('--output')
    parser.add_argument('--unescape', required=False)

    known_args, pipeline_args = parser.parse_known_args(options)
    self.created_tables.add(known_args.output)

    with beam.Pipeline(argv=pipeline_args) as p:
      _ = (
          p
          | "Create rows with JSON data" >> beam.Create(rows_to_write)
          | "Write to BigQuery" >> beam.io.WriteToBigQuery(
              method=known_args.write_method,
              table=known_args.output,
              schema=json_table_schema,
              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
          ))

    extra_opts = {
        'read_method': "EXPORT",
        'input': known_args.output,
        'unescape': known_args.unescape
    }
    read_options = self.test_pipeline.get_full_options_as_args(**extra_opts)
    self.read_and_validate_rows(read_options)

  def read_and_validate_rows(self, options):
    json_data = self.generate_data()

    parser = argparse.ArgumentParser()
    parser.add_argument('--read_method')
    parser.add_argument('--query')
    parser.add_argument('--input')
    parser.add_argument('--unescape', required=False)

    known_args, pipeline_args = parser.parse_known_args(options)

    # TODO(yathu) remove this conversion when FILE_LOAD produces unescaped
    # JSON string
    def maybe_unescape(value):
      if known_args.unescape:
        value = bytes(value, "utf-8").decode("unicode_escape")[1:-1]
      return json.loads(value)

    class CompareJson(beam.DoFn, unittest.TestCase):
      def process(self, row):
        country_code = row["country_code"]
        expected = json_data[country_code]

        # Test country (JSON String)
        country_actual = maybe_unescape(row["country"])
        country_expected = json.loads(expected["country"])

        self.assertTrue(country_expected == country_actual)

        # Test stats (JSON String in BigQuery struct)
        for stat, value in row["stats"].items():
          stats_actual = maybe_unescape(value)
          stats_expected = json.loads(expected["stats"][stat])
          self.assertTrue(stats_expected == stats_actual)

        # Test cities (JSON String in BigQuery array of structs)
        for city_row in row["cities"]:
          city = city_row["city"]
          city_name = city_row["city_name"]

          city_actual = maybe_unescape(city)
          city_expected = json.loads(expected["cities"][city_name])
          self.assertTrue(city_expected == city_actual)

        # Test landmarks (JSON String in BigQuery array)
        landmarks_actual = row["landmarks"]
        landmarks_expected = expected["landmarks"]
        for i in range(len(landmarks_actual)):
          l_actual = maybe_unescape(landmarks_actual[i])
          l_expected = json.loads(landmarks_expected[i])
          self.assertTrue(l_expected == l_actual)

    method = ReadFromBigQuery.Method.DIRECT_READ if \
      known_args.read_method == "DIRECT_READ" else \
      ReadFromBigQuery.Method.EXPORT

    if known_args.query:
      json_query_data = self.generate_query_data()
      with beam.Pipeline(argv=pipeline_args) as p:
        data = p | 'Read rows' >> ReadFromBigQuery(
            query=known_args.query, method=method, use_standard_sql=True)
        assert_that(data, equal_to(json_query_data))
    else:
      with beam.Pipeline(argv=pipeline_args) as p:
        _ = p | 'Read rows' >> ReadFromBigQuery(
            table=known_args.input,
            method=method,
        ) | 'Validate rows' >> beam.ParDo(CompareJson())

  @pytest.mark.it_postcommit
  def test_direct_read(self):
    extra_opts = {
        'read_method': "DIRECT_READ",
        'input': JSON_TABLE_DESTINATION,
    }
    options = self.test_pipeline.get_full_options_as_args(**extra_opts)

    self.read_and_validate_rows(options)

  @pytest.mark.it_postcommit
  def test_export_read(self):
    extra_opts = {
        'read_method': "EXPORT",
        'input': JSON_TABLE_DESTINATION,
    }
    options = self.test_pipeline.get_full_options_as_args(**extra_opts)

    self.read_and_validate_rows(options)

  @pytest.mark.it_postcommit
  def test_query_read(self):
    extra_opts = {
        'query': "SELECT "
        "country_code, "
        "country.past_leaders[2] AS past_leader, "
        "stats.gdp_per_capita[\"gdp_per_capita\"] AS gdp, "
        "cities[OFFSET(1)].city.name AS city_name, "
        "landmarks[OFFSET(1)][\"name\"] AS landmark_name "
        f"FROM `{PROJECT}.{DATASET_ID}.{JSON_TABLE_NAME}`",
    }
    options = self.test_pipeline.get_full_options_as_args(**extra_opts)

    self.read_and_validate_rows(options)

  @pytest.mark.it_postcommit
  def test_streaming_inserts(self):
    extra_opts = {
        'output': f"{PROJECT}:{DATASET_ID}.{STREAMING_TEST_TABLE}",
        'write_method': "STREAMING_INSERTS"
    }
    options = self.test_pipeline.get_full_options_as_args(**extra_opts)

    self.run_test_write(options)

  @pytest.mark.it_postcommit
  def test_file_loads_write(self):
    extra_opts = {
        'output': f"{PROJECT}:{DATASET_ID}.{FILE_LOAD_TABLE}",
        'write_method': "FILE_LOADS",
        "unescape": "True"
    }
    options = self.test_pipeline.get_full_options_as_args(**extra_opts)
    self.run_test_write(options)

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
