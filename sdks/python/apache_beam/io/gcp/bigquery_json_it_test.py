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

import logging
import unittest
import json

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

_LOGGER = logging.getLogger(__name__)


class BigQueryJsonIT(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.test_pipeline = TestPipeline(is_integration_test=True)
    cls.args = cls.test_pipeline.get_full_options_as_args()

  def read_and_validate_rows(self, options):
    json_data = self.generate_data()
    class CompareJson(beam.DoFn, unittest.TestCase):
      def process(self, row):
        country_code = row["country_code"]
        expected = json_data[country_code]

        # Test country (JSON String)
        country_actual = json.loads(row["country"])
        country_expected = json.loads(expected["country"])
        self.assertTrue(country_expected == country_actual)

        # Test stats (JSON String in BigQuery struct)
        for stat, value in row["stats"].items():
          stats_actual = json.loads(value)
          stats_expected = json.loads(expected["stats"][stat])
          self.assertTrue(stats_expected == stats_actual)

        # Test cities (JSON String in BigQuery array of structs)
        for city_row in row["cities"]:
          city = city_row["city"]
          city_name = city_row["city_name"]

          city_actual = json.loads(city)
          city_expected = json.loads(expected["cities"][city_name])
          self.assertTrue(city_expected == city_actual)

        # Test landmarks (JSON String in BigQuery array)
        landmarks_actual = row["landmarks"]
        landmarks_expected = expected["landmarks"]
        for i in range(len(landmarks_actual)):
          l_actual = json.loads(landmarks_actual[i])
          l_expected = json.loads(landmarks_expected[i])
          self.assertTrue(l_expected == l_actual)

  # Expected data for query test
  def generate_query_data(self):
    JSON_QUERY_DATA = [
      {'country_code': 'usa',
       'past_leader': '\"George W. Bush\"',
       'gdp': '58559.675',
       'city_name': '\"Los Angeles\"',
       'landmark_name': '\"Golden Gate Bridge\"'},
      {'country_code': 'aus',
       'past_leader': '\"Kevin Rudd\"',
       'gdp': '58043.581',
       'city_name': '\"Melbourne\"',
       'landmark_name': '\"Great Barrier Reef\"'},
      {'country_code': 'special',
       'past_leader': '\"!@#$%^&*()_+\"',
       'gdp': '421.7',
       'city_name': '\"Bikini Bottom\"',
       'landmark_name': "\"Willy Wonka's Factory\""}
    ]
    return JSON_QUERY_DATA

  def generate_data(self):
    # Raw country data
    usa = {
      "name": "United States of America",
      "population": 329484123,
      "cities": {
        "nyc": {
          "name": "New York City",
          "state": "NY",
          "population": 8622357
        },
        "la": {
          "name": "Los Angeles",
          "state": "CA",
          "population": 4085014
        },
        "chicago": {
          "name": "Chicago",
          "state": "IL",
          "population": 2670406
        },
      },
      "past_leaders": [
        "Donald Trump",
        "Barack Obama",
        "George W. Bush",
        "Bill Clinton"
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
          "name": "Melbourne",
          "state": "Victoria",
          "population": 5159211
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
      "name": "newline\n, form\f, tab\t, \"quotes\", \\backslash\\, backspace\b, \u0000_hex_\u0f0f",
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
        "name": "Statue of Liberty",
        "cool rating": None
      },
      "usa_1": {
        "name": "Golden Gate Bridge",
        "cool rating": "very cool"
      },
      "usa_2": {
        "name": "Grand Canyon",
        "cool rating": "very very cool"
      },
      "aus_0": {
        "name": "Sydney Opera House",
        "cool rating": "amazing"
      },
      "aus_1": {
        "name": "Great Barrier Reef",
        "cool rating": None
      },
      "special_0": {
        "name": "Hogwarts School of WitchCraft and Wizardry",
        "cool rating": "magical"
      },
      "special_1": {
        "name": "Willy Wonka's Factory",
        "cool rating": None
      },
      "special_2": {
        "name": "Rivendell",
        "cool rating": "precious"
      },
    }
    stats = {
      "usa_gdp_per_capita": {
        "gdp_per_capita": 58559.675,
        "currency": "constant 2015 US$"
      },
      "usa_co2_emissions": {
        "co2 emissions": 15.241,
        "measurement": "metric tons per capita",
        "year": 2018
      },
      "aus_gdp_per_capita": {
        "gdp_per_capita": 58043.581,
        "currency": "constant 2015 US$"
      },
      "aus_co2_emissions": {
        "co2 emissions": 15.476,
        "measurement": "metric tons per capita",
        "year": 2018
      },
      "special_gdp_per_capita": {
        "gdp_per_capita": 421.70,
        "currency": "constant 200 BC gold"
      },
      "special_co2_emissions": {
        "co2 emissions": -10.79,
        "measurement": "metric tons per capita",
        "year": 2018
      }
    }

    JSON_DATA = {
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
          json.dumps(landmarks["aus_0"]),
          json.dumps(landmarks["aus_1"])
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
    return JSON_DATA

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()