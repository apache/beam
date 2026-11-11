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

"""Tests for the Apache Beam Python TPC-DS suite with cross-language SQL and PrismRunner."""

import logging
import unittest

from apache_beam.testing.benchmarks.tpcds import tpcds_launcher
from apache_beam.testing.benchmarks.tpcds import tpcds_queries
from apache_beam.testing.benchmarks.tpcds import tpcds_schemas


class TpcdsUnitTest(unittest.TestCase):
  """Unit tests for TPC-DS benchmark suite."""
  def test_schema_coercion(self):
    self.assertEqual(tpcds_schemas.coerce_value("42", "integer"), 42)
    self.assertEqual(tpcds_schemas.coerce_value("123", "identifier"), 123)
    self.assertEqual(
        str(tpcds_schemas.coerce_value("19.99", "decimal(7,2)")), "19.99")
    self.assertEqual(tpcds_schemas.coerce_value("abc", "varchar(20)"), "abc")
    self.assertIsNone(tpcds_schemas.coerce_value("", "integer"))
    self.assertIsNone(tpcds_schemas.coerce_value(None, "decimal(7,2)"))

  def test_known_failing_queries_map_valid(self):
    all_queries = set(tpcds_queries.get_all_query_names())
    supported_queries = set(tpcds_queries.parse_query_selection("supported"))
    failing_queries = set(tpcds_queries.KNOWN_FAILING_QUERIES.keys())
    for qname, expected_err in tpcds_queries.KNOWN_FAILING_QUERIES.items():
      self.assertIn(qname, all_queries)
      self.assertTrue(len(expected_err) > 0)
    self.assertTrue(
        tpcds_queries.PLANNER_TIMEOUT_QUERIES.issubset(failing_queries))
    self.assertEqual(supported_queries & failing_queries, set())
    self.assertEqual(supported_queries | failing_queries, all_queries)

  def test_query_selection_and_normalization(self):
    qnames = tpcds_queries.parse_query_selection("canonical13")
    self.assertEqual(len(qnames), 13)
    self.assertEqual(tpcds_queries.normalize_query_name("03"), "query3")
    self.assertEqual(tpcds_queries.normalize_query_name("q14a"), "query14a")
    self.assertEqual(
        tpcds_queries.parse_query_selection("3, 7, query96"),
        ["query3", "query7", "query96"])
    with self.assertRaises(ValueError):
      tpcds_queries.parse_query_selection("query999")

  def test_supported_data_sizes(self):
    self.assertEqual(
        tpcds_launcher.check_and_normalize_data_size("10M"), "10MB")
    self.assertEqual(
        tpcds_launcher.check_and_normalize_data_size("10MB"), "10MB")
    self.assertEqual(tpcds_launcher.check_and_normalize_data_size("1G"), "1GB")
    self.assertEqual(tpcds_launcher.check_and_normalize_data_size("1GB"), "1GB")
    with self.assertRaises(ValueError):
      tpcds_launcher.check_and_normalize_data_size("5G")


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
