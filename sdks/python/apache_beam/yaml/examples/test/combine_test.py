# coding=utf-8
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

import unittest

from apache_beam.yaml.examples.test.test_yaml_example import test_yaml_example


class CombineYamlTest(unittest.TestCase):
  def test_combine_max_minimal_yaml(self):
    YAML_FILE = '../transforms/aggregation/combine_max_minimal.yaml'

    EXPECTED = [
        "{'amount': 3, 'produce': '🥕'}",
        "{'amount': 1, 'produce': '🍆'}",
        "{'amount': 5, 'produce': '🍅'}"
    ]
    test_yaml_example(YAML_FILE, EXPECTED)

  def test_combine_mean_minimal_yaml(self):
    YAML_FILE = '../transforms/aggregation/combine_mean_minimal.yaml'

    EXPECTED = [
        "{'amount': 2.5, 'produce': '🥕'}",
        "{'amount': 1, 'produce': '🍆'}",
        "{'amount': 4, 'produce': '🍅'}"
    ]
    test_yaml_example(YAML_FILE, EXPECTED)

  def test_combine_min_minimal_yaml(self):
    YAML_FILE = '../transforms/aggregation/combine_min_minimal.yaml'

    EXPECTED = [
        "{'amount': 2, 'produce': '🥕'}",
        "{'amount': 1, 'produce': '🍆'}",
        "{'amount': 3, 'produce': '🍅'}"
    ]
    test_yaml_example(YAML_FILE, EXPECTED)

  def test_combine_sum_minimal_yaml(self):
    YAML_FILE = '../transforms/aggregation/combine_sum_minimal.yaml'

    EXPECTED = [
        "{'amount': 5, 'produce': '🥕'}",
        "{'amount': 1, 'produce': '🍆'}",
        "{'amount': 12, 'produce': '🍅'}"
    ]
    test_yaml_example(YAML_FILE, EXPECTED)

  def test_combine_sum_yaml(self):
    YAML_FILE = '../transforms/aggregation/combine_sum.yaml'

    EXPECTED = [
        "{'fruit': 'raspberry', 'total_quantity': 1}",
        "{'fruit': 'blackberry', 'total_quantity': 1}",
        "{'fruit': 'blueberry', 'total_quantity': 3}",
        "{'fruit': 'banana', 'total_quantity': 3}"
    ]
    test_yaml_example(YAML_FILE, EXPECTED)

  def test_combine_count_minimal_yaml(self):
    YAML_FILE = '../transforms/aggregation/combine_count_minimal.yaml'

    EXPECTED = [
        "{'produce': 4, 'season': 'spring'}",
        "{'produce': 3, 'season': 'summer'}",
        "{'produce': 2, 'season': 'fall'}",
        "{'produce': 1, 'season': 'winter'}"
    ]
    test_yaml_example(YAML_FILE, EXPECTED)

  def test_combine_multiple_aggregations_yaml(self):
    YAML_FILE = '../transforms/aggregation/combine_multiple_aggregations.yaml'

    EXPECTED = ["{'min_price': 1.0, 'mean_price': 2.5, 'max_price': 4.0}"]
    test_yaml_example(YAML_FILE, EXPECTED)

  def test_group_into_batches_yaml(self):
    YAML_FILE = '../transforms/aggregation/group_into_batches.yaml'

    EXPECTED = [
        "{'produce': ['🥕', '🍓', '🍆'], 'season': 'spring'}",
        "{'produce': ['🥕', '🍅', '🌽'], 'season': 'summer'}",
        "{'produce': ['🥕', '🍅'], 'season': 'fall'}",
        "{'produce': ['🍆'], 'season': 'winter'}"
    ]
    test_yaml_example(YAML_FILE, EXPECTED)

  def test_top_largest_per_key_yaml(self):
    YAML_FILE = '../transforms/aggregation/top_largest_per_key.yaml'

    EXPECTED = [
        "{'biggest': [3, 2], 'produce': '🥕'}",
        "{'biggest': [1], 'produce': '🍆'}",
        "{'biggest': [5, 4], 'produce': '🍅'}"
    ]
    test_yaml_example(YAML_FILE, EXPECTED)

  def test_top_smallest_per_key_yaml(self):
    YAML_FILE = '../transforms/aggregation/top_smallest_per_key.yaml'

    EXPECTED = [
        "{'produce': '🥕', 'smallest': [2, 3]}",
        "{'produce': '🍆', 'smallest': [1]}",
        "{'produce': '🍅', 'smallest': [3, 4]}"
    ]
    test_yaml_example(YAML_FILE, EXPECTED)


if __name__ == '__main__':
  unittest.main()
