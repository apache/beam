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
import glob
import logging
import os
import unittest
from unittest import mock

from hamcrest.core import assert_that as hamcrest_assert
from hamcrest.library.collection import has_items

import apache_beam as beam
from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.yaml import cache_provider_artifacts
from apache_beam.yaml import main


def check_output(expected, matcher):
  def _check_inner(actual):
    formatted_actual = actual | beam.Map(
        lambda row: str(beam.Row(**row._asdict())))
    matcher(formatted_actual, expected)

  return _check_inner


def create_test_method(pipeline_spec_file, custom_matcher=None):
  @mock.patch('apache_beam.Pipeline', TestPipeline)
  def test_yaml_example(self):
    with open(pipeline_spec_file) as f:
      lines = f.readlines()
      expected_key = '# Expected:\n'
      if expected_key in lines:
        expected = lines[lines.index('# Expected:\n') + 1:]
      else:
        raise ValueError(
            f"Missing '# Expected:' tag in example file '{pipeline_spec_file}'")
      for i, line in enumerate(expected):
        expected[i] = line.replace('#  ', '').replace('\n', '')

      cache_provider_artifacts.cache_provider_artifacts()
      matcher = assert_matches_stdout if not custom_matcher else custom_matcher
      main.run(
          argv=[f"--pipeline_spec_file={pipeline_spec_file}"],
          test=check_output(expected, matcher))

  return test_yaml_example


class YamlExamplesTestSuite:
  _custom_matchers = {}

  def __init__(self, name, path):
    self._test_suite = self.create_test_suite(name, path)

  def run(self):
    return self._test_suite

  @classmethod
  def parse_test_methods(cls, path):
    files = glob.glob(os.path.join(path, r'*.yaml'))
    if not files and os.path.exists(path) and os.path.isfile(path):
      files = [path]
    for file in files:
      custom_matcher = cls._custom_matchers.get(file, None)
      test_name = f'test_{file.split("/")[-1].replace(".", "_")}'
      yield test_name, create_test_method(file, custom_matcher)

  @classmethod
  def create_test_suite(cls, name, path):
    return type(name, (unittest.TestCase, ), dict(cls.parse_test_methods(path)))

  @classmethod
  def register_custom_matcher(cls, yaml_file: str):
    def apply(matcher):
      cls._custom_matchers[yaml_file] = matcher
      return matcher

    return apply


@YamlExamplesTestSuite.register_custom_matcher('./wordcount_minimal.yaml')
def _at_least_matcher(actual, expected):
  def _matches(value):
    hamcrest_assert(value, has_items(*expected))

  return assert_that(actual, _matches)


ExamplesTest = YamlExamplesTestSuite('ExamplesTest', './').run()

ElementWiseTest = YamlExamplesTestSuite(
    'ElementwiseExamplesTest', 'transforms/elementwise/').run()

AggregationTest = YamlExamplesTestSuite(
    'AggregationExamplesTest', 'transforms/aggregation/').run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
