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
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from unittest import mock

from hamcrest.core import assert_that as hamcrest_assert
from hamcrest.library.collection import has_items

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.yaml import cache_provider_artifacts
from apache_beam.yaml import main


def check_output(expected: List[str], matcher: Callable[..., None]):
  def _check_inner(actual: PCollection[str]):
    formatted_actual = actual | beam.Map(
        lambda row: str(beam.Row(**row._asdict())))
    matcher(formatted_actual, expected)

  return _check_inner


def create_test_method(
    pipeline_spec_file: str,
    custom_matcher: Optional[Callable[..., None]] = None):
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
  _custom_matchers: Dict[str, Callable[..., None]] = {}

  def __init__(self, name: str, path: str):
    self._test_suite = self.create_test_suite(name, path)

  def run(self):
    return self._test_suite

  @classmethod
  def parse_test_methods(cls, path: str):
    files = glob.glob(path)
    if not files and os.path.exists(path) and os.path.isfile(path):
      files = [path]
    for file in files:
      test_name = f'test_{file.split("/")[-1].replace(".", "_")}'
      custom_matcher = cls._custom_matchers.get(test_name, None)
      yield test_name, create_test_method(file, custom_matcher)

  @classmethod
  def create_test_suite(cls, name: str, path: str):
    return type(name, (unittest.TestCase, ), dict(cls.parse_test_methods(path)))

  @classmethod
  def register_custom_matcher(cls, test_name: str):
    def apply(matcher):
      cls._custom_matchers[test_name] = matcher
      return matcher

    return apply


@YamlExamplesTestSuite.register_custom_matcher('test_wordcount_minimal_yaml')
def _at_least_matcher(actual: PCollection[str], expected: List[str]):
  def _matches(value):
    hamcrest_assert(value, has_items(*expected))

  return assert_that(actual, _matches)


YAML_DOCS_DIR = os.path.join(os.path.join(os.path.dirname(__file__), 'docs'))
ExamplesTest = YamlExamplesTestSuite('ExamplesTest', r'examples/*.yaml').run()

ElementWiseTest = YamlExamplesTestSuite(
    'ElementwiseExamplesTest', r'examples/transforms/elementwise/*.yaml').run()

AggregationTest = YamlExamplesTestSuite(
    'AggregationExamplesTest', r'examples/transforms/aggregation/*.yaml').run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
