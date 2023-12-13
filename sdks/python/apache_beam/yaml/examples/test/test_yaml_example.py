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

from unittest import mock

import apache_beam as beam
from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.yaml import cache_provider_artifacts
from apache_beam.yaml import main


class FormatOutput(beam.PTransform):
  def __init__(self, element=None):
    super().__init__()
    self._element = element

  def expand(self, p):
    def get_map_format():
      if self._element is not None:
        return beam.Map(lambda row: eval(f"row.{self._element}"))  # pylint: disable=eval-used
      return beam.Map(lambda row: row._asdict())

    return p | get_map_format()


def check_output(expected, element=None, matcher=assert_matches_stdout):
  def _check_inner(actual):
    formatted_actual = actual | "Format Output" >> FormatOutput(element)
    matcher(formatted_actual, expected)

  return _check_inner


@mock.patch('apache_beam.Pipeline', TestPipeline)
def test_yaml_example(
    pipeline_spec_file, expected, element=None, matcher=assert_matches_stdout):
  cache_provider_artifacts.cache_provider_artifacts()
  main.run(
      argv=[f"--pipeline_spec_file={pipeline_spec_file}"],
      test=check_output(expected, element, matcher))
