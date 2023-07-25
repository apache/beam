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

"""Tests for utils module."""

# pytype: skip-file

import unittest
from typing import NamedTuple
from typing import Optional
from typing import Union
from unittest.mock import patch

import pytest

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive.sql.utils import DataflowOptionsForm
from apache_beam.runners.interactive.sql.utils import find_pcolls
from apache_beam.runners.interactive.sql.utils import pformat_dict
from apache_beam.runners.interactive.sql.utils import pformat_namedtuple
from apache_beam.runners.interactive.sql.utils import register_coder_for_schema
from apache_beam.runners.interactive.sql.utils import replace_single_pcoll_token


class ANamedTuple(NamedTuple):
  a: int
  b: str


class OptionalUnionType(NamedTuple):
  unnamed: Optional[Union[int, str]]


class UtilsTest(unittest.TestCase):
  def test_register_coder_for_schema(self):
    self.assertNotIsInstance(
        beam.coders.registry.get_coder(ANamedTuple), beam.coders.RowCoder)
    register_coder_for_schema(ANamedTuple)
    self.assertIsInstance(
        beam.coders.registry.get_coder(ANamedTuple), beam.coders.RowCoder)

  def test_find_pcolls(self):
    with patch('apache_beam.runners.interactive.interactive_beam.collect',
               lambda _: None):
      found = find_pcolls(
          """SELECT * FROM pcoll_1 JOIN pcoll_2
          USING (common_column)""", {
              'pcoll_1': None, 'pcoll_2': None
          })
      self.assertIn('pcoll_1', found)
      self.assertIn('pcoll_2', found)

  def test_replace_single_pcoll_token(self):
    sql = 'SELECT * FROM abc WHERE a=1 AND b=2'
    replaced_sql = replace_single_pcoll_token(sql, 'wow')
    self.assertEqual(replaced_sql, sql)
    replaced_sql = replace_single_pcoll_token(sql, 'abc')
    self.assertEqual(
        replaced_sql, 'SELECT * FROM PCOLLECTION WHERE a=1 AND b=2')

  def test_pformat_namedtuple(self):
    actual = pformat_namedtuple(ANamedTuple)
    self.assertEqual("ANamedTuple(a: <class 'int'>, b: <class 'str'>)", actual)

  def test_pformat_namedtuple_with_unnamed_fields(self):
    actual = pformat_namedtuple(OptionalUnionType)
    # Parameters of an Union type can be in any order.
    possible_expected = (
        'OptionalUnionType(unnamed: typing.Union[int, str, NoneType])',
        'OptionalUnionType(unnamed: typing.Union[str, int, NoneType])')
    self.assertIn(actual, possible_expected)

  def test_pformat_dict(self):
    actual = pformat_dict({'a': 1, 'b': '2'})
    self.assertEqual('{\na: 1,\nb: 2\n}', actual)


@unittest.skipIf(
    not ie.current_env().is_interactive_ready,
    '[interactive] dependency is not installed.')
@pytest.mark.skipif(
    not ie.current_env().is_interactive_ready,
    reason='[interactive] dependency is not installed.')
class OptionsFormTest(unittest.TestCase):
  def test_dataflow_options_form(self):
    p = beam.Pipeline()
    pcoll = p | beam.Create([1, 2, 3])
    with patch('google.auth') as ga:
      ga.default = lambda: ['', 'default_project_id']
      df_form = DataflowOptionsForm('pcoll', pcoll)
      df_form.display_for_input()
      df_form.entries[2].input.value = 'gs://test-bucket'
      df_form.entries[3].input.value = 'a-pkg'
      options = df_form.to_options()
      cloud_options = options.view_as(GoogleCloudOptions)
      self.assertEqual(cloud_options.project, 'default_project_id')
      self.assertEqual(cloud_options.region, 'us-central1')
      self.assertEqual(
          cloud_options.staging_location, 'gs://test-bucket/staging')
      self.assertEqual(cloud_options.temp_location, 'gs://test-bucket/temp')
      self.assertIsNotNone(options.view_as(SetupOptions).requirements_file)


if __name__ == '__main__':
  unittest.main()
