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

"""Tests for beam_sql_magics module."""

# pytype: skip-file

import unittest
from unittest.mock import patch

import pytest

import apache_beam as beam
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie

try:
  from apache_beam.runners.interactive.sql.beam_sql_magics import _build_query_components
  from apache_beam.runners.interactive.sql.beam_sql_magics import _generate_output_name
except (ImportError, NameError):
  pass  # The test is to be skipped because [interactive] dep not installed.


@unittest.skipIf(
    not ie.current_env().is_interactive_ready,
    '[interactive] dependency is not installed.')
@pytest.mark.skipif(
    not ie.current_env().is_interactive_ready,
    reason='[interactive] dependency is not installed.')
class BeamSqlMagicsTest(unittest.TestCase):
  def test_generate_output_name_when_not_provided(self):
    output_name = None
    self.assertTrue(
        _generate_output_name(output_name, '', {}).startswith('sql_output_'))

  def test_use_given_output_name_when_provided(self):
    output_name = 'output'
    self.assertEqual(_generate_output_name(output_name, '', {}), output_name)

  def test_build_query_components_when_no_pcoll_queried(self):
    query = """SELECT CAST(1 AS INT) AS `id`,
                      CAST('foo' AS VARCHAR) AS `str`,
                      CAST(3.14  AS DOUBLE) AS `flt`"""
    processed_query, sql_source = _build_query_components(query, {})
    self.assertEqual(processed_query, query)
    self.assertIsInstance(sql_source, beam.Pipeline)

  def test_build_query_components_when_single_pcoll_queried(self):
    p = beam.Pipeline()
    target = p | beam.Create([1, 2, 3])
    ib.watch(locals())
    query = 'SELECT * FROM target where a=1'
    found = {'target': target}

    with patch('apache_beam.runners.interactive.sql.beam_sql_magics.'
               'pcoll_from_file_cache',
               lambda a,
               b,
               c,
               d: target):
      processed_query, sql_source = _build_query_components(query, found)

      self.assertEqual(processed_query, 'SELECT * FROM PCOLLECTION where a=1')
      self.assertIsInstance(sql_source, beam.PCollection)

  def test_build_query_components_when_multiple_pcolls_queried(self):
    p = beam.Pipeline()
    pcoll_1 = p | 'Create 1' >> beam.Create([1, 2, 3])
    pcoll_2 = p | 'Create 2' >> beam.Create([4, 5, 6])
    ib.watch(locals())
    query = 'SELECT * FROM pcoll_1 JOIN pcoll_2 USING (a)'
    found = {'pcoll_1': pcoll_1, 'pcoll_2': pcoll_2}

    with patch('apache_beam.runners.interactive.sql.beam_sql_magics.'
               'pcoll_from_file_cache',
               lambda a,
               b,
               c,
               d: pcoll_1):
      processed_query, sql_source = _build_query_components(query, found)

      self.assertEqual(processed_query, query)
      self.assertIsInstance(sql_source, dict)
      self.assertIn('pcoll_1', sql_source)
      self.assertIn('pcoll_2', sql_source)

  def test_build_query_components_when_unbounded_pcolls_queried(self):
    p = beam.Pipeline()
    pcoll = p | beam.io.ReadFromPubSub(
        subscription='projects/fake-project/subscriptions/fake_sub')
    ib.watch(locals())
    query = 'SELECT * FROM pcoll'
    found = {'pcoll': pcoll}

    with patch('apache_beam.runners.interactive.sql.beam_sql_magics.'
               'pcolls_from_streaming_cache',
               lambda a,
               b,
               c,
               d,
               e: found):
      _, sql_source = _build_query_components(query, found)
      self.assertIs(sql_source, pcoll)


if __name__ == '__main__':
  unittest.main()
