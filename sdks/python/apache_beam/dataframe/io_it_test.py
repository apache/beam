#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

"""Integration tests for Dataframe sources and sinks."""
# pytype: skip-file

import logging
import unittest

import pytest

import apache_beam.io.gcp.bigquery
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

_LOGGER = logging.getLogger(__name__)

try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  HttpError = None


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class ReadUsingReadGbqTests(unittest.TestCase):
  @pytest.mark.it_postcommit
  def test_ReadGbq(self):
    from apache_beam.dataframe import convert
    with TestPipeline(is_integration_test=True) as p:
      actual_df = p | apache_beam.dataframe.io.read_gbq(
          table="apache-beam-testing:beam_bigquery_io_test."
          "dfsqltable_3c7d6fd5_16e0460dfd0",
          use_bqstorage_api=False)
      assert_that(
          convert.to_pcollection(actual_df),
          equal_to([(3, 'customer1', 'test'), (1, 'customer1', 'test'),
                    (2, 'customer2', 'test'), (4, 'customer2', 'test')]))

  @pytest.mark.it_postcommit
  def test_ReadGbq_export_with_project(self):
    from apache_beam.dataframe import convert
    with TestPipeline(is_integration_test=True) as p:
      actual_df = p | apache_beam.dataframe.io.read_gbq(
          table="dfsqltable_3c7d6fd5_16e0460dfd0",
          dataset="beam_bigquery_io_test",
          project_id="apache-beam-testing",
          use_bqstorage_api=False)
      assert_that(
          convert.to_pcollection(actual_df),
          equal_to([(3, 'customer1', 'test'), (1, 'customer1', 'test'),
                    (2, 'customer2', 'test'), (4, 'customer2', 'test')]))

  @pytest.mark.it_postcommit
  def test_ReadGbq_direct_read(self):
    from apache_beam.dataframe import convert
    with TestPipeline(is_integration_test=True) as p:
      actual_df = p | apache_beam.dataframe.io.\
          read_gbq(
          table=
          "apache-beam-testing:beam_bigquery_io_test."
          "dfsqltable_3c7d6fd5_16e0460dfd0",
          use_bqstorage_api=True)
      assert_that(
          convert.to_pcollection(actual_df),
          equal_to([(3, 'customer1', 'test'), (1, 'customer1', 'test'),
                    (2, 'customer2', 'test'), (4, 'customer2', 'test')]))

  @pytest.mark.it_postcommit
  def test_ReadGbq_direct_read_with_project(self):
    from apache_beam.dataframe import convert
    with TestPipeline(is_integration_test=True) as p:
      actual_df = p | apache_beam.dataframe.io.read_gbq(
          table="dfsqltable_3c7d6fd5_16e0460dfd0",
          dataset="beam_bigquery_io_test",
          project_id="apache-beam-testing",
          use_bqstorage_api=True)
      assert_that(
          convert.to_pcollection(actual_df),
          equal_to([(3, 'customer1', 'test'), (1, 'customer1', 'test'),
                    (2, 'customer2', 'test'), (4, 'customer2', 'test')]))

  @pytest.mark.it_postcommit
  def test_ReadGbq_with_computation(self):
    from apache_beam.dataframe import convert
    with TestPipeline(is_integration_test=True) as p:
      beam_df = p | apache_beam.dataframe.io.read_gbq(
          table="dfsqltable_3c7d6fd5_16e0460dfd0",
          dataset="beam_bigquery_io_test",
          project_id="apache-beam-testing")
      actual_df = beam_df.groupby('id').count()
      assert_that(
          convert.to_pcollection(actual_df, include_indexes=True),
          equal_to([(1, 1, 1), (2, 1, 1), (3, 1, 1), (4, 1, 1)]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
