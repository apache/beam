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

"""Test for the BigQuery tornadoes example."""

from __future__ import absolute_import

import logging
import unittest

import apache_beam as beam
from apache_beam.examples.cookbook import bigquery_tornadoes
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class BigQueryTornadoesTest(unittest.TestCase):

  def test_basics(self):
    with TestPipeline() as p:
      rows = (p | 'create' >> beam.Create([
          {'month': 1, 'day': 1, 'tornado': False},
          {'month': 1, 'day': 2, 'tornado': True},
          {'month': 1, 'day': 3, 'tornado': True},
          {'month': 2, 'day': 1, 'tornado': True}]))
      results = bigquery_tornadoes.count_tornadoes(rows)
      assert_that(results, equal_to([{'month': 1, 'tornado_count': 2},
                                     {'month': 2, 'tornado_count': 1}]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
