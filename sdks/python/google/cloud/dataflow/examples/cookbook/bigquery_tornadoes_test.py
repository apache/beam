# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test for the BigQuery tornadoes example."""

import logging
import unittest

import google.cloud.dataflow as df
from google.cloud.dataflow.examples.cookbook import bigquery_tornadoes


class BigQueryTornadoesTest(unittest.TestCase):

  def test_basics(self):
    p = df.Pipeline('DirectPipelineRunner')
    rows = (p | df.Create('create', [
        {'month': 1, 'day': 1, 'tornado': False},
        {'month': 1, 'day': 2, 'tornado': True},
        {'month': 1, 'day': 3, 'tornado': True},
        {'month': 2, 'day': 1, 'tornado': True}]))
    results = bigquery_tornadoes.count_tornadoes(rows)
    df.assert_that(results, df.equal_to([{'month': 1, 'tornado_count': 2},
                                         {'month': 2, 'tornado_count': 1}]))
    p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
