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

"""
A performance test for reading data from a Spanner database table.
Besides of the standard options, there are options with special meaning:
* spanner_instance - Spanner Instance ID.
* spanner_database - Spanner Database ID.
The table will be created and populated with data from Synthetic Source if it
does not exist.
* input_options - options for Synthetic Source:
num_records - number of rows to be inserted,
value_size - the length of a single row,
key_size - required option, but its value has no meaning.

Example test run on DataflowRunner:
python -m apache_beam.io.gcp.experimental.spannerio_read_perf_test \
  --test-pipeline-options="
  --runner=TestDataflowRunner
  --project='...'
  --region='...'
  --temp_location='gs://...'
  --sdk_location=build/apache-beam.tar.gz
  --publish_to_big_query=true
  --metrics_dataset='...'
  --metrics_table='...'
  --spanner_instance='...'
  --spanner_database='...'
  --input_options='{
    \"num_records\": 10,
    \"key_size\": 1,
    \"value_size\": 1024
    }'"

This setup will result in a table of 1MB size.
"""

from __future__ import absolute_import

import logging

from apache_beam import FlatMap
from apache_beam import Map
from apache_beam import ParDo
from apache_beam.io import Read
from apache_beam.io.gcp.experimental.spannerio import ReadFromSpanner
from apache_beam.io.gcp.experimental.spannerio import WriteToSpanner
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.load_test_metrics_utils import CountMessages
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.synthetic_pipeline import SyntheticSource
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.combiners import Count

# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.api_core.exceptions import AlreadyExists
  from google.cloud import spanner
except ImportError:
  spanner = None
  AlreadyExists = None
# pylint: enable=wrong-import-order, wrong-import-position


class SpannerReadPerfTest(LoadTest):
  def __init__(self):
    super().__init__()
    self.project = self.pipeline.get_option('project')
    self.spanner_instance = self.pipeline.get_option('spanner_instance')
    self.spanner_database = self.pipeline.get_option('spanner_database')
    self._init_setup()

  def _create_database(self):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(self.spanner_instance)
    database = instance.database(
        self.spanner_database,
        ddl_statements=[
            """CREATE TABLE test_data (
            id      STRING(99) NOT NULL,
            data    BYTES(MAX) NOT NULL
         ) PRIMARY KEY (id)""",
        ])
    database.create()

  def _init_setup(self):
    """Checks if a spanner database exists and creates it if not."""
    try:
      self._create_database()
      self._create_input_data()
    except AlreadyExists:
      # pass if the database already exists
      pass

  def _create_input_data(self):
    """
    Runs an additional pipeline which creates test data and waits for its
    completion.
    """
    def format_record(record):
      import base64
      return base64.b64encode(record[1])

    def make_insert_mutations(element):
      import uuid
      from apache_beam.io.gcp.experimental.spannerio import WriteMutation
      ins_mutation = WriteMutation.insert(
          table='test_data',
          columns=('id', 'data'),
          values=[(str(uuid.uuid1()), element)])
      return [ins_mutation]

    with TestPipeline() as p:
      (  # pylint: disable=expression-not-assigned
          p
          | 'Produce rows' >> Read(
              SyntheticSource(self.parse_synthetic_source_options()))
          | 'Format' >> Map(format_record)
          | 'Make mutations' >> FlatMap(make_insert_mutations)
          | 'Write to Spanner' >> WriteToSpanner(
            project_id=self.project,
            instance_id=self.spanner_instance,
            database_id=self.spanner_database,
            max_batch_size_bytes=5120))

  def test(self):
    output = (
        self.pipeline
        | 'Read from Spanner' >> ReadFromSpanner(
            self.project,
            self.spanner_instance,
            self.spanner_database,
            sql="select data from test_data")
        | 'Count messages' >> ParDo(CountMessages(self.metrics_namespace))
        | 'Measure time' >> ParDo(MeasureTime(self.metrics_namespace))
        | 'Count' >> Count.Globally())
    assert_that(output, equal_to([self.input_options['num_records']]))


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  SpannerReadPerfTest().run()
