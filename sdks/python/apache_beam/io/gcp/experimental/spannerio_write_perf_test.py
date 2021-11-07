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
A pipeline that writes data from Synthetic Source to a Spanner.
Besides of the standard options, there are options with special meaning:
* spanner_instance - Spanner Instance ID.
* spanner_database - Spanner Database ID.
* input_options - options for Synthetic Source:
num_records - number of rows to be inserted,
value_size - the length of a single row,
key_size - required option, but its value has no meaning.

Example test run on DataflowRunner:

python -m apache_beam.io.gcp.experimental.spannerio_write_perf_test \
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
import random
import uuid

from apache_beam import FlatMap
from apache_beam import Map
from apache_beam import ParDo
from apache_beam.io import Read
from apache_beam.io.gcp.experimental.spannerio import WriteToSpanner
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.load_test_metrics_utils import CountMessages
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.synthetic_pipeline import SyntheticSource

# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud import spanner
except ImportError:
  spanner = None
# pylint: enable=wrong-import-order, wrong-import-position


class SpannerWritePerfTest(LoadTest):
  TEST_DATABASE = None

  def __init__(self):
    super().__init__()
    self.project = self.pipeline.get_option('project')
    self.spanner_instance = self.pipeline.get_option('spanner_instance')
    self.spanner_database = self.pipeline.get_option('spanner_database')
    self._init_setup()

  def _generate_table_name(self):
    self.TEST_DATABASE = "{}_{}".format(
        self.spanner_database, ''.join(random.sample(uuid.uuid4().hex, 4)))
    return self.TEST_DATABASE

  def _create_database(self):
    spanner_client = spanner.Client()
    instance = self._SPANNER_INSTANCE = spanner_client.instance(
        self.spanner_instance)
    database = instance.database(
        self.TEST_DATABASE,
        ddl_statements=[
            """CREATE TABLE test (
            id      STRING(99) NOT NULL,
            data    BYTES(MAX) NOT NULL
         ) PRIMARY KEY (id)""",
        ])
    database.create()

  def _init_setup(self):
    """Create database."""
    self._generate_table_name()
    self._create_database()

  def test(self):
    def format_record(record):
      import base64
      return base64.b64encode(record[1])

    def make_insert_mutations(element):
      import uuid  # pylint: disable=reimported
      from apache_beam.io.gcp.experimental.spannerio import WriteMutation
      ins_mutation = WriteMutation.insert(
          table='test',
          columns=('id', 'data'),
          values=[(str(uuid.uuid1()), element)])
      return [ins_mutation]

    (  # pylint: disable=expression-not-assigned
        self.pipeline
        | 'Produce rows' >> Read(
            SyntheticSource(self.parse_synthetic_source_options()))
        | 'Count messages' >> ParDo(CountMessages(self.metrics_namespace))
        | 'Format' >> Map(format_record)
        | 'Make mutations' >> FlatMap(make_insert_mutations)
        | 'Measure time' >> ParDo(MeasureTime(self.metrics_namespace))
        | 'Write to Spanner' >> WriteToSpanner(
          project_id=self.project,
          instance_id=self.spanner_instance,
          database_id=self.TEST_DATABASE,
          max_batch_size_bytes=5120)
    )

  def cleanup(self):
    """Removes test database."""
    database = self._SPANNER_INSTANCE.database(self.TEST_DATABASE)
    database.drop()


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  SpannerWritePerfTest().run()
