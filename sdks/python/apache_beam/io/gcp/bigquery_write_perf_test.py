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
A pipeline that writes data from Synthetic Source to a BigQuery table.
Besides of the standard options, there are options with special meaning:
* output_dataset - BQ dataset name.
* output_table - BQ table name. The table will be removed after test completion,
* input_options - options for Synthetic Source:
num_records - number of rows to be inserted,
value_size - the length of a single row,
key_size - required option, but its value has no meaning.

Example test run on DataflowRunner:

python -m apache_beam.io.gcp.bigquery_write_perf_test \
    --test-pipeline-options="
    --runner=TestDataflowRunner
    --project=...
    --region=...
    --staging_location=gs://...
    --temp_location=gs://...
    --sdk_location=.../dist/apache-beam-x.x.x.dev0.tar.gz
    --publish_to_big_query=true
    --metrics_dataset=gs://...
    --metrics_table=...
    --output_dataset=...
    --output_table=...
    --input_options='{
    \"num_records\": 1024,
    \"key_size\": 1,
    \"value_size\": 1024,
    }'"

This setup will result in a table of 1MB size.
"""

# pytype: skip-file

from __future__ import absolute_import

import logging

from apache_beam import Map
from apache_beam import ParDo
from apache_beam.io import BigQueryDisposition
from apache_beam.io import Read
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.io.gcp.tests import utils
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.load_test_metrics_utils import CountMessages
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.synthetic_pipeline import SyntheticSource


class BigQueryWritePerfTest(LoadTest):
  def __init__(self):
    super(BigQueryWritePerfTest, self).__init__()
    self.output_dataset = self.pipeline.get_option('output_dataset')
    self.output_table = self.pipeline.get_option('output_table')

  def test(self):
    SCHEMA = parse_table_schema_from_json(
        '{"fields": [{"name": "data", "type": "BYTES"}]}')

    def format_record(record):
      # Since Synthetic Source returns data as a dictionary, we should skip one
      # of the part
      import base64
      return {'data': base64.b64encode(record[1])}

    (  # pylint: disable=expression-not-assigned
        self.pipeline
        | 'Produce rows' >> Read(
            SyntheticSource(self.parse_synthetic_source_options()))
        | 'Count messages' >> ParDo(CountMessages(self.metrics_namespace))
        | 'Format' >> Map(format_record)
        | 'Measure time' >> ParDo(MeasureTime(self.metrics_namespace))
        | 'Write to BigQuery' >> WriteToBigQuery(
            dataset=self.output_dataset,
            table=self.output_table,
            schema=SCHEMA,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE))

  def cleanup(self):
    """Removes an output BQ table."""
    utils.delete_bq_table(
        self.project_id, self.output_dataset, self.output_table)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  BigQueryWritePerfTest().run()
