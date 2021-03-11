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

"""Job that reads and writes data to BigQuery.

A Dataflow job that reads from BQ using a query and then writes to a
big query table at the end of the pipeline.
"""

# pytype: skip-file

# pylint: disable=wrong-import-order, wrong-import-position
from __future__ import absolute_import

import argparse

import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline


def run_bq_pipeline(argv=None):
  """Run the sample BigQuery pipeline.

  Args:
    argv: Arguments to the run function.
  """
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--query', required=True, help='Query to process for the table.')
  parser.add_argument(
      '--output', required=True, help='Output BQ table to write results to.')
  parser.add_argument(
      '--output_schema',
      dest='output_schema',
      required=True,
      help='Schema for output BQ table.')
  parser.add_argument(
      '--use_standard_sql',
      action='store_true',
      dest='use_standard_sql',
      help='Output BQ table to write results to.')
  parser.add_argument(
      '--kms_key', default=None, help='Use this Cloud KMS key with BigQuery.')
  parser.add_argument(
      '--native',
      default=False,
      action='store_true',
      help='Use NativeSources and Sinks.')
  parser.add_argument(
      '--use_json_exports',
      default=False,
      action='store_true',
      help='Use JSON as the file format for exports.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  table_schema = parse_table_schema_from_json(known_args.output_schema)
  kms_key = known_args.kms_key

  options = PipelineOptions(pipeline_args)
  p = TestPipeline(options=options)

  # Note to future modifiers: Keep using BigQuerySource if known_args.native is
  # True.
  if known_args.native:
    data = p | 'read' >> beam.io.Read(
        beam.io.BigQuerySource(
            query=known_args.query,
            use_standard_sql=known_args.use_standard_sql,
            kms_key=kms_key))
  else:
    data = p | 'read' >> beam.io.gcp.bigquery.ReadFromBigQuery(
        query=known_args.query,
        project=options.view_as(GoogleCloudOptions).project,
        use_standard_sql=known_args.use_standard_sql,
        use_json_exports=known_args.use_json_exports,
        kms_key=kms_key)

  temp_file_format = (
      'NEWLINE_DELIMITED_JSON' if known_args.use_json_exports else 'AVRO')
  _ = data | 'write' >> beam.io.WriteToBigQuery(
      known_args.output,
      schema=table_schema,
      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
      write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY,
      temp_file_format=temp_file_format,
      kms_key=kms_key)

  result = p.run()
  result.wait_until_finish()
