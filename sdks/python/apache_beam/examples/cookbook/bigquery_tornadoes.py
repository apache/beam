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

"""A workflow using BigQuery sources and sinks.

The workflow will read from a table that has the 'month' and 'tornado' fields as
part of the table schema (other additional fields are ignored). The 'month'
field is a number represented as a string (e.g., '23') and the 'tornado' field
is a boolean field.

The workflow will compute the number of tornadoes in each month and output
the results to a table (created if needed) with the following schema:

- month: number
- tornado_count: number

This example uses the default behavior for BigQuery source and sinks that
represents table rows as plain Python dictionaries.
"""

# pytype: skip-file

import argparse
import logging

import apache_beam as beam


def count_tornadoes(input_data):
  """Workflow computing the number of tornadoes for each month that had one.

  Args:
    input_data: a PCollection of dictionaries representing table rows. Each
      dictionary will have a 'month' and a 'tornado' key as described in the
      module comment.

  Returns:
    A PCollection of dictionaries containing 'month' and 'tornado_count' keys.
    Months without tornadoes are skipped.
  """

  return (
      input_data
      | 'months with tornadoes' >> beam.FlatMap(
          lambda row: [(int(row['month']), 1)] if row['tornado'] else [])
      | 'monthly count' >> beam.CombinePerKey(sum)
      | 'format' >>
      beam.Map(lambda k_v: {
          'month': k_v[0], 'tornado_count': k_v[1]
      }))


def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      default='clouddataflow-readonly:samples.weather_stations',
      help=(
          'Input BigQuery table to process specified as: '
          'PROJECT:DATASET.TABLE or DATASET.TABLE.'))
  parser.add_argument(
      '--output',
      required=True,
      help=(
          'Output BigQuery table for results specified as: '
          'PROJECT:DATASET.TABLE or DATASET.TABLE.'))

  parser.add_argument(
      '--gcs_location',
      required=False,
      help=('GCS Location to store files to load '
            'data into Bigquery'))

  known_args, pipeline_args = parser.parse_known_args(argv)

  with beam.Pipeline(argv=pipeline_args) as p:

    # Read the table rows into a PCollection.
    rows = p | 'read' >> beam.io.ReadFromBigQuery(table=known_args.input)
    counts = count_tornadoes(rows)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    counts | 'Write' >> beam.io.WriteToBigQuery(
        known_args.output,
        schema='month:INTEGER, tornado_count:INTEGER',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

    # Run the pipeline (all operations are deferred until run() is called).


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
