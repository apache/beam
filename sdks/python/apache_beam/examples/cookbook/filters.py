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

"""An example workflow that demonstrates filters and other features.

  - Reading and writing data from BigQuery.
  - Manipulating BigQuery rows (as Python dicts) in memory.
  - Global aggregates.
  - Filtering PCollections using both user-specified parameters
    as well as global aggregates computed during pipeline execution.
"""

# pytype: skip-file

import argparse
import logging

import apache_beam as beam
from apache_beam.pvalue import AsSingleton


def filter_cold_days(input_data, month_filter):
  """Workflow computing rows in a specific month with low temperatures.

  Args:
    input_data: a PCollection of dictionaries representing table rows. Each
      dictionary must have the keys ['year', 'month', 'day', and 'mean_temp'].
    month_filter: an int representing the month for which colder-than-average
      days should be returned.

  Returns:
    A PCollection of dictionaries with the same keys described above. Each
      row represents a day in the specified month where temperatures were
      colder than the global mean temperature in the entire dataset.
  """

  # Project to only the desired fields from a complete input row.
  # E.g., SELECT f1, f2, f3, ... FROM InputTable.
  projection_fields = ['year', 'month', 'day', 'mean_temp']
  fields_of_interest = (
      input_data
      | 'Projected' >>
      beam.Map(lambda row: {f: row[f]
                            for f in projection_fields}))

  # Compute the global mean temperature.
  global_mean = AsSingleton(
      fields_of_interest
      | 'ExtractMean' >> beam.Map(lambda row: row['mean_temp'])
      | 'GlobalMean' >> beam.combiners.Mean.Globally())

  # Filter to the rows representing days in the month of interest
  # in which the mean daily temperature is below the global mean.
  return (
      fields_of_interest
      | 'DesiredMonth' >> beam.Filter(lambda row: row['month'] == month_filter)
      | 'BelowMean' >> beam.Filter(
          lambda row, mean: row['mean_temp'] < mean, global_mean))


def run(argv=None):
  """Constructs and runs the example filtering pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      help='BigQuery table to read from.',
      default='apache-beam-testing.samples.weather_stations')
  parser.add_argument(
      '--output', required=True, help='BigQuery table to write to.')
  parser.add_argument(
      '--month_filter', default=7, help='Numeric value of month to filter on.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  with beam.Pipeline(argv=pipeline_args) as p:

    input_data = p | beam.io.ReadFromBigQuery(table=known_args.input)

    # pylint: disable=expression-not-assigned
    (
        filter_cold_days(input_data, known_args.month_filter)
        | 'SaveToBQ' >> beam.io.WriteToBigQuery(
            known_args.output,
            schema='year:INTEGER,month:INTEGER,day:INTEGER,mean_temp:FLOAT',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
