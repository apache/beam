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

"""A pipeline using dataframes to compute typical flight delay times."""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.options.pipeline_options import PipelineOptions


def get_mean_delay_at_top_airports(airline_df):
  arr = airline_df.rename(columns={
      'arrival_airport': 'airport'
  }).airport.value_counts()
  dep = airline_df.rename(columns={
      'departure_airport': 'airport'
  }).airport.value_counts()
  total = arr + dep
  # Note we keep all to include duplicates - this ensures the result is
  # deterministic.
  # NaNs can be included in the output in pandas 1.4.0 and above, so we
  # explicitly drop them.
  top_airports = total.nlargest(10, keep='all').dropna()
  at_top_airports = airline_df['arrival_airport'].isin(
      top_airports.index.values)
  return airline_df[at_top_airports].mean()


def input_date(date):
  import datetime
  parsed = datetime.datetime.strptime(date, '%Y-%m-%d')
  if (parsed > datetime.datetime(2012, 12, 31) or
      parsed < datetime.datetime(2002, 1, 1)):
    raise ValueError("There's only data from 2002-01-01 to 2012-12-31")
  return date


def run_flight_delay_pipeline(
    pipeline, start_date=None, end_date=None, output=None):
  query = f"""
  SELECT
    FlightDate AS date,
    IATA_CODE_Reporting_Airline AS airline,
    Origin AS departure_airport,
    Dest AS arrival_airport,
    DepDelay AS departure_delay,
    ArrDelay AS arrival_delay
  FROM `apache-beam-testing.airline_ontime_data.flights`
  WHERE
    FlightDate >= '{start_date}' AND FlightDate <= '{end_date}' AND
    DepDelay IS NOT NULL AND ArrDelay IS NOT NULL
  """

  # Import this here to avoid pickling the main session.
  import time
  from apache_beam import window

  def to_unixtime(s):
    return time.mktime(s.timetuple())

  # The pipeline will be run on exiting the with block.
  with pipeline as p:
    tbl = (
        p
        | 'read table' >> beam.io.ReadFromBigQuery(
            query=query, use_standard_sql=True)
        | 'assign timestamp' >>
        beam.Map(lambda x: window.TimestampedValue(x, to_unixtime(x['date'])))
        # Use beam.Select to make sure data has a schema
        # The casts in lambdas ensure data types are properly inferred
        | 'set schema' >> beam.Select(
            date=lambda x: str(x['date']),
            airline=lambda x: str(x['airline']),
            departure_airport=lambda x: str(x['departure_airport']),
            arrival_airport=lambda x: str(x['arrival_airport']),
            departure_delay=lambda x: float(x['departure_delay']),
            arrival_delay=lambda x: float(x['arrival_delay'])))

    daily = tbl | 'daily windows' >> beam.WindowInto(
        beam.window.FixedWindows(60 * 60 * 24))

    # group the flights data by carrier
    df = to_dataframe(daily)
    result = df.groupby('airline').apply(get_mean_delay_at_top_airports)
    result.to_csv(output)


def run(argv=None):
  """Main entry point; defines and runs the flight delay pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--start_date',
      dest='start_date',
      type=input_date,
      default='2012-12-22',
      help='YYYY-MM-DD lower bound (inclusive) for input dataset.')
  parser.add_argument(
      '--end_date',
      dest='end_date',
      type=input_date,
      default='2012-12-26',
      help='YYYY-MM-DD upper bound (inclusive) for input dataset.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Location to write the output.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  run_flight_delay_pipeline(
      beam.Pipeline(options=PipelineOptions(pipeline_args)),
      start_date=known_args.start_date,
      end_date=known_args.end_date,
      output=known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
