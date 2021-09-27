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

"""Pipelines that use the DataFrame API to process NYC taxiride CSV data."""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.dataframe.io import read_csv
from apache_beam.options.pipeline_options import PipelineOptions

ZONE_LOOKUP_PATH = (
    "gs://apache-beam-samples/nyc_taxi/misc/taxi+_zone_lookup.csv")


def run_aggregation_pipeline(pipeline, input_path, output_path):
  # The pipeline will be run on exiting the with block.
  # [START DataFrame_taxiride_aggregation]
  with pipeline as p:
    rides = p | read_csv(input_path)

    # Count the number of passengers dropped off per LocationID
    agg = rides.groupby('DOLocationID').passenger_count.sum()
    agg.to_csv(output_path)
    # [END DataFrame_taxiride_aggregation]


def run_enrich_pipeline(
    pipeline, input_path, output_path, zone_lookup_path=ZONE_LOOKUP_PATH):
  """Enrich taxi ride data with zone lookup table and perform a grouped
  aggregation."""
  # The pipeline will be run on exiting the with block.
  # [START DataFrame_taxiride_enrich]
  with pipeline as p:
    rides = p | "Read taxi rides" >> read_csv(input_path)
    zones = p | "Read zone lookup" >> read_csv(zone_lookup_path)

    # Enrich taxi ride data with boroughs from zone lookup table
    # Joins on zones.LocationID and rides.DOLocationID, by first making the
    # former the index for zones.
    rides = rides.merge(
        zones.set_index('LocationID').Borough,
        right_index=True,
        left_on='DOLocationID',
        how='left')

    # Sum passengers dropped off per Borough
    agg = rides.groupby('Borough').passenger_count.sum()
    agg.to_csv(output_path)
    # [END DataFrame_taxiride_enrich]

    # A more intuitive alternative to the above merge call, but this option
    # doesn't preserve index, thus requires non-parallel execution.
    #rides = rides.merge(zones[['LocationID','Borough']],
    #                    how="left",
    #                    left_on='DOLocationID',
    #                    right_on='LocationID')


def run(argv=None):
  """Main entry point."""
  parser = argparse.ArgumentParser(
      formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://apache-beam-samples/nyc_taxi/misc/sample.csv',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  parser.add_argument(
      '--zone_lookup',
      dest='zone_lookup_path',
      default=ZONE_LOOKUP_PATH,
      help='Location for taxi zone lookup CSV.')
  parser.add_argument(
      '--pipeline',
      dest='pipeline',
      default='location_id_agg',
      help=(
          "Choice of pipeline to run. Must be one of "
          "(location_id_agg, borough_enrich)."))

  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline = beam.Pipeline(options=PipelineOptions(pipeline_args))

  if known_args.pipeline == 'location_id_agg':
    run_aggregation_pipeline(pipeline, known_args.input, known_args.output)
  elif known_args.pipeline == 'borough_enrich':
    run_enrich_pipeline(
        pipeline,
        known_args.input,
        known_args.output,
        known_args.zone_lookup_path)
  else:
    raise ValueError(
        f"Unrecognized value for --pipeline: {known_args.pipeline!r}. "
        "Must be one of ('location_id_agg', 'borough_enrich')")


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
