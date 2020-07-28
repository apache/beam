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

"""An example that processes streaming NYC Taxi data with SqlTransform.

 This example reads from the PubSub NYC Taxi stream described in
 https://github.com/googlecodelabs/cloud-dataflow-nyc-taxi-tycoon, aggregates
 the data in 15s windows using SqlTransform, and writes the output to
 a user-defined PubSub topic.
 """

# pytype: skip-file

from __future__ import absolute_import

import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.sql import SqlTransform


def run(output_topic, pipeline_args):
  pipeline_options = PipelineOptions(
      pipeline_args, save_main_session=True, streaming=True)

  with beam.Pipeline(options=pipeline_options) as pipeline:
    _ = (
        pipeline
        | beam.io.ReadFromPubSub(
            topic='projects/pubsub-public-data/topics/taxirides-realtime',
            timestamp_attribute="ts").with_output_types(bytes)
        | "Parse JSON payload" >> beam.Map(json.loads)
        # Use beam.Row to create a schema-aware PCollection
        | "Create beam Row" >> beam.Map(
            lambda x: beam.Row(
                ride_status=str(x['ride_status']),
                passenger_count=int(x['passenger_count'])))
        # SqlTransform will computes result within an existing window
        | "15s fixed windows" >> beam.WindowInto(beam.window.FixedWindows(15))
        # Aggregate drop offs and pick ups that occur within each 15s window
        | SqlTransform(
            """
             SELECT
               ride_status,
               COUNT(*) AS num_rides,
               SUM(passenger_count) AS total_passengers
             FROM PCOLLECTION
             WHERE NOT ride_status = 'enroute'
             GROUP BY ride_status""")
        # SqlTransform yields python objects with attributes corresponding to
        # the outputs of the query.
        # Collect those attributes, as well as window information, into a dict
        | "Assemble Dictionary" >> beam.Map(
            lambda row,
            window=beam.DoFn.WindowParam: {
                "ride_status": row.ride_status,
                "num_rides": row.num_rides,
                "total_passengers": row.total_passengers,
                "window_start": window.start.to_rfc3339(),
                "window_end": window.end.to_rfc3339()
            })
        | "Convert to JSON" >> beam.Map(json.dumps)
        | beam.io.WriteStringsToPubSub(topic=output_topic))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  import argparse

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output_topic',
      dest='output_topic',
      required=True,
      help=(
          'Cloud PubSub topic to write to (e.g. '
          'projects/my-project/topics/my-topic), must be created prior to '
          'running the pipeline.'))
  known_args, pipeline_args = parser.parse_known_args()

  run(known_args.output_topic, pipeline_args)
