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

"""An example that writes to and reads from Kafka.

 This example reads from the PubSub NYC Taxi stream described in
 https://github.com/googlecodelabs/cloud-dataflow-nyc-taxi-tycoon, writes to a
 given Kafka topic and reads back from the same Kafka topic.
 """

# pytype: skip-file

import logging
import sys
import typing

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.kafka import WriteToKafka
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions


def run(
    bootstrap_servers,
    topic,
    with_metadata,
    bq_dataset,
    bq_table_name,
    project,
    pipeline_options):
  # bootstrap_servers = '123.45.67.89:123:9092'
  # topic = 'kafka_taxirides_realtime'
  # pipeline_args = ['--project', 'my-project',
  #                  '--runner', 'DataflowRunner',
  #                  '--temp_location', 'my-temp-location',
  #                  '--region', 'my-region',
  #                  '--num_workers', 'my-num-workers',
  #                  '--experiments', 'use_runner_v2']

  window_size = 15  # size of the Window in seconds.

  def log_ride(ride):
    if 'timestamp' in ride:
      logging.info(
          'Found ride at latitude %r and longitude %r with %r '
          'passengers at timestamp %r',
          ride['latitude'],
          ride['longitude'],
          ride['passenger_count'],
          ride['timestamp'])
    else:
      logging.info(
          'Found ride at latitude %r and longitude %r with %r '
          'passengers',
          ride['latitude'],
          ride['longitude'],
          ride['passenger_count'])

  def convert_kafka_record_to_dictionary(record):
    # the records have 'value' attribute when --with_metadata is given
    if hasattr(record, 'value'):
      ride_bytes = record.value
    elif isinstance(record, tuple):
      ride_bytes = record[1]
    else:
      raise RuntimeError('unknown record type: %s' % type(record))
    # Converting bytes record from Kafka to a dictionary.
    import ast
    ride = ast.literal_eval(ride_bytes.decode("UTF-8"))
    output = {
        key: ride[key]
        for key in ['latitude', 'longitude', 'passenger_count']
    }
    if hasattr(record, 'timestamp'):
      # timestamp is read from Kafka metadata
      output['timestamp'] = record.timestamp
    return output

  with beam.Pipeline(options=pipeline_options) as pipeline:
    _ = (
        pipeline
        | beam.io.ReadFromPubSub(
            topic='projects/pubsub-public-data/topics/taxirides-realtime').
        with_output_types(bytes)
        | beam.Map(lambda x: (b'', x)).with_output_types(
            typing.Tuple[bytes, bytes])  # Kafka write transforms expects KVs.
        | beam.WindowInto(beam.window.FixedWindows(window_size))
        | WriteToKafka(
            producer_config={'bootstrap.servers': bootstrap_servers},
            topic=topic))

    ride_col = (
        pipeline
        | ReadFromKafka(
            consumer_config={'bootstrap.servers': bootstrap_servers},
            topics=[topic],
            with_metadata=with_metadata)
        | beam.Map(lambda record: convert_kafka_record_to_dictionary(record)))

    if bq_dataset:
      schema = 'latitude:STRING,longitude:STRING,passenger_count:INTEGER'
      if with_metadata:
        schema += ',timestamp:STRING'
      _ = (
          ride_col
          | beam.io.WriteToBigQuery(bq_table_name, bq_dataset, project, schema))
    else:
      _ = ride_col | beam.FlatMap(lambda ride: log_ride(ride))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  import argparse

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--bootstrap_servers',
      dest='bootstrap_servers',
      required=True,
      help='Bootstrap servers for the Kafka cluster. Should be accessible by '
      'the runner')
  parser.add_argument(
      '--topic',
      dest='topic',
      default='kafka_taxirides_realtime',
      help='Kafka topic to write to and read from')
  parser.add_argument(
      '--with_metadata',
      default=False,
      action='store_true',
      help='If set, also reads metadata from the Kafka broker.')
  parser.add_argument(
      '--bq_dataset',
      type=str,
      default='',
      help='BigQuery Dataset to write tables to. '
      'If set, export data to a BigQuery table instead of just logging. '
      'Must already exist.')
  parser.add_argument(
      '--bq_table_name',
      default='xlang_kafka_taxi',
      help='The BigQuery table name. Should not already exist.')
  known_args, pipeline_args = parser.parse_known_args()

  pipeline_options = PipelineOptions(
      pipeline_args, save_main_session=True, streaming=True)

  # We also require the --project option to access --bq_dataset
  project = pipeline_options.view_as(GoogleCloudOptions).project
  if project is None:
    parser.print_usage()
    print(sys.argv[0] + ': error: argument --project is required')
    sys.exit(1)

  run(
      known_args.bootstrap_servers,
      known_args.topic,
      known_args.with_metadata,
      known_args.bq_dataset,
      known_args.bq_table_name,
      project,
      pipeline_options)
