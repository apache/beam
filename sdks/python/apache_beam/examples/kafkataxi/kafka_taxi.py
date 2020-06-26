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

from __future__ import absolute_import

import argparse
import logging
import typing

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.kafka import WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""
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
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  pipeline_options.view_as(StandardOptions).streaming = True

  pipeline = beam.Pipeline(options=pipeline_options)
  bootstrap_servers = known_args.bootstrap_servers
  _ = (
      pipeline
      | beam.io.ReadFromPubSub(
          topic='projects/pubsub-public-data/topics/taxirides-realtime').
      with_output_types(bytes)
      | beam.Map(lambda x: (b'', x)).with_output_types(
          typing.Tuple[bytes, bytes])
      | beam.WindowInto(beam.window.FixedWindows(15))
      | WriteToKafka(
          producer_config={'bootstrap.servers': bootstrap_servers},
          topic=known_args.topic))

  _ = (
      pipeline
      | ReadFromKafka(
          consumer_config={'bootstrap.servers': bootstrap_servers},
          topics=[known_args.topic])
      | beam.FlatMap(lambda x: []))

  pipeline.run().wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
