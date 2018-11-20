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

"""A streaming workflow that uses a synthetic streaming source.

This can only be used with the Flink portable runner.
"""

from __future__ import absolute_import

import argparse
import logging
import sys

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io.flink.flink_streaming_impulse_source import FlinkStreamingImpulseSource
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.trigger import AfterProcessingTime
from apache_beam.transforms.trigger import Repeatedly


def split(s):
  a = s.split("-")
  return a[0], int(a[1])


def count(x):
  return x[0], sum(x[1])


def apply_timestamp(element):
  import time
  yield window.TimestampedValue(element, time.time())


def run(argv=None):
  """Build and run the pipeline."""
  args = ["--runner=PortableRunner",
          "--job_endpoint=localhost:8099",
          "--streaming"]
  if argv:
    args.extend(argv)

  parser = argparse.ArgumentParser()
  parser.add_argument('--count',
                      dest='count',
                      default=0,
                      help='Number of triggers to generate '
                           '(0 means emit forever).')
  parser.add_argument('--interval_ms',
                      dest='interval_ms',
                      default=500,
                      help='Interval between records per parallel '
                           'Flink subtask.')

  known_args, pipeline_args = parser.parse_known_args(args)

  pipeline_options = PipelineOptions(pipeline_args)

  p = beam.Pipeline(options=pipeline_options)

  messages = (p | FlinkStreamingImpulseSource()
              .set_message_count(known_args.count)
              .set_interval_ms(known_args.interval_ms))

  _ = (messages | 'decode' >> beam.Map(lambda x: ('', 1))
       | 'window' >> beam.WindowInto(window.GlobalWindows(),
                                     trigger=Repeatedly(
                                         AfterProcessingTime(5 * 1000)),
                                     accumulation_mode=
                                     AccumulationMode.DISCARDING)
       | 'group' >> beam.GroupByKey()
       | 'count' >> beam.Map(count)
       | 'log' >> beam.Map(lambda x: logging.info("%d" % x[1])))

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run(sys.argv[1:])
