# coding=utf-8
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

# pytype: skip-file
# pylint:disable=line-too-long

# beam-playground:
#   name: WithTimestampsLogicalClock
#   description: Demonstration of WithTimestamps transform usage with logical clock.
#   multifile: false
#   default_example: false
#   context_line: 40
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - timestamps


def withtimestamps_logical_clock(test=None):
  # [START withtimestamps_logical_clock]
  import apache_beam as beam

  class GetTimestamp(beam.DoFn):
    def process(self, plant, timestamp=beam.DoFn.TimestampParam):
      event_id = int(timestamp.micros / 1e6)  # equivalent to seconds
      yield '{} - {}'.format(event_id, plant['name'])

  with beam.Pipeline() as pipeline:
    plant_events = (
        pipeline
        | 'Garden plants' >> beam.Create([
            {'name': 'Strawberry', 'event_id': 1},
            {'name': 'Carrot', 'event_id': 4},
            {'name': 'Artichoke', 'event_id': 2},
            {'name': 'Tomato', 'event_id': 3},
            {'name': 'Potato', 'event_id': 5},
        ])
        | 'With timestamps' >> beam.Map(lambda plant: \
            beam.window.TimestampedValue(plant, plant['event_id']))
        | 'Get timestamp' >> beam.ParDo(GetTimestamp())
        | beam.Map(print)
    )
    # [END withtimestamps_logical_clock]
    if test:
      test(plant_events)


if __name__ == '__main__':
  withtimestamps_logical_clock()
