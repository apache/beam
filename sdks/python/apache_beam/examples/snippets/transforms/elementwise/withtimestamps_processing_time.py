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
#   name: WithTimestampsProcessingTime
#   description: Demonstration of WithTimestamps transform usage.
#   multifile: false
#   default_example: false
#   context_line: 41
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - timestamps


def withtimestamps_processing_time(test=None):
  # [START withtimestamps_processing_time]
  import apache_beam as beam
  import time

  class GetTimestamp(beam.DoFn):
    def process(self, plant, timestamp=beam.DoFn.TimestampParam):
      yield '{} - {}'.format(timestamp.to_utc_datetime(), plant['name'])

  with beam.Pipeline() as pipeline:
    plant_processing_times = (
        pipeline
        | 'Garden plants' >> beam.Create([
            {'name': 'Strawberry'},
            {'name': 'Carrot'},
            {'name': 'Artichoke'},
            {'name': 'Tomato'},
            {'name': 'Potato'},
        ])
        | 'With timestamps' >> beam.Map(lambda plant: \
            beam.window.TimestampedValue(plant, time.time()))
        | 'Get timestamp' >> beam.ParDo(GetTimestamp())
        | beam.Map(print)
    )
    # [END withtimestamps_processing_time]
    if test:
      test(plant_processing_times)


if __name__ == '__main__':
  withtimestamps_processing_time()
