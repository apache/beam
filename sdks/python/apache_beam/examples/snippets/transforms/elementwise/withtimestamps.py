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


def withtimestamps_event_time(test=None):
  # [START withtimestamps_event_time]
  import apache_beam as beam

  class GetTimestamp(beam.DoFn):
    def process(self, plant, timestamp=beam.DoFn.TimestampParam):
      yield '{} - {}'.format(timestamp.to_utc_datetime(), plant['name'])

  with beam.Pipeline() as pipeline:
    plant_timestamps = (
        pipeline
        | 'Garden plants' >> beam.Create([
            {'name': 'Strawberry', 'season': 1585699200}, # April, 2020
            {'name': 'Carrot', 'season': 1590969600},     # June, 2020
            {'name': 'Artichoke', 'season': 1583020800},  # March, 2020
            {'name': 'Tomato', 'season': 1588291200},     # May, 2020
            {'name': 'Potato', 'season': 1598918400},     # September, 2020
        ])
        | 'With timestamps' >> beam.Map(
            lambda plant: beam.window.TimestampedValue(plant, plant['season']))
        | 'Get timestamp' >> beam.ParDo(GetTimestamp())
        | beam.Map(print)
    )
    # [END withtimestamps_event_time]
    if test:
      test(plant_timestamps)


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


def time_tuple2unix_time():
  # [START time_tuple2unix_time]
  import time

  time_tuple = time.strptime('2020-03-19 20:50:00', '%Y-%m-%d %H:%M:%S')
  unix_time = time.mktime(time_tuple)
  # [END time_tuple2unix_time]
  return unix_time


def datetime2unix_time():
  # [START datetime2unix_time]
  import time
  import datetime

  now = datetime.datetime.now()
  time_tuple = now.timetuple()
  unix_time = time.mktime(time_tuple)
  # [END datetime2unix_time]
  return unix_time
