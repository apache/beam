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
#   name: LatestGlobally
#   description: Demonstration of Latest transform usage.
#   multifile: false
#   default_example: false
#   context_line: 45
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - timestamps
#     - latest


def latest_globally(test=None):
  # [START latest_globally]
  import apache_beam as beam
  import time

  def to_unix_time(time_str, format='%Y-%m-%d %H:%M:%S'):
    return time.mktime(time.strptime(time_str, format))

  with beam.Pipeline() as pipeline:
    latest_element = (
        pipeline
        | 'Create crops' >> beam.Create([
            {
                'item': '🥬', 'harvest': '2020-02-24 00:00:00'
            },
            {
                'item': '🍓', 'harvest': '2020-06-16 00:00:00'
            },
            {
                'item': '🥕', 'harvest': '2020-07-17 00:00:00'
            },
            {
                'item': '🍆', 'harvest': '2020-10-26 00:00:00'
            },
            {
                'item': '🍅', 'harvest': '2020-10-01 00:00:00'
            },
        ])
        | 'With timestamps' >> beam.Map(
            lambda crop: beam.window.TimestampedValue(
                crop['item'], to_unix_time(crop['harvest'])))
        | 'Get latest element' >> beam.combiners.Latest.Globally()
        | beam.Map(print))
    # [END latest_globally]
    if test:
      test(latest_element)


if __name__ == '__main__':
  latest_globally()
