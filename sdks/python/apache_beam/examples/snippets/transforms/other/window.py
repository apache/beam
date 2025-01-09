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
#   name: Window
#   description: Demonstration of Window transform usage.
#   multifile: false
#   default_example: false
#   context_line: 43
#   categories:
#     - Core Transforms
#     - Windowing
#   complexity: BASIC
#   tags:
#     - transforms
#     - strings
#     - timestamps
#     - windows


def window(test=None):
  # [START window]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    produce = (
        pipeline
        | 'Garden plants' >> beam.Create([
            {
                'name': 'Strawberry', 'season': 1585699200
            },  # April, 2020
            {
                'name': 'Strawberry', 'season': 1588291200
            },  # May, 2020
            {
                'name': 'Carrot', 'season': 1590969600
            },  # June, 2020
            {
                'name': 'Artichoke', 'season': 1583020800
            },  # March, 2020
            {
                'name': 'Artichoke', 'season': 1585699200
            },  # April, 2020
            {
                'name': 'Tomato', 'season': 1588291200
            },  # May, 2020
            {
                'name': 'Potato', 'season': 1598918400
            },  # September, 2020
        ])
        | 'With timestamps' >> beam.Map(
            lambda plant: beam.window.TimestampedValue(
                plant['name'], plant['season']))
        | 'Window into fixed 2-month windows' >> beam.WindowInto(
            beam.window.FixedWindows(2 * 30 * 24 * 60 * 60))
        | 'Count per window' >> beam.combiners.Count.PerElement()
        | 'Print results' >> beam.Map(print))
    # [END window]

    if test:
      test(produce)


if __name__ == '__main__':
  window()
