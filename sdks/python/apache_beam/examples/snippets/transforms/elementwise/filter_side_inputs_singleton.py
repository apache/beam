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
#   name: FilterSideInputsSingleton
#   description: Demonstration of Filter transform usage with side inputs as a singleton
#   multifile: false
#   default_example: false
#   context_line: 41
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - strings
#     - filter


def filter_side_inputs_singleton(test=None):
  # [START filter_side_inputs_singleton]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    perennial = pipeline | 'Perennial' >> beam.Create(['perennial'])

    perennials = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            {
                'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
            },
            {
                'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'
            },
            {
                'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
            },
            {
                'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
            },
            {
                'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'
            },
        ])
        | 'Filter perennials' >> beam.Filter(
            lambda plant, duration: plant['duration'] == duration,
            duration=beam.pvalue.AsSingleton(perennial),
        )
        | beam.Map(print))
    # [END filter_side_inputs_singleton]
    if test:
      test(perennials)


if __name__ == '__main__':
  filter_side_inputs_singleton()
