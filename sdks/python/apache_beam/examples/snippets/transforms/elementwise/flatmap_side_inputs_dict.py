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
#   name: FlatMapSideInputsDict
#   description: Demonstration of FlatMap transform usage with a side input from a dictionary.
#   multifile: false
#   default_example: false
#   context_line: 41
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - strings
#     - map


def flatmap_side_inputs_dict(test=None):
  # [START flatmap_side_inputs_dict]
  import apache_beam as beam

  def replace_duration_if_valid(plant, durations):
    if plant['duration'] in durations:
      plant['duration'] = durations[plant['duration']]
      yield plant

  with beam.Pipeline() as pipeline:
    durations = pipeline | 'Durations dict' >> beam.Create([
        (0, 'annual'),
        (1, 'biennial'),
        (2, 'perennial'),
    ])

    valid_plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            {
                'icon': '🍓', 'name': 'Strawberry', 'duration': 2
            },
            {
                'icon': '🥕', 'name': 'Carrot', 'duration': 1
            },
            {
                'icon': '🍆', 'name': 'Eggplant', 'duration': 2
            },
            {
                'icon': '🍅', 'name': 'Tomato', 'duration': 0
            },
            {
                'icon': '🥔', 'name': 'Potato', 'duration': -1
            },
        ])
        | 'Replace duration if valid' >> beam.FlatMap(
            replace_duration_if_valid,
            durations=beam.pvalue.AsDict(durations),
        )
        | beam.Map(print))
    # [END flatmap_side_inputs_dict]
    if test:
      test(valid_plants)


if __name__ == '__main__':
  flatmap_side_inputs_dict()
