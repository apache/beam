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
#   name: MapSideInputsDict
#   description: Demonstration of Map transform usage with side inputs from a dictionary.
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


def map_side_inputs_dict(test=None):
  # [START map_side_inputs_dict]
  import apache_beam as beam

  def replace_duration(plant, durations):
    plant['duration'] = durations[plant['duration']]
    return plant

  with beam.Pipeline() as pipeline:
    durations = pipeline | 'Durations' >> beam.Create([
        (0, 'annual'),
        (1, 'biennial'),
        (2, 'perennial'),
    ])

    plant_details = (
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
                'icon': '🥔', 'name': 'Potato', 'duration': 2
            },
        ])
        | 'Replace duration' >> beam.Map(
            replace_duration,
            durations=beam.pvalue.AsDict(durations),
        )
        | beam.Map(print))
    # [END map_side_inputs_dict]
    if test:
      test(plant_details)


if __name__ == '__main__':
  map_side_inputs_dict()
