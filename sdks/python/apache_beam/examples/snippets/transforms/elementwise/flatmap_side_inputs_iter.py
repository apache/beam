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
#   name: FlatMapSideInputsIter
#   description: Demonstration of FlatMap transform usage with a side input from an iterator.
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


def flatmap_side_inputs_iter(test=None):
  # [START flatmap_side_inputs_iter]
  import apache_beam as beam

  def normalize_and_validate_durations(plant, valid_durations):
    plant['duration'] = plant['duration'].lower()
    if plant['duration'] in valid_durations:
      yield plant

  with beam.Pipeline() as pipeline:
    valid_durations = pipeline | 'Valid durations' >> beam.Create([
        'annual',
        'biennial',
        'perennial',
    ])

    valid_plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            {
                'icon': '🍓', 'name': 'Strawberry', 'duration': 'Perennial'
            },
            {
                'icon': '🥕', 'name': 'Carrot', 'duration': 'BIENNIAL'
            },
            {
                'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
            },
            {
                'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
            },
            {
                'icon': '🥔', 'name': 'Potato', 'duration': 'unknown'
            },
        ])
        | 'Normalize and validate durations' >> beam.FlatMap(
            normalize_and_validate_durations,
            valid_durations=beam.pvalue.AsIter(valid_durations),
        )
        | beam.Map(print))
    # [END flatmap_side_inputs_iter]
    if test:
      test(valid_plants)


if __name__ == '__main__':
  flatmap_side_inputs_iter()
