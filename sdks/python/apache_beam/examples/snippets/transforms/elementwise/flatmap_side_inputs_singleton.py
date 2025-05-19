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
#   name: FlatMapSideInputSingleton
#   description: Demonstration of FlatMap transform usage with a side input as a singleton.
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


def flatmap_side_inputs_singleton(test=None):
  # [START flatmap_side_inputs_singleton]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    delimiter = pipeline | 'Create delimiter' >> beam.Create([','])

    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            '🍓Strawberry,🥕Carrot,🍆Eggplant',
            '🍅Tomato,🥔Potato',
        ])
        | 'Split words' >> beam.FlatMap(
            lambda text, delimiter: text.split(delimiter),
            delimiter=beam.pvalue.AsSingleton(delimiter),
        )
        | beam.Map(print))
    # [END flatmap_side_inputs_singleton]
    if test:
      test(plants)


if __name__ == '__main__':
  flatmap_side_inputs_singleton()
