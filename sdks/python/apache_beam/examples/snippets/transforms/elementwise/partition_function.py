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
#   name: PartitionFunction
#   description: Demonstration of Partition transform usage with a function.
#   multifile: false
#   default_example: false
#   context_line: 41
#   categories:
#     - Core Transforms
#   complexity: MEDIUM
#   tags:
#     - transforms
#     - partitions


def partition_function(test=None):
  # pylint:disable=expression-not-assigned
  # [START partition_function]
  import apache_beam as beam

  durations = ['annual', 'biennial', 'perennial']

  def by_duration(plant, num_partitions):
    return durations.index(plant['duration'])

  with beam.Pipeline() as pipeline:
    annuals, biennials, perennials = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            {'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'},
            {'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'},
            {'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'},
            {'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'},
            {'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'},
        ])
        | 'Partition' >> beam.Partition(by_duration, len(durations))
    )

    annuals | 'Annuals' >> beam.Map(lambda x: print('annual: {}'.format(x)))
    biennials | 'Biennials' >> beam.Map(
        lambda x: print('biennial: {}'.format(x)))
    perennials | 'Perennials' >> beam.Map(
        lambda x: print('perennial: {}'.format(x)))
    # [END partition_function]
    if test:
      test(annuals, biennials, perennials)


if __name__ == '__main__':
  partition_function()
