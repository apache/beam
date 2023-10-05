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
#   name: PartitionMultipleArguments
#   description: Demonstration of Partition transform usage with a lambda function with multiple arguments.
#   multifile: false
#   default_example: false
#   context_line: 42
#   categories:
#     - Core Transforms
#   complexity: MEDIUM
#   tags:
#     - transforms
#     - partitions


def partition_multiple_arguments(test=None):
  # pylint: disable=expression-not-assigned
  # [START partition_multiple_arguments]
  import apache_beam as beam
  import json

  def split_dataset(plant, num_partitions, ratio):
    assert num_partitions == len(ratio)
    bucket = sum(map(ord, json.dumps(plant))) % sum(ratio)
    total = 0
    for i, part in enumerate(ratio):
      total += part
      if bucket < total:
        return i
    return len(ratio) - 1

  with beam.Pipeline() as pipeline:
    train_dataset, test_dataset = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            {'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'},
            {'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'},
            {'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'},
            {'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'},
            {'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'},
        ])
        | 'Partition' >> beam.Partition(split_dataset, 2, ratio=[8, 2])
    )

    train_dataset | 'Train' >> beam.Map(lambda x: print('train: {}'.format(x)))
    test_dataset | 'Test' >> beam.Map(lambda x: print('test: {}'.format(x)))
    # [END partition_multiple_arguments]
    # pylint: enable=expression-not-assigned
    if test:
      test(train_dataset, test_dataset)


if __name__ == '__main__':
  partition_multiple_arguments()
