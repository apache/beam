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
#   name: TopLargestPerKey
#   description: Demonstration of Top transform usage with the largest values per key.
#   multifile: false
#   default_example: false
#   context_line: 40
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - numbers


def top_largest_per_key(test=None):
  # [START top_largest_per_key]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    largest_elements_per_key = (
        pipeline
        | 'Create produce' >> beam.Create([
            ('🥕', 3),
            ('🥕', 2),
            ('🍆', 1),
            ('🍅', 4),
            ('🍅', 5),
            ('🍅', 3),
        ])
        | 'Largest N values per key' >> beam.combiners.Top.LargestPerKey(2)
        | beam.Map(print))
    # [END top_largest_per_key]
    if test:
      test(largest_elements_per_key)


if __name__ == '__main__':
  top_largest_per_key()
