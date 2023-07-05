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
#   name: CombineValuesSimple
#   description: Demonstration of CombineValue transform usage.
#   multifile: false
#   default_example: false
#   context_line: 39
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - pairs
#     - combine


def combinevalues_simple(test=None):
  # [START combinevalues_simple]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    total = (
        pipeline
        | 'Create produce counts' >> beam.Create([
            ('🥕', [3, 2]),
            ('🍆', [1]),
            ('🍅', [4, 5, 3]),
        ])
        | 'Sum' >> beam.CombineValues(sum)
        | beam.Map(print))
    # [END combinevalues_simple]
    if test:
      test(total)


if __name__ == '__main__':
  combinevalues_simple()
