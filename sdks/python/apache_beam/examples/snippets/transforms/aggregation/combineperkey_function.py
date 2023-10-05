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
#   name: CombinePerKeyFunction
#   description: Demonstration of CombinePerKey transform usage with side inputs.
#   multifile: false
#   default_example: false
#   context_line: 39
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - strings
#     - group


def combineperkey_function(test=None):
  # [START combineperkey_function]
  import apache_beam as beam

  def saturated_sum(values):
    max_value = 8
    return min(sum(values), max_value)

  with beam.Pipeline() as pipeline:
    saturated_total = (
        pipeline
        | 'Create plant counts' >> beam.Create([
            ('🥕', 3),
            ('🥕', 2),
            ('🍆', 1),
            ('🍅', 4),
            ('🍅', 5),
            ('🍅', 3),
        ])
        | 'Saturated sum' >> beam.CombinePerKey(saturated_sum)
        | beam.Map(print))
    # [END combineperkey_function]
    if test:
      test(saturated_total)


if __name__ == '__main__':
  combineperkey_function()
