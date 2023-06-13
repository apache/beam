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
#   name: CombineValuesSideInputsIter
#   description: Demonstration of CombineValue transform usage with side inputs as Iter.
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


def combinevalues_side_inputs_iter(test=None):
  # [START combinevalues_side_inputs_iter]
  import apache_beam as beam

  def bounded_sum(values, data_range):
    min_value = min(data_range)
    result = sum(values)
    if result < min_value:
      return min_value
    max_value = max(data_range)
    if result > max_value:
      return max_value
    return result

  with beam.Pipeline() as pipeline:
    data_range = pipeline | 'Create data_range' >> beam.Create([2, 4, 8])

    bounded_total = (
        pipeline
        | 'Create plant counts' >> beam.Create([
            ('🥕', [3, 2]),
            ('🍆', [1]),
            ('🍅', [4, 5, 3]),
        ])
        | 'Bounded sum' >> beam.CombineValues(
            bounded_sum, data_range=beam.pvalue.AsIter(data_range))
        | beam.Map(print))
    # [END combinevalues_side_inputs_iter]
    if test:
      test(bounded_total)


if __name__ == '__main__':
  combinevalues_side_inputs_iter()
