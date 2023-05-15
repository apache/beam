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
#   name: CombinePerKeyCombineFn
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


def combineperkey_combinefn(test=None):
  # [START combineperkey_combinefn]
  import apache_beam as beam

  class AverageFn(beam.CombineFn):
    def create_accumulator(self):
      sum = 0.0
      count = 0
      accumulator = sum, count
      return accumulator

    def add_input(self, accumulator, input):
      sum, count = accumulator
      return sum + input, count + 1

    def merge_accumulators(self, accumulators):
      # accumulators = [(sum1, count1), (sum2, count2), (sum3, count3), ...]
      sums, counts = zip(*accumulators)
      # sums = [sum1, sum2, sum3, ...]
      # counts = [count1, count2, count3, ...]
      return sum(sums), sum(counts)

    def extract_output(self, accumulator):
      sum, count = accumulator
      if count == 0:
        return float('NaN')
      return sum / count

  with beam.Pipeline() as pipeline:
    average = (
        pipeline
        | 'Create plant counts' >> beam.Create([
            ('🥕', 3),
            ('🥕', 2),
            ('🍆', 1),
            ('🍅', 4),
            ('🍅', 5),
            ('🍅', 3),
        ])
        | 'Average' >> beam.CombinePerKey(AverageFn())
        | beam.Map(print))
    # [END combineperkey_combinefn]
    if test:
      test(average)


if __name__ == '__main__':
  combineperkey_combinefn()
