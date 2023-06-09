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
#   name: CombineValuesCombineFn
#   description: Demonstration of CombineValue transform usage with CombineFn.
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


def combinevalues_combinefn(test=None):
  # [START combinevalues_combinefn]
  import apache_beam as beam

  class AverageFn(beam.CombineFn):
    def create_accumulator(self):
      return {}

    def add_input(self, accumulator, input):
      # accumulator == {}
      # input == '🥕'
      if input not in accumulator:
        accumulator[input] = 0  # {'🥕': 0}
      accumulator[input] += 1  # {'🥕': 1}
      return accumulator

    def merge_accumulators(self, accumulators):
      # accumulators == [
      #     {'🥕': 1, '🍅': 1},
      #     {'🥕': 1, '🍅': 1, '🍆': 1},
      # ]
      merged = {}
      for accum in accumulators:
        for item, count in accum.items():
          if item not in merged:
            merged[item] = 0
          merged[item] += count
      # merged == {'🥕': 2, '🍅': 2, '🍆': 1}
      return merged

    def extract_output(self, accumulator):
      # accumulator == {'🥕': 2, '🍅': 2, '🍆': 1}
      total = sum(accumulator.values())  # 5
      percentages = {item: count / total for item, count in accumulator.items()}
      # percentages == {'🥕': 0.4, '🍅': 0.4, '🍆': 0.2}
      return percentages

  with beam.Pipeline() as pipeline:
    percentages_per_season = (
        pipeline
        | 'Create produce' >> beam.Create([
            ('spring', ['🥕', '🍅', '🥕', '🍅', '🍆']),
            ('summer', ['🥕', '🍅', '🌽', '🍅', '🍅']),
            ('fall', ['🥕', '🥕', '🍅', '🍅']),
            ('winter', ['🍆', '🍆']),
        ])
        | 'Average' >> beam.CombineValues(AverageFn())
        | beam.Map(print))
    # [END combinevalues_combinefn]
    if test:
      test(percentages_per_season)


if __name__ == '__main__':
  combinevalues_combinefn()
