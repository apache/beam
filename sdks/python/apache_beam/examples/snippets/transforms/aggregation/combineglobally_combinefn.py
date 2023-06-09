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
#   name: CombineGloballyCombineFn
#   description: Demonstration of CombineGlobally transform usage with a CombineFn.
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


def combineglobally_combinefn(test=None):
  # [START combineglobally_combinefn]
  import apache_beam as beam

  class PercentagesFn(beam.CombineFn):
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
      #     {'🥕': 1, '🍅': 2},
      #     {'🥕': 1, '🍅': 1, '🍆': 1},
      #     {'🥕': 1, '🍅': 3},
      # ]
      merged = {}
      for accum in accumulators:
        for item, count in accum.items():
          if item not in merged:
            merged[item] = 0
          merged[item] += count
      # merged == {'🥕': 3, '🍅': 6, '🍆': 1}
      return merged

    def extract_output(self, accumulator):
      # accumulator == {'🥕': 3, '🍅': 6, '🍆': 1}
      total = sum(accumulator.values())  # 10
      percentages = {item: count / total for item, count in accumulator.items()}
      # percentages == {'🥕': 0.3, '🍅': 0.6, '🍆': 0.1}
      return percentages

  with beam.Pipeline() as pipeline:
    percentages = (
        pipeline
        | 'Create produce' >> beam.Create(
            ['🥕', '🍅', '🍅', '🥕', '🍆', '🍅', '🍅', '🍅', '🥕', '🍅'])
        | 'Get percentages' >> beam.CombineGlobally(PercentagesFn())
        | beam.Map(print))
    # [END combineglobally_combinefn]
    if test:
      test(percentages)


if __name__ == '__main__':
  combineglobally_combinefn()
