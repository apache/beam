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
#   name: ParDoDoFn
#   description: Demonstration of ParDo transform usage with DoFn.
#   multifile: false
#   default_example: false
#   context_line: 40
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - strings


def pardo_dofn(test=None):
  # [START pardo_dofn]
  import apache_beam as beam

  class SplitWords(beam.DoFn):
    def __init__(self, delimiter=','):
      self.delimiter = delimiter

    def process(self, text):
      for word in text.split(self.delimiter):
        yield word

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            '🍓Strawberry,🥕Carrot,🍆Eggplant',
            '🍅Tomato,🥔Potato',
        ])
        | 'Split words' >> beam.ParDo(SplitWords(','))
        | beam.Map(print))
    # [END pardo_dofn]
    if test:
      test(plants)


if __name__ == '__main__':
  pardo_dofn()
