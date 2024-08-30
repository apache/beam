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
#   name: RegexSplit
#   description: Demonstration of Regex.split transform usage.
#   multifile: false
#   default_example: false
#   context_line: 41
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - strings
#     - regex


def regex_split(test=None):
  # [START regex_split]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    plants_split = (
        pipeline
        | 'Garden plants' >> beam.Create([
            '🍓 : Strawberry : perennial',
            '🥕 : Carrot : biennial',
            '🍆\t:\tEggplant : perennial',
            '🍅 : Tomato : annual',
            '🥔 : Potato : perennial',
        ])
        | 'Parse plants' >> beam.Regex.split(r'\s*:\s*')
        | beam.Map(print))
    # [END regex_split]
    if test:
      test(plants_split)


if __name__ == '__main__':
  regex_split()
