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
#   name: RegexReplaceAll
#   description: Demonstration of Regex.replace_all transform usage.
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


def regex_replace_all(test=None):
  # [START regex_replace_all]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    plants_replace_all = (
        pipeline
        | 'Garden plants' >> beam.Create([
            '🍓 : Strawberry : perennial',
            '🥕 : Carrot : biennial',
            '🍆\t:\tEggplant\t:\tperennial',
            '🍅 : Tomato : annual',
            '🥔 : Potato : perennial',
        ])
        | 'To CSV' >> beam.Regex.replace_all(r'\s*:\s*', ',')
        | beam.Map(print))
    # [END regex_replace_all]
    if test:
      test(plants_replace_all)


if __name__ == '__main__':
  regex_replace_all()
