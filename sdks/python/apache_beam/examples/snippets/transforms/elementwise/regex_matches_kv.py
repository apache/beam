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
#   name: RegexMatchesKV
#   description: Demonstration of Regex.matches_kv transform usage.
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


def regex_matches_kv(test=None):
  # [START regex_matches_kv]
  import apache_beam as beam

  # Matches a named group 'icon', and then two comma-separated groups.
  regex = r'(?P<icon>[^\s,]+), *(\w+), *(\w+)'
  with beam.Pipeline() as pipeline:
    plants_matches_kv = (
        pipeline
        | 'Garden plants' >> beam.Create([
            '🍓, Strawberry, perennial',
            '🥕, Carrot, biennial ignoring trailing words',
            '🍆, Eggplant, perennial',
            '🍅, Tomato, annual',
            '🥔, Potato, perennial',
            '# 🍌, invalid, format',
            'invalid, 🍉, format',
        ])
        | 'Parse plants' >> beam.Regex.matches_kv(regex, keyGroup='icon')
        | beam.Map(print))
    # [END regex_matches_kv]
    if test:
      test(plants_matches_kv)


if __name__ == '__main__':
  regex_matches_kv()
