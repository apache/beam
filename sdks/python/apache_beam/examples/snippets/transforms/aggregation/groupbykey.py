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
#   name: GroupByKeySort
#   description: Demonstration of GroupByKey transform usage with per-key sorting.
#   multifile: false
#   default_example: false
#   context_line: 40
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - strings
#     - pairs
#     - group


def groupbykey(test=None):
  # [START groupbykey]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    produce_per_season = (
        pipeline
        | 'Create produce list' >> beam.Create([
            ('spring', '🍓'),
            ('spring', '🥕'),
            ('spring', '🍆'),
            ('spring', '🍅'),
            ('summer', '🥕'),
            ('summer', '🍅'),
            ('summer', '🌽'),
            ('fall', '🥕'),
            ('fall', '🍅'),
            ('winter', '🍆'),
        ])
        | 'Group produce per season' >> beam.GroupByKey()
        | beam.MapTuple(lambda k, vs: (k, sorted(vs)))  # sort and format
        | beam.Map(print))
    # [END groupbykey]
    if test:
      test(produce_per_season)


if __name__ == '__main__':
  groupbykey()
