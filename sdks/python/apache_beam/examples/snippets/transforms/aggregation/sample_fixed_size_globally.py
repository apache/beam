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
#   name: SampleFixedSizeGlobally
#   description: Demonstration of Sample transform usage with fixed size.
#   multifile: false
#   default_example: false
#   context_line: 41
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - pairs
#     - group


def sample_fixed_size_globally(test=None):
  # [START sample_fixed_size_globally]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    sample = (
        pipeline
        | 'Create produce' >> beam.Create([
            '🍓 Strawberry',
            '🥕 Carrot',
            '🍆 Eggplant',
            '🍅 Tomato',
            '🥔 Potato',
        ])
        | 'Sample N elements' >> beam.combiners.Sample.FixedSizeGlobally(3)
        | beam.Map(print))
    # [END sample_fixed_size_globally]
    if test:
      test(sample)


if __name__ == '__main__':
  sample_fixed_size_globally()
