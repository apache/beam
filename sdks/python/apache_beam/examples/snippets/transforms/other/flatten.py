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
#   name: FlattenTransform
#   description: Demonstration of Flatten transform usage.
#   multifile: false
#   default_example: false
#   context_line: 40
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - strings


def flatten(test=None):
  # [START flatten]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    pc1 = pipeline | 'Create produce 1' >> beam.Create(['🍓', '🥕', '🍆'])
    pc2 = pipeline | 'Create produce 2' >> beam.Create(['🍅', '🥔'])
    pc3 = pipeline | 'Create produce 3' >> beam.Create(['🍎', '🍐', '🍊'])

    icons = ((pc1, pc2, pc3) | beam.Flatten() | beam.Map(print))
    # [END flatten]

    if test:
      test(icons)


if __name__ == '__main__':
  flatten()
