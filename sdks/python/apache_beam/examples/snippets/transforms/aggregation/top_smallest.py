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
#   name: TopSmallest
#   description: Demonstration of Top transform usage with the smallest values.
#   multifile: false
#   default_example: false
#   context_line: 40
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - numbers


def top_smallest(test=None):
  # [START top_smallest]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    smallest_elements = (
        pipeline
        | 'Create numbers' >> beam.Create([3, 4, 1, 2])
        | 'Smallest N values' >> beam.combiners.Top.Smallest(2)
        | beam.Map(print))
    # [END top_smallest]
    if test:
      test(smallest_elements)


if __name__ == '__main__':
  top_smallest()
