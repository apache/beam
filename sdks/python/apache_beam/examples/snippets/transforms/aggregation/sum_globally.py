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
#   name: SumGlobally
#   description: Demonstration of CombineGlobally transform usage with Sum.
#   multifile: false
#   default_example: false
#   context_line: 40
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - numbers


def sum_globally(test=None):
  # [START sum_globally]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    total = (
        pipeline
        | 'Create numbers' >> beam.Create([3, 4, 1, 2])
        | 'Sum values' >> beam.CombineGlobally(sum)
        | beam.Map(print))
    # [END sum_globally]
    if test:
      test(total)


if __name__ == '__main__':
  sum_globally()
