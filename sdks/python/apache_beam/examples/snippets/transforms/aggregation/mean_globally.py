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
#   name: MeanGlobally
#   description: Demonstration of Mean transform usage.
#   multifile: false
#   default_example: false
#   context_line: 41
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - numbers
#     - pairs


def mean_globally(test=None):
  # [START mean_globally]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    mean_element = (
        pipeline
        | 'Create numbers' >> beam.Create([3, 4, 1, 2])
        | 'Get mean value' >> beam.combiners.Mean.Globally()
        | beam.Map(print))
    # [END mean_globally]
    if test:
      test(mean_element)


if __name__ == '__main__':
  mean_globally()
