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
#   name: AppromximateUnique
#   description: Demonstration of ApproximateUnique transform usage.
#   multifile: false
#   default_example: false
#   context_line: 43
#   categories:
#   complexity: BASIC
#   tags:
#     - transforms
#     - integers


def approximateunique(test=None):
  # [START approximateunique]
  import apache_beam as beam
  from apache_beam.transforms.stats import ApproximateUnique
  import random

  with beam.Pipeline() as pipeline:
    data = list(range(1000))
    random.shuffle(data)

    with beam.Pipeline() as pipeline:
      result = (
          pipeline
          | 'create' >> beam.Create(data)
          | 'get_estimate' >> ApproximateUnique.Globally(
              size=16)
          | beam.Map(print)
      )
      # [END approximateunique]
      if test:
        test(result)


