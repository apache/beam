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


def sum_per_key(test=None):
  # [START sum_per_key]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    totals_per_key = (
        pipeline
        | 'Create produce' >> beam.Create([
            ('🥕', 3),
            ('🥕', 2),
            ('🍆', 1),
            ('🍅', 4),
            ('🍅', 5),
            ('🍅', 3),
        ])
        | 'Sum values per key' >> beam.CombinePerKey(sum)
        | beam.Map(print))
    # [END sum_per_key]
    if test:
      test(totals_per_key)
