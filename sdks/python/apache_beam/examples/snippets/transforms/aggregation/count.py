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


def count_globally(test=None):
  # [START count_globally]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    total_elements = (
        pipeline
        | 'Create plants' >> beam.Create(
            ['🍓', '🥕', '🥕', '🥕', '🍆', '🍆', '🍅', '🍅', '🍅', '🌽'])
        | 'Count all elements' >> beam.combiners.Count.Globally()
        | beam.Map(print))
    # [END count_globally]
    if test:
      test(total_elements)


def count_per_key(test=None):
  # [START count_per_key]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    total_elements_per_keys = (
        pipeline
        | 'Create plants' >> beam.Create([
            ('spring', '🍓'),
            ('spring', '🥕'),
            ('summer', '🥕'),
            ('fall', '🥕'),
            ('spring', '🍆'),
            ('winter', '🍆'),
            ('spring', '🍅'),
            ('summer', '🍅'),
            ('fall', '🍅'),
            ('summer', '🌽'),
        ])
        | 'Count elements per key' >> beam.combiners.Count.PerKey()
        | beam.Map(print))
    # [END count_per_key]
    if test:
      test(total_elements_per_keys)


def count_per_element(test=None):
  # [START count_per_element]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    total_unique_elements = (
        pipeline
        | 'Create produce' >> beam.Create(
            ['🍓', '🥕', '🥕', '🥕', '🍆', '🍆', '🍅', '🍅', '🍅', '🌽'])
        | 'Count unique elements' >> beam.combiners.Count.PerElement()
        | beam.Map(print))
    # [END count_per_element]
    if test:
      test(total_unique_elements)
