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


def top_largest(test=None):
  # [START top_largest]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    largest_elements = (
        pipeline
        | 'Create numbers' >> beam.Create([3, 4, 1, 2])
        | 'Largest N values' >> beam.combiners.Top.Largest(2)
        | beam.Map(print))
    # [END top_largest]
    if test:
      test(largest_elements)


def top_largest_per_key(test=None):
  # [START top_largest_per_key]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    largest_elements_per_key = (
        pipeline
        | 'Create produce' >> beam.Create([
            ('🥕', 3),
            ('🥕', 2),
            ('🍆', 1),
            ('🍅', 4),
            ('🍅', 5),
            ('🍅', 3),
        ])
        | 'Largest N values per key' >> beam.combiners.Top.LargestPerKey(2)
        | beam.Map(print))
    # [END top_largest_per_key]
    if test:
      test(largest_elements_per_key)


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


def top_smallest_per_key(test=None):
  # [START top_smallest_per_key]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    smallest_elements_per_key = (
        pipeline
        | 'Create produce' >> beam.Create([
            ('🥕', 3),
            ('🥕', 2),
            ('🍆', 1),
            ('🍅', 4),
            ('🍅', 5),
            ('🍅', 3),
        ])
        | 'Smallest N values per key' >> beam.combiners.Top.SmallestPerKey(2)
        | beam.Map(print))
    # [END top_smallest_per_key]
    if test:
      test(smallest_elements_per_key)


def top_of(test=None):
  # [START top_of]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    shortest_elements = (
        pipeline
        | 'Create produce names' >> beam.Create([
            '🍓 Strawberry',
            '🥕 Carrot',
            '🍏 Green apple',
            '🍆 Eggplant',
            '🌽 Corn',
        ])
        | 'Shortest names' >> beam.combiners.Top.Of(
            2,             # number of elements
            key=len,       # optional, defaults to the element itself
            reverse=True,  # optional, defaults to False (largest/descending)
        )
        | beam.Map(print)
    )
    # [END top_of]
    if test:
      test(shortest_elements)


def top_per_key(test=None):
  # [START top_per_key]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    shortest_elements_per_key = (
        pipeline
        | 'Create produce names' >> beam.Create([
            ('spring', '🥕 Carrot'),
            ('spring', '🍓 Strawberry'),
            ('summer', '🥕 Carrot'),
            ('summer', '🌽 Corn'),
            ('summer', '🍏 Green apple'),
            ('fall', '🥕 Carrot'),
            ('fall', '🍏 Green apple'),
            ('winter', '🍆 Eggplant'),
        ])
        | 'Shortest names per key' >> beam.combiners.Top.PerKey(
            2,             # number of elements
            key=len,       # optional, defaults to the value itself
            reverse=True,  # optional, defaults to False (largest/descending)
        )
        | beam.Map(print)
    )
    # [END top_per_key]
    if test:
      test(shortest_elements_per_key)
