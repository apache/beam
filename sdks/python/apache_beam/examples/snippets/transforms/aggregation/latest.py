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

from __future__ import absolute_import
from __future__ import print_function


def latest_globally(test=None):
  # [START latest_globally]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    latest_element = (
        pipeline
        | 'Create produce' >> beam.Create([
            '🍓 Strawberry',
            '🥕 Carrot',
            '🍆 Eggplant',
            '🍅 Tomato',
            '🌽 Corn',
        ])
        | 'Get latest element' >> beam.combiners.Latest.Globally()
        | beam.Map(print)
    )
    # [END latest_globally]
    if test:
      test(latest_element)


def latest_per_key(test=None):
  # [START latest_per_key]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    latest_elements_per_key = (
        pipeline
        | 'Create produce' >> beam.Create([
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
        | 'Get latest elements per key' >> beam.combiners.Latest.PerKey()
        | beam.Map(print)
    )
    # [END latest_per_key]
    if test:
      test(latest_elements_per_key)
