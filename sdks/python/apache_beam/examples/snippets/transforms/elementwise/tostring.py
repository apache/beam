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

from __future__ import absolute_import
from __future__ import print_function


def tostring_kvs(test=None):
  # [START tostring_kvs]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Garden plants' >> beam.Create([
            ('🍓', 'Strawberry'),
            ('🥕', 'Carrot'),
            ('🍆', 'Eggplant'),
            ('🍅', 'Tomato'),
            ('🥔', 'Potato'),
        ])
        | 'To string' >> beam.ToString.Kvs()
        | beam.Map(print))
    # [END tostring_kvs]
    if test:
      test(plants)


def tostring_element(test=None):
  # [START tostring_element]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    plant_lists = (
        pipeline
        | 'Garden plants' >> beam.Create([
            ['🍓', 'Strawberry', 'perennial'],
            ['🥕', 'Carrot', 'biennial'],
            ['🍆', 'Eggplant', 'perennial'],
            ['🍅', 'Tomato', 'annual'],
            ['🥔', 'Potato', 'perennial'],
        ])
        | 'To string' >> beam.ToString.Element()
        | beam.Map(print))
    # [END tostring_element]
    if test:
      test(plant_lists)


def tostring_iterables(test=None):
  # [START tostring_iterables]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    plants_csv = (
        pipeline
        | 'Garden plants' >> beam.Create([
            ['🍓', 'Strawberry', 'perennial'],
            ['🥕', 'Carrot', 'biennial'],
            ['🍆', 'Eggplant', 'perennial'],
            ['🍅', 'Tomato', 'annual'],
            ['🥔', 'Potato', 'perennial'],
        ])
        | 'To string' >> beam.ToString.Iterables()
        | beam.Map(print))
    # [END tostring_iterables]
    if test:
      test(plants_csv)
