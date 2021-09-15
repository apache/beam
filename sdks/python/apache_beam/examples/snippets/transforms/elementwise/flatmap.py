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


def flatmap_simple(test=None):
  # [START flatmap_simple]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            '🍓Strawberry 🥕Carrot 🍆Eggplant',
            '🍅Tomato 🥔Potato',
        ])
        | 'Split words' >> beam.FlatMap(str.split)
        | beam.Map(print))
    # [END flatmap_simple]
    if test:
      test(plants)


def flatmap_function(test=None):
  # [START flatmap_function]
  import apache_beam as beam

  def split_words(text):
    return text.split(',')

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            '🍓Strawberry,🥕Carrot,🍆Eggplant',
            '🍅Tomato,🥔Potato',
        ])
        | 'Split words' >> beam.FlatMap(split_words)
        | beam.Map(print))
    # [END flatmap_function]
    if test:
      test(plants)


def flatmap_lambda(test=None):
  # [START flatmap_lambda]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            ['🍓Strawberry', '🥕Carrot', '🍆Eggplant'],
            ['🍅Tomato', '🥔Potato'],
        ])
        | 'Flatten lists' >> beam.FlatMap(lambda elements: elements)
        | beam.Map(print))
    # [END flatmap_lambda]
    if test:
      test(plants)


def flatmap_generator(test=None):
  # [START flatmap_generator]
  import apache_beam as beam

  def generate_elements(elements):
    for element in elements:
      yield element

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            ['🍓Strawberry', '🥕Carrot', '🍆Eggplant'],
            ['🍅Tomato', '🥔Potato'],
        ])
        | 'Flatten lists' >> beam.FlatMap(generate_elements)
        | beam.Map(print))
    # [END flatmap_generator]
    if test:
      test(plants)


def flatmap_multiple_arguments(test=None):
  # [START flatmap_multiple_arguments]
  import apache_beam as beam

  def split_words(text, delimiter=None):
    return text.split(delimiter)

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            '🍓Strawberry,🥕Carrot,🍆Eggplant',
            '🍅Tomato,🥔Potato',
        ])
        | 'Split words' >> beam.FlatMap(split_words, delimiter=',')
        | beam.Map(print))
    # [END flatmap_multiple_arguments]
    if test:
      test(plants)


def flatmap_tuple(test=None):
  # [START flatmap_tuple]
  import apache_beam as beam

  def format_plant(icon, plant):
    if icon:
      yield '{}{}'.format(icon, plant)

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            ('🍓', 'Strawberry'),
            ('🥕', 'Carrot'),
            ('🍆', 'Eggplant'),
            ('🍅', 'Tomato'),
            ('🥔', 'Potato'),
            (None, 'Invalid'),
        ])
        | 'Format' >> beam.FlatMapTuple(format_plant)
        | beam.Map(print))
    # [END flatmap_tuple]
    if test:
      test(plants)


def flatmap_side_inputs_singleton(test=None):
  # [START flatmap_side_inputs_singleton]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    delimiter = pipeline | 'Create delimiter' >> beam.Create([','])

    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            '🍓Strawberry,🥕Carrot,🍆Eggplant',
            '🍅Tomato,🥔Potato',
        ])
        | 'Split words' >> beam.FlatMap(
            lambda text,
            delimiter: text.split(delimiter),
            delimiter=beam.pvalue.AsSingleton(delimiter),
        )
        | beam.Map(print))
    # [END flatmap_side_inputs_singleton]
    if test:
      test(plants)


def flatmap_side_inputs_iter(test=None):
  # [START flatmap_side_inputs_iter]
  import apache_beam as beam

  def normalize_and_validate_durations(plant, valid_durations):
    plant['duration'] = plant['duration'].lower()
    if plant['duration'] in valid_durations:
      yield plant

  with beam.Pipeline() as pipeline:
    valid_durations = pipeline | 'Valid durations' >> beam.Create([
        'annual',
        'biennial',
        'perennial',
    ])

    valid_plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            {
                'icon': '🍓', 'name': 'Strawberry', 'duration': 'Perennial'
            },
            {
                'icon': '🥕', 'name': 'Carrot', 'duration': 'BIENNIAL'
            },
            {
                'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
            },
            {
                'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
            },
            {
                'icon': '🥔', 'name': 'Potato', 'duration': 'unknown'
            },
        ])
        | 'Normalize and validate durations' >> beam.FlatMap(
            normalize_and_validate_durations,
            valid_durations=beam.pvalue.AsIter(valid_durations),
        )
        | beam.Map(print))
    # [END flatmap_side_inputs_iter]
    if test:
      test(valid_plants)


def flatmap_side_inputs_dict(test=None):
  # [START flatmap_side_inputs_dict]
  import apache_beam as beam

  def replace_duration_if_valid(plant, durations):
    if plant['duration'] in durations:
      plant['duration'] = durations[plant['duration']]
      yield plant

  with beam.Pipeline() as pipeline:
    durations = pipeline | 'Durations dict' >> beam.Create([
        (0, 'annual'),
        (1, 'biennial'),
        (2, 'perennial'),
    ])

    valid_plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            {
                'icon': '🍓', 'name': 'Strawberry', 'duration': 2
            },
            {
                'icon': '🥕', 'name': 'Carrot', 'duration': 1
            },
            {
                'icon': '🍆', 'name': 'Eggplant', 'duration': 2
            },
            {
                'icon': '🍅', 'name': 'Tomato', 'duration': 0
            },
            {
                'icon': '🥔', 'name': 'Potato', 'duration': -1
            },
        ])
        | 'Replace duration if valid' >> beam.FlatMap(
            replace_duration_if_valid,
            durations=beam.pvalue.AsDict(durations),
        )
        | beam.Map(print))
    # [END flatmap_side_inputs_dict]
    if test:
      test(valid_plants)
