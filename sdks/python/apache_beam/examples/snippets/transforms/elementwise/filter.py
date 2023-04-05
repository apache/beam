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


def filter_function(test=None):
  # [START filter_function]
  import apache_beam as beam

  def is_perennial(plant):
    return plant['duration'] == 'perennial'

  with beam.Pipeline() as pipeline:
    perennials = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            {
                'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
            },
            {
                'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'
            },
            {
                'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
            },
            {
                'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
            },
            {
                'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'
            },
        ])
        | 'Filter perennials' >> beam.Filter(is_perennial)
        | beam.Map(print))
    # [END filter_function]
    if test:
      test(perennials)


def filter_lambda(test=None):
  # [START filter_lambda]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    perennials = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            {
                'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
            },
            {
                'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'
            },
            {
                'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
            },
            {
                'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
            },
            {
                'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'
            },
        ])
        | 'Filter perennials' >>
        beam.Filter(lambda plant: plant['duration'] == 'perennial')
        | beam.Map(print))
    # [END filter_lambda]
    if test:
      test(perennials)


def filter_multiple_arguments(test=None):
  # [START filter_multiple_arguments]
  import apache_beam as beam

  def has_duration(plant, duration):
    return plant['duration'] == duration

  with beam.Pipeline() as pipeline:
    perennials = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            {
                'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
            },
            {
                'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'
            },
            {
                'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
            },
            {
                'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
            },
            {
                'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'
            },
        ])
        | 'Filter perennials' >> beam.Filter(has_duration, 'perennial')
        | beam.Map(print))
    # [END filter_multiple_arguments]
    if test:
      test(perennials)


def filter_side_inputs_singleton(test=None):
  # [START filter_side_inputs_singleton]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    perennial = pipeline | 'Perennial' >> beam.Create(['perennial'])

    perennials = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            {
                'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
            },
            {
                'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'
            },
            {
                'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
            },
            {
                'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
            },
            {
                'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'
            },
        ])
        | 'Filter perennials' >> beam.Filter(
            lambda plant,
            duration: plant['duration'] == duration,
            duration=beam.pvalue.AsSingleton(perennial),
        )
        | beam.Map(print))
    # [END filter_side_inputs_singleton]
    if test:
      test(perennials)


def filter_side_inputs_iter(test=None):
  # [START filter_side_inputs_iter]
  import apache_beam as beam

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
                'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
            },
            {
                'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'
            },
            {
                'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
            },
            {
                'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
            },
            {
                'icon': '🥔', 'name': 'Potato', 'duration': 'PERENNIAL'
            },
        ])
        | 'Filter valid plants' >> beam.Filter(
            lambda plant,
            valid_durations: plant['duration'] in valid_durations,
            valid_durations=beam.pvalue.AsIter(valid_durations),
        )
        | beam.Map(print))
    # [END filter_side_inputs_iter]
    if test:
      test(valid_plants)


def filter_side_inputs_dict(test=None):
  # [START filter_side_inputs_dict]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    keep_duration = pipeline | 'Duration filters' >> beam.Create([
        ('annual', False),
        ('biennial', False),
        ('perennial', True),
    ])

    perennials = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            {
                'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
            },
            {
                'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'
            },
            {
                'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
            },
            {
                'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
            },
            {
                'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'
            },
        ])
        | 'Filter plants by duration' >> beam.Filter(
            lambda plant,
            keep_duration: keep_duration[plant['duration']],
            keep_duration=beam.pvalue.AsDict(keep_duration),
        )
        | beam.Map(print))
    # [END filter_side_inputs_dict]
    if test:
      test(perennials)
