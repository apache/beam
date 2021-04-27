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


def map_simple(test=None):
  # [START map_simple]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            '   🍓Strawberry   \n',
            '   🥕Carrot   \n',
            '   🍆Eggplant   \n',
            '   🍅Tomato   \n',
            '   🥔Potato   \n',
        ])
        | 'Strip' >> beam.Map(str.strip)
        | beam.Map(print))
    # [END map_simple]
    if test:
      test(plants)


def map_function(test=None):
  # [START map_function]
  import apache_beam as beam

  def strip_header_and_newline(text):
    return text.strip('# \n')

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            '# 🍓Strawberry\n',
            '# 🥕Carrot\n',
            '# 🍆Eggplant\n',
            '# 🍅Tomato\n',
            '# 🥔Potato\n',
        ])
        | 'Strip header' >> beam.Map(strip_header_and_newline)
        | beam.Map(print))
    # [END map_function]
    if test:
      test(plants)


def map_lambda(test=None):
  # [START map_lambda]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            '# 🍓Strawberry\n',
            '# 🥕Carrot\n',
            '# 🍆Eggplant\n',
            '# 🍅Tomato\n',
            '# 🥔Potato\n',
        ])
        | 'Strip header' >> beam.Map(lambda text: text.strip('# \n'))
        | beam.Map(print))
    # [END map_lambda]
    if test:
      test(plants)


def map_multiple_arguments(test=None):
  # [START map_multiple_arguments]
  import apache_beam as beam

  def strip(text, chars=None):
    return text.strip(chars)

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            '# 🍓Strawberry\n',
            '# 🥕Carrot\n',
            '# 🍆Eggplant\n',
            '# 🍅Tomato\n',
            '# 🥔Potato\n',
        ])
        | 'Strip header' >> beam.Map(strip, chars='# \n')
        | beam.Map(print))
    # [END map_multiple_arguments]
    if test:
      test(plants)


def map_tuple(test=None):
  # [START map_tuple]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            ('🍓', 'Strawberry'),
            ('🥕', 'Carrot'),
            ('🍆', 'Eggplant'),
            ('🍅', 'Tomato'),
            ('🥔', 'Potato'),
        ])
        | 'Format' >>
        beam.MapTuple(lambda icon, plant: '{}{}'.format(icon, plant))
        | beam.Map(print))
    # [END map_tuple]
    if test:
      test(plants)


def map_side_inputs_singleton(test=None):
  # [START map_side_inputs_singleton]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    chars = pipeline | 'Create chars' >> beam.Create(['# \n'])

    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            '# 🍓Strawberry\n',
            '# 🥕Carrot\n',
            '# 🍆Eggplant\n',
            '# 🍅Tomato\n',
            '# 🥔Potato\n',
        ])
        | 'Strip header' >> beam.Map(
            lambda text,
            chars: text.strip(chars),
            chars=beam.pvalue.AsSingleton(chars),
        )
        | beam.Map(print))
    # [END map_side_inputs_singleton]
    if test:
      test(plants)


def map_side_inputs_iter(test=None):
  # [START map_side_inputs_iter]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    chars = pipeline | 'Create chars' >> beam.Create(['#', ' ', '\n'])

    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            '# 🍓Strawberry\n',
            '# 🥕Carrot\n',
            '# 🍆Eggplant\n',
            '# 🍅Tomato\n',
            '# 🥔Potato\n',
        ])
        | 'Strip header' >> beam.Map(
            lambda text,
            chars: text.strip(''.join(chars)),
            chars=beam.pvalue.AsIter(chars),
        )
        | beam.Map(print))
    # [END map_side_inputs_iter]
    if test:
      test(plants)


def map_side_inputs_dict(test=None):
  # [START map_side_inputs_dict]
  import apache_beam as beam

  def replace_duration(plant, durations):
    plant['duration'] = durations[plant['duration']]
    return plant

  with beam.Pipeline() as pipeline:
    durations = pipeline | 'Durations' >> beam.Create([
        (0, 'annual'),
        (1, 'biennial'),
        (2, 'perennial'),
    ])

    plant_details = (
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
                'icon': '🥔', 'name': 'Potato', 'duration': 2
            },
        ])
        | 'Replace duration' >> beam.Map(
            replace_duration,
            durations=beam.pvalue.AsDict(durations),
        )
        | beam.Map(print))
    # [END map_side_inputs_dict]
    if test:
      test(plant_details)
