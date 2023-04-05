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


def combineglobally_function(test=None):
  # [START combineglobally_function]
  import apache_beam as beam

  def get_common_items(sets):
    # set.intersection() takes multiple sets as separete arguments.
    # We unpack the `sets` list into multiple arguments with the * operator.
    # The combine transform might give us an empty list of `sets`,
    # so we use a list with an empty set as a default value.
    return set.intersection(*(sets or [set()]))

  with beam.Pipeline() as pipeline:
    common_items = (
        pipeline
        | 'Create produce' >> beam.Create([
            {'🍓', '🥕', '🍌', '🍅', '🌶️'},
            {'🍇', '🥕', '🥝', '🍅', '🥔'},
            {'🍉', '🥕', '🍆', '🍅', '🍍'},
            {'🥑', '🥕', '🌽', '🍅', '🥥'},
        ])
        | 'Get common items' >> beam.CombineGlobally(get_common_items)
        | beam.Map(print))
    # [END combineglobally_function]
    if test:
      test(common_items)


def combineglobally_lambda(test=None):
  # [START combineglobally_lambda]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    common_items = (
        pipeline
        | 'Create produce' >> beam.Create([
            {'🍓', '🥕', '🍌', '🍅', '🌶️'},
            {'🍇', '🥕', '🥝', '🍅', '🥔'},
            {'🍉', '🥕', '🍆', '🍅', '🍍'},
            {'🥑', '🥕', '🌽', '🍅', '🥥'},
        ])
        | 'Get common items' >>
        beam.CombineGlobally(lambda sets: set.intersection(*(sets or [set()])))
        | beam.Map(print))
    # [END combineglobally_lambda]
    if test:
      test(common_items)


def combineglobally_multiple_arguments(test=None):
  # [START combineglobally_multiple_arguments]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    common_items_with_exceptions = (
        pipeline
        | 'Create produce' >> beam.Create([
            {'🍓', '🥕', '🍌', '🍅', '🌶️'},
            {'🍇', '🥕', '🥝', '🍅', '🥔'},
            {'🍉', '🥕', '🍆', '🍅', '🍍'},
            {'🥑', '🥕', '🌽', '🍅', '🥥'},
        ])
        | 'Get common items with exceptions' >> beam.CombineGlobally(
            lambda sets, exclude: \
                set.intersection(*(sets or [set()])) - exclude,
            exclude={'🥕'})
        | beam.Map(print)
    )
    # [END combineglobally_multiple_arguments]
    if test:
      test(common_items_with_exceptions)


def combineglobally_side_inputs_singleton(test=None):
  # [START combineglobally_side_inputs_singleton]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    single_exclude = pipeline | 'Create single_exclude' >> beam.Create(['🥕'])

    common_items_with_exceptions = (
        pipeline
        | 'Create produce' >> beam.Create([
            {'🍓', '🥕', '🍌', '🍅', '🌶️'},
            {'🍇', '🥕', '🥝', '🍅', '🥔'},
            {'🍉', '🥕', '🍆', '🍅', '🍍'},
            {'🥑', '🥕', '🌽', '🍅', '🥥'},
        ])
        | 'Get common items with exceptions' >> beam.CombineGlobally(
            lambda sets, single_exclude: \
                set.intersection(*(sets or [set()])) - {single_exclude},
            single_exclude=beam.pvalue.AsSingleton(single_exclude))
        | beam.Map(print)
    )
    # [END combineglobally_side_inputs_singleton]
    if test:
      test(common_items_with_exceptions)


def combineglobally_side_inputs_iter(test=None):
  # [START combineglobally_side_inputs_iter]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    exclude = pipeline | 'Create exclude' >> beam.Create(['🥕'])

    common_items_with_exceptions = (
        pipeline
        | 'Create produce' >> beam.Create([
            {'🍓', '🥕', '🍌', '🍅', '🌶️'},
            {'🍇', '🥕', '🥝', '🍅', '🥔'},
            {'🍉', '🥕', '🍆', '🍅', '🍍'},
            {'🥑', '🥕', '🌽', '🍅', '🥥'},
        ])
        | 'Get common items with exceptions' >> beam.CombineGlobally(
            lambda sets, exclude: \
                set.intersection(*(sets or [set()])) - set(exclude),
            exclude=beam.pvalue.AsIter(exclude))
        | beam.Map(print)
    )
    # [END combineglobally_side_inputs_iter]
    if test:
      test(common_items_with_exceptions)


def combineglobally_side_inputs_dict(test=None):
  # [START combineglobally_side_inputs_dict]
  import apache_beam as beam

  def get_custom_common_items(sets, options):
    sets = sets or [set()]
    common_items = set.intersection(*sets)
    common_items |= options['include']  # union
    common_items &= options['exclude']  # intersection
    return common_items

  with beam.Pipeline() as pipeline:
    options = pipeline | 'Create options' >> beam.Create([
        ('exclude', {'🥕'}),
        ('include', {'🍇', '🌽'}),
    ])

    custom_common_items = (
        pipeline
        | 'Create produce' >> beam.Create([
            {'🍓', '🥕', '🍌', '🍅', '🌶️'},
            {'🍇', '🥕', '🥝', '🍅', '🥔'},
            {'🍉', '🥕', '🍆', '🍅', '🍍'},
            {'🥑', '🥕', '🌽', '🍅', '🥥'},
        ])
        | 'Get common items' >> beam.CombineGlobally(
            get_custom_common_items, options=beam.pvalue.AsDict(options))
        | beam.Map(print))
    # [END combineglobally_side_inputs_dict]
    if test:
      test(custom_common_items)


def combineglobally_combinefn(test=None):
  # [START combineglobally_combinefn]
  import apache_beam as beam

  class PercentagesFn(beam.CombineFn):
    def create_accumulator(self):
      return {}

    def add_input(self, accumulator, input):
      # accumulator == {}
      # input == '🥕'
      if input not in accumulator:
        accumulator[input] = 0  # {'🥕': 0}
      accumulator[input] += 1  # {'🥕': 1}
      return accumulator

    def merge_accumulators(self, accumulators):
      # accumulators == [
      #     {'🥕': 1, '🍅': 2},
      #     {'🥕': 1, '🍅': 1, '🍆': 1},
      #     {'🥕': 1, '🍅': 3},
      # ]
      merged = {}
      for accum in accumulators:
        for item, count in accum.items():
          if item not in merged:
            merged[item] = 0
          merged[item] += count
      # merged == {'🥕': 3, '🍅': 6, '🍆': 1}
      return merged

    def extract_output(self, accumulator):
      # accumulator == {'🥕': 3, '🍅': 6, '🍆': 1}
      total = sum(accumulator.values())  # 10
      percentages = {item: count / total for item, count in accumulator.items()}
      # percentages == {'🥕': 0.3, '🍅': 0.6, '🍆': 0.1}
      return percentages

  with beam.Pipeline() as pipeline:
    percentages = (
        pipeline
        | 'Create produce' >> beam.Create(
            ['🥕', '🍅', '🍅', '🥕', '🍆', '🍅', '🍅', '🍅', '🥕', '🍅'])
        | 'Get percentages' >> beam.CombineGlobally(PercentagesFn())
        | beam.Map(print))
    # [END combineglobally_combinefn]
    if test:
      test(percentages)
