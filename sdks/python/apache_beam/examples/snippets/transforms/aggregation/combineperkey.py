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


def combineperkey_simple(test=None):
  # [START combineperkey_simple]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    total = (
        pipeline
        | 'Create plant counts' >> beam.Create([
            ('🥕', 3),
            ('🥕', 2),
            ('🍆', 1),
            ('🍅', 4),
            ('🍅', 5),
            ('🍅', 3),
        ])
        | 'Sum' >> beam.CombinePerKey(sum)
        | beam.Map(print))
    # [END combineperkey_simple]
    if test:
      test(total)


def combineperkey_function(test=None):
  # [START combineperkey_function]
  import apache_beam as beam

  def saturated_sum(values):
    max_value = 8
    return min(sum(values), max_value)

  with beam.Pipeline() as pipeline:
    saturated_total = (
        pipeline
        | 'Create plant counts' >> beam.Create([
            ('🥕', 3),
            ('🥕', 2),
            ('🍆', 1),
            ('🍅', 4),
            ('🍅', 5),
            ('🍅', 3),
        ])
        | 'Saturated sum' >> beam.CombinePerKey(saturated_sum)
        | beam.Map(print))
    # [END combineperkey_function]
    if test:
      test(saturated_total)


def combineperkey_lambda(test=None):
  # [START combineperkey_lambda]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    saturated_total = (
        pipeline
        | 'Create plant counts' >> beam.Create([
            ('🥕', 3),
            ('🥕', 2),
            ('🍆', 1),
            ('🍅', 4),
            ('🍅', 5),
            ('🍅', 3),
        ])
        | 'Saturated sum' >>
        beam.CombinePerKey(lambda values: min(sum(values), 8))
        | beam.Map(print))
    # [END combineperkey_lambda]
    if test:
      test(saturated_total)


def combineperkey_multiple_arguments(test=None):
  # [START combineperkey_multiple_arguments]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    saturated_total = (
        pipeline
        | 'Create plant counts' >> beam.Create([
            ('🥕', 3),
            ('🥕', 2),
            ('🍆', 1),
            ('🍅', 4),
            ('🍅', 5),
            ('🍅', 3),
        ])
        | 'Saturated sum' >> beam.CombinePerKey(
            lambda values, max_value: min(sum(values), max_value), max_value=8)
        | beam.Map(print))
    # [END combineperkey_multiple_arguments]
    if test:
      test(saturated_total)


def combineperkey_side_inputs_singleton(test=None):
  # [START combineperkey_side_inputs_singleton]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    max_value = pipeline | 'Create max_value' >> beam.Create([8])

    saturated_total = (
        pipeline
        | 'Create plant counts' >> beam.Create([
            ('🥕', 3),
            ('🥕', 2),
            ('🍆', 1),
            ('🍅', 4),
            ('🍅', 5),
            ('🍅', 3),
        ])
        | 'Saturated sum' >> beam.CombinePerKey(
            lambda values,
            max_value: min(sum(values), max_value),
            max_value=beam.pvalue.AsSingleton(max_value))
        | beam.Map(print))
    # [END combineperkey_side_inputs_singleton]
    if test:
      test(saturated_total)


def combineperkey_side_inputs_iter(test=None):
  # [START combineperkey_side_inputs_iter]
  import apache_beam as beam

  def bounded_sum(values, data_range):
    min_value = min(data_range)
    result = sum(values)
    if result < min_value:
      return min_value
    max_value = max(data_range)
    if result > max_value:
      return max_value
    return result

  with beam.Pipeline() as pipeline:
    data_range = pipeline | 'Create data_range' >> beam.Create([2, 4, 8])

    bounded_total = (
        pipeline
        | 'Create plant counts' >> beam.Create([
            ('🥕', 3),
            ('🥕', 2),
            ('🍆', 1),
            ('🍅', 4),
            ('🍅', 5),
            ('🍅', 3),
        ])
        | 'Bounded sum' >> beam.CombinePerKey(
            bounded_sum, data_range=beam.pvalue.AsIter(data_range))
        | beam.Map(print))
    # [END combineperkey_side_inputs_iter]
    if test:
      test(bounded_total)


def combineperkey_side_inputs_dict(test=None):
  # [START combineperkey_side_inputs_dict]
  import apache_beam as beam

  def bounded_sum(values, data_range):
    min_value = data_range['min']
    result = sum(values)
    if result < min_value:
      return min_value
    max_value = data_range['max']
    if result > max_value:
      return max_value
    return result

  with beam.Pipeline() as pipeline:
    data_range = pipeline | 'Create data_range' >> beam.Create([
        ('min', 2),
        ('max', 8),
    ])

    bounded_total = (
        pipeline
        | 'Create plant counts' >> beam.Create([
            ('🥕', 3),
            ('🥕', 2),
            ('🍆', 1),
            ('🍅', 4),
            ('🍅', 5),
            ('🍅', 3),
        ])
        | 'Bounded sum' >> beam.CombinePerKey(
            bounded_sum, data_range=beam.pvalue.AsDict(data_range))
        | beam.Map(print))
    # [END combineperkey_side_inputs_dict]
    if test:
      test(bounded_total)


def combineperkey_combinefn(test=None):
  # [START combineperkey_combinefn]
  import apache_beam as beam

  class AverageFn(beam.CombineFn):
    def create_accumulator(self):
      sum = 0.0
      count = 0
      accumulator = sum, count
      return accumulator

    def add_input(self, accumulator, input):
      sum, count = accumulator
      return sum + input, count + 1

    def merge_accumulators(self, accumulators):
      # accumulators = [(sum1, count1), (sum2, count2), (sum3, count3), ...]
      sums, counts = zip(*accumulators)
      # sums = [sum1, sum2, sum3, ...]
      # counts = [count1, count2, count3, ...]
      return sum(sums), sum(counts)

    def extract_output(self, accumulator):
      sum, count = accumulator
      if count == 0:
        return float('NaN')
      return sum / count

  with beam.Pipeline() as pipeline:
    average = (
        pipeline
        | 'Create plant counts' >> beam.Create([
            ('🥕', 3),
            ('🥕', 2),
            ('🍆', 1),
            ('🍅', 4),
            ('🍅', 5),
            ('🍅', 3),
        ])
        | 'Average' >> beam.CombinePerKey(AverageFn())
        | beam.Map(print))
    # [END combineperkey_combinefn]
    if test:
      test(average)
