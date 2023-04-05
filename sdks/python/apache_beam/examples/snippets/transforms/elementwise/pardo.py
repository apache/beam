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


def pardo_dofn(test=None):
  # [START pardo_dofn]
  import apache_beam as beam

  class SplitWords(beam.DoFn):
    def __init__(self, delimiter=','):
      self.delimiter = delimiter

    def process(self, text):
      for word in text.split(self.delimiter):
        yield word

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            '🍓Strawberry,🥕Carrot,🍆Eggplant',
            '🍅Tomato,🥔Potato',
        ])
        | 'Split words' >> beam.ParDo(SplitWords(','))
        | beam.Map(print))
    # [END pardo_dofn]
    if test:
      test(plants)


def pardo_dofn_params(test=None):
  # pylint: disable=line-too-long
  # [START pardo_dofn_params]
  import apache_beam as beam

  class AnalyzeElement(beam.DoFn):
    def process(
        self,
        elem,
        timestamp=beam.DoFn.TimestampParam,
        window=beam.DoFn.WindowParam):
      yield '\n'.join([
          '# timestamp',
          'type(timestamp) -> ' + repr(type(timestamp)),
          'timestamp.micros -> ' + repr(timestamp.micros),
          'timestamp.to_rfc3339() -> ' + repr(timestamp.to_rfc3339()),
          'timestamp.to_utc_datetime() -> ' + repr(timestamp.to_utc_datetime()),
          '',
          '# window',
          'type(window) -> ' + repr(type(window)),
          'window.start -> {} ({})'.format(
              window.start, window.start.to_utc_datetime()),
          'window.end -> {} ({})'.format(
              window.end, window.end.to_utc_datetime()),
          'window.max_timestamp() -> {} ({})'.format(
              window.max_timestamp(), window.max_timestamp().to_utc_datetime()),
      ])

  with beam.Pipeline() as pipeline:
    dofn_params = (
        pipeline
        | 'Create a single test element' >> beam.Create([':)'])
        | 'Add timestamp (Spring equinox 2020)' >>
        beam.Map(lambda elem: beam.window.TimestampedValue(elem, 1584675660))
        |
        'Fixed 30sec windows' >> beam.WindowInto(beam.window.FixedWindows(30))
        | 'Analyze element' >> beam.ParDo(AnalyzeElement())
        | beam.Map(print))
    # [END pardo_dofn_params]
    # pylint: enable=line-too-long
    if test:
      test(dofn_params)


def pardo_dofn_methods(test=None):
  # [START pardo_dofn_methods]
  import apache_beam as beam

  class DoFnMethods(beam.DoFn):
    def __init__(self):
      print('__init__')
      self.window = beam.transforms.window.GlobalWindow()

    def setup(self):
      print('setup')

    def start_bundle(self):
      print('start_bundle')

    def process(self, element, window=beam.DoFn.WindowParam):
      self.window = window
      yield '* process: ' + element

    def finish_bundle(self):
      yield beam.utils.windowed_value.WindowedValue(
          value='* finish_bundle: 🌱🌳🌍',
          timestamp=0,
          windows=[self.window],
      )

    def teardown(self):
      print('teardown')

  with beam.Pipeline() as pipeline:
    results = (
        pipeline
        | 'Create inputs' >> beam.Create(['🍓', '🥕', '🍆', '🍅', '🥔'])
        | 'DoFn methods' >> beam.ParDo(DoFnMethods())
        | beam.Map(print))
    # [END pardo_dofn_methods]
    if test:
      return test(results)
