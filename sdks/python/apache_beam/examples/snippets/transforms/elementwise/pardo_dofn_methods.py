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
# pylint:disable=line-too-long

# beam-playground:
#   name: ParDoDoFnMethods
#   description: Demonstration of ParDo transform usage with a DoFn with methods.
#   multifile: false
#   default_example: false
#   context_line: 40
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - strings


def pardo_dofn_methods(test=None):
  # Portable runners do not guarantee that teardown will be executed, so we
  # use FnApiRunner instead of prism.
  runner = 'FnApiRunner'
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
      # Teardown is best effort and not guaranteed to be executed by all
      # runners in all cases (for example, it may be skipped if the pipeline
      # can otherwise complete). It should be used for best effort resource
      # cleanup.
      print('teardown')

  with beam.Pipeline(runner) as pipeline:
    results = (
        pipeline
        | 'Create inputs' >> beam.Create(['🍓', '🥕', '🍆', '🍅', '🥔'])
        | 'DoFn methods' >> beam.ParDo(DoFnMethods())
        | beam.Map(print))
    # [END pardo_dofn_methods]
    if test:
      return test(results)


if __name__ == '__main__':
  pardo_dofn_methods()
