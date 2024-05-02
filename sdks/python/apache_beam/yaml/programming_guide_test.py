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

import logging
import unittest

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.yaml.readme_test import create_test_method
from apache_beam.yaml.yaml_transform import YamlTransform

DATA = [
    beam.Row(animal='cat', weight=1),
    beam.Row(animal='cat', weight=5),
    beam.Row(animal='dog', weight=10),
]


class ProgrammingGuideTest(unittest.TestCase):
  test_pipelines_constructing_reading = create_test_method(
      'RUN',
      'test_pipelines_constructing_reading',
      '''
      # [START pipelines_constructing_reading]
      pipeline:
        source:
          type: ReadFromText
          config:
            path: ...
      # [END pipelines_constructing_reading]
        transforms: []
      ''')

  test_create_pcollection = create_test_method(
      'RUN',
      'test_create_pcollection',
      '''
      # [START create_pcollection]
      pipeline:
        transforms:
          - type: Create
            config:
              elements:
                - A
                - B
                - ...
      # [END create_pcollection]
        transforms: []
      ''')

  def test_group_by(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          # [START group_by]
          type: Combine
          config:
            group_by: animal
            combine:
              weight: group
          # [END group_by]
          ''')
      assert_that(
          result | beam.Map(lambda x: beam.Row(**x._asdict())),
          equal_to([
              beam.Row(animal='cat', weight=[1, 5]),
              beam.Row(animal='dog', weight=[10]),
          ]))

  def test_co_group_by(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: composite
          transforms:
            # [START cogroupbykey_inputs]
            - type: Create
              name: CreateEmails
              config:
                elements:
                  - { name: "amy", email: "amy@example.com" }
                  - { name: "carl", email: "carl@example.com" }
                  - { name: "julia", email: "julia@example.com" }
                  - { name: "carl", email: "carl@email.com" }

            - type: Create
              name: CreatePhones
              config:
                elements:
                  - { name: "amy", phone: "111-222-3333" }
                  - { name: "james", phone: "222-333-4444" }
                  - { name: "amy", phone: "333-444-5555" }
                  - { name: "carl", phone: "444-555-6666" }
              # [END cogroupbykey_inputs]

              # [START cogroupbykey]
            - type: MapToFields
              name: PrepareEmails
              input: CreateEmails
              config:
                language: python
                fields:
                  name: name
                  email: "[email]"
                  phone: "[]"

            - type: MapToFields
              name: PreparePhones
              input: CreatePhones
              config:
                language: python
                fields:
                  name: name
                  email: "[]"
                  phone: "[phone]"

            - type: Combine
              name: CoGropuBy
              input: [PrepareEmails, PreparePhones]
              config:
                group_by: [name]
                combine:
                  email: concat
                  phone: concat

            - type: MapToFields
              name: FormatResults
              input: CoGropuBy
              config:
                language: python
                fields:
                  formatted:
                      "'%s; %s; %s' % (name, sorted(email), sorted(phone))"
            # [END cogroupbykey]

          output: FormatResults
          ''')
      assert_that(
          result | beam.Map(lambda x: x.formatted),
          equal_to([
              # [START cogroupbykey_formatted_outputs]
              "amy; ['amy@example.com']; ['111-222-3333', '333-444-5555']",
              "carl; ['carl@email.com', 'carl@example.com']; ['444-555-6666']",
              "james; []; ['222-333-4444']",
              "julia; ['julia@example.com']; []",
              # [END cogroupbykey_formatted_outputs]
          ]))

  def test_combine_ref(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          # [START combine_simple_py]
          type: Combine
          config:
            language: python
            group_by: animal
            combine:
              biggest:
                fn:
                  type: 'apache_beam.transforms.combiners.TopCombineFn'
                  config:
                    n: 2
                value: weight
          # [END combine_simple_py]
          ''')
      assert_that(
          result | beam.Map(lambda x: beam.Row(**x._asdict())),
          equal_to([
              beam.Row(animal='cat', biggest=[5, 1]),
              beam.Row(animal='dog', biggest=[10]),
          ]))

  def test_combine_globally(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          # [START combine_global_sum]
          type: Combine
          config:
            group_by: []
            combine:
              weight: sum
          # [END combine_global_sum]
          ''')
      assert_that(
          result | beam.Map(lambda x: beam.Row(**x._asdict())),
          equal_to([
              beam.Row(weight=16),
          ]))

  def test_combine_per_key(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          # [START combine_per_key]
          type: Combine
          config:
            group_by: [animal]
            combine:
              total_weight:
                fn: sum
                value: weight
              average_weight:
                fn: mean
                value: weight
          # [END combine_per_key]
          ''')
      assert_that(
          result | beam.Map(lambda x: beam.Row(**x._asdict())),
          equal_to([
              beam.Row(animal='cat', total_weight=6, average_weight=3),
              beam.Row(animal='dog', total_weight=10, average_weight=10),
          ]))

  def test_flatten(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: composite
          transforms:
          - type: Create
            name: SomeProducingTransform
            config:
              elements: [a, b, c]

          - type: Create
            name: AnotherProducingTransform
            config:
              elements: [d, e, f]

            # [START model_multiple_pcollections_flatten]
          - type: Flatten
            input: [SomeProducingTransform, AnotherProducingTransform]
            # [END model_multiple_pcollections_flatten]

          output: Flatten
          ''')
      assert_that(
          result | beam.Map(lambda x: x.element),
          equal_to(['a', 'b', 'c', 'd', 'e', 'f']))

  def test_expode(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: composite
          transforms:
          - type: Create
            name: SomeProducingTransform
            config:
              elements:
                - {line: "a aa"}
                - {line: "b"}

          # [START model_multiple_output_dofn]
          - type: MapToFields
            input: SomeProducingTransform
            config:
              language: python
              fields:
                word: "line.split()"

          - type: Explode
            input: MapToFields
            config:
              fields: word
            # [END model_multiple_output_dofn]

          output: Explode
          ''')
      assert_that(
          result | beam.Map(lambda x: x.word), equal_to(['a', 'aa', 'b']))

  def test_schema_output_type(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          # [START schema_output_type]
          type: MapToFields
          config:
            language: python
            fields:
              new_field:
                expression: "hex(weight)"
                output_type: { "type": "string" }
          # [END schema_output_type]
          ''')
      assert_that(
          result | beam.Map(lambda x: x.new_field),
          equal_to(['0x1', '0x5', '0xa']))

  def test_fixed_windows(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      _ = elements | YamlTransform(
          '''
          # [START setting_fixed_windows]
          type: WindowInto
          windowing:
            type: fixed
            size: 60s
          # [END setting_fixed_windows]
          ''')

  def test_sliding_windows(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      _ = elements | YamlTransform(
          '''
          # [START setting_sliding_windows]
          type: WindowInto
          windowing:
            type: sliding
            size: 5m
            period: 30s
          # [END setting_sliding_windows]
          ''')

  def test_session_windows(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      _ = elements | YamlTransform(
          '''
          # [START setting_session_windows]
          type: WindowInto
          windowing:
            type: sessions
            gap: 60s
          # [END setting_session_windows]
          ''')

  def test_global_windows(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      _ = elements | YamlTransform(
          '''
          # [START setting_global_window]
          type: WindowInto
          windowing:
            type: global
          # [END setting_global_window]
          ''')

  def test_assign_timestamps(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create([
          beam.Row(external_timestamp_field='2024-01-01', value=1),
          beam.Row(external_timestamp_field='2024-01-01', value=2),
          beam.Row(external_timestamp_field='2024-01-02', value=10),
      ])
      _ = elements | YamlTransform(
          '''
          # [START setting_timestamp]
          type: AssignTimestamps
          config:
            language: python
            timestamp:
              callable: |
                import datetime

                def extract_timestamp(x):
                  raw = datetime.datetime.strptime(
                      x.external_timestamp_field, "%Y-%m-%d")
                  return raw.astimezone(datetime.timezone.utc)
          # [END setting_timestamp]
          ''')

  def test_partition(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create([
          beam.Row(percentile=1),
          beam.Row(percentile=20),
          beam.Row(percentile=90),
      ])
      _ = elements | YamlTransform(
          '''
          # [START model_multiple_pcollections_partition]
          type: Partition
          config:
            by: str(percentile // 10)
            language: python
            outputs: ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]
          # [END model_multiple_pcollections_partition]
          ''')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
