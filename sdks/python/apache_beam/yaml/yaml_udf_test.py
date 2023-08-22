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
from unittest import mock

import apache_beam as beam
from apache_beam.options import pipeline_options
from apache_beam.testing.util import equal_to, assert_that
from apache_beam.yaml.yaml_transform import YamlTransform

MOCK_UDF_FILE_PREFIX = "/path/to/udf"


class YamlUDFMappingTest(unittest.TestCase):
  def __init__(self, method_name='runYamlMappingTest'):
    super().__init__(method_name)
    self.data = [
        beam.Row(label='11a', conductor=11, rank=0),
        beam.Row(label='37a', conductor=37, rank=1),
        beam.Row(label='389a', conductor=389, rank=2),
    ]
    self.builtin_open = open  # save the unpatched version

  class MockBlob:
    def __init__(self, data):
      self.data = data

    def download_as_string(self):
      return bytes(self.data, encoding='utf-8')

  class MockBucket:
    def __init__(self, data):
      self.data = data

    def blob(self, gcs_folder):
      return YamlUDFMappingTest.MockBlob(self.data)

  def test_map_to_fields_filter_inline_js(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(self.data)
      result = elements | YamlTransform(
          '''
        type: MapToFields
        input: input
        config:
          language: javascript
          fields:
            label:
              callable: "function label_map(x) {return x.label + 'x'}"
            conductor:
              callable: "function conductor_map(x) {return x.conductor + 1}"
          keep:
            callable: "function filter(x) {return x.rank > 0}"
        ''')
      assert_that(
          result,
          equal_to([
              beam.Row(label='37ax', conductor=38),
              beam.Row(label='389ax', conductor=390),
          ]))

  def test_map_to_fields_filter_inline_py(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(self.data)
      result = elements | YamlTransform(
          '''
        type: MapToFields
        input: input
        config:
          language: python
          fields:
            label:
              callable: "lambda x: x.label + 'x'"
            conductor:
              callable: "lambda x: x.conductor + 1"
          keep: 
            callable: "lambda x: x.rank > 0"
        ''')
      assert_that(
          result,
          equal_to([
              beam.Row(label='37ax', conductor=38),
              beam.Row(label='389ax', conductor=390),
          ]))

  def test_filter_inline_js(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(self.data)
      result = elements | YamlTransform(
          '''
        type: Filter
        input: input
        config:
          language: javascript
          keep: 
            callable: "function filter(x) {return x.rank > 0}"
        ''')
      assert_that(
          result,
          equal_to([
              beam.Row(label='37a', conductor=37, rank=1),
              beam.Row(label='389a', conductor=389, rank=2),
          ]))

  def test_filter_inline_py(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(self.data)
      result = elements | YamlTransform(
          '''
        type: Filter
        input: input
        config:
          language: python
          keep: 
            callable: "lambda x: x.rank > 0"
        ''')
      assert_that(
          result,
          equal_to([
              beam.Row(label='37a', conductor=37, rank=1),
              beam.Row(label='389a', conductor=389, rank=2),
          ]))

  def test_filter_expression_js(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(self.data)
      result = elements | YamlTransform(
          '''
        type: Filter
        input: input
        config:
          language: javascript
          keep: 
            expression: "label.toUpperCase().indexOf('3') == -1"
        ''')
      assert_that(
          result, equal_to([
              beam.Row(label='11a', conductor=11, rank=0),
          ]))

  def test_filter_expression_py(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(self.data)
      result = elements | YamlTransform(
          '''
        type: Filter
        input: input
        config:
          language: python
          keep: 
            expression: "'3' not in label"
        ''')
      assert_that(
          result, equal_to([
              beam.Row(label='11a', conductor=11, rank=0),
          ]))

  def test_filter_inline_js_file(self):
    path = "/path/to/udf.js"
    data = '''
    function f(x) {
      return x.rank > 0
    }

    function g(x) {
      return x.rank > 1
    }'''

    def mock_open(*args, **kwargs):
      if args[0] == path:
        return mock.mock_open(read_data=data)(*args, **kwargs)
      return self.builtin_open(*args, **kwargs)

    with mock.patch('builtins.open', mock_open):
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        elements = p | beam.Create(self.data)
        result = elements | YamlTransform(
            f'''
          type: Filter
          input: input
          config:
            language: javascript
            keep: 
              path: {path}
              name: "f"
          ''')
        assert_that(
            result,
            equal_to([
                beam.Row(label='37a', conductor=37, rank=1),
                beam.Row(label='389a', conductor=389, rank=2),
            ]))

  def test_filter_inline_py_file(self):
    path = "/path/to/udf.py"
    data = '''
    def f(x):
      return x.rank > 0

    def g(x):
      return x.rank > 1'''

    def mock_open(*args, **kwargs):
      if args[0] == path:
        return mock.mock_open(read_data=data)(*args, **kwargs)
      return self.builtin_open(*args, **kwargs)

    with mock.patch('builtins.open', mock_open):
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        elements = p | beam.Create(self.data)
        result = elements | YamlTransform(
            f'''
          type: Filter
          input: input
          config:
            language: python
            keep: 
              path: {path}
              name: "f"
          ''')
        assert_that(
            result,
            equal_to([
                beam.Row(label='37a', conductor=37, rank=1),
                beam.Row(label='389a', conductor=389, rank=2),
            ]))

  def test_filter_inline_js_gcs_file(self):
    path = "gs://mock-bucket/path/to/udf.js"
    data = '''
    function f(x) {
      return x.rank > 0
    }

    function g(x) {
      return x.rank > 1
    }'''

    class MockGCSClient:
      def get_bucket(self, bucket_name):
        return YamlUDFMappingTest.MockBucket(data)

    with mock.patch('google.cloud.storage.Client', MockGCSClient):
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        elements = p | beam.Create(self.data)
        result = elements | YamlTransform(
            f'''
          type: Filter
          input: input
          config:
            language: javascript
            keep: 
              path: {path}
              name: "f"
          ''')
        assert_that(
            result,
            equal_to([
                beam.Row(label='37a', conductor=37, rank=1),
                beam.Row(label='389a', conductor=389, rank=2),
            ]))

  def test_filter_inline_py_gcs_file(self):
    path = "gs://mock-bucket/path/to/udf.py"
    data = '''
    def f(x):
      return x.rank > 0

    def g(x):
      return x.rank > 1'''

    class MockGCSClient:
      def get_bucket(self, bucket_name):
        return YamlUDFMappingTest.MockBucket(data)

    with mock.patch('google.cloud.storage.Client', MockGCSClient):
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        elements = p | beam.Create(self.data)
        result = elements | YamlTransform(
            f'''
          type: Filter
          input: input
          config:
            language: python
            keep: 
              path: {path}
              name: "f"
          ''')
        assert_that(
            result,
            equal_to([
                beam.Row(label='37a', conductor=37, rank=1),
                beam.Row(label='389a', conductor=389, rank=2),
            ]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
