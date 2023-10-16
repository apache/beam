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
import os
import shutil
import tempfile
import unittest

import apache_beam as beam
from apache_beam.io import localfilesystem
from apache_beam.options import pipeline_options
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.yaml.yaml_transform import YamlTransform


def AsRows():
  return beam.Map(lambda named_tuple: beam.Row(**named_tuple._asdict()))


class YamlUDFMappingTest(unittest.TestCase):
  def __init__(self, method_name='runYamlMappingTest'):
    super().__init__(method_name)
    self.data = [
        beam.Row(label='11a', conductor=11, rank=0),
        beam.Row(label='37a', conductor=37, rank=1),
        beam.Row(label='389a', conductor=389, rank=2),
    ]

  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()
    self.fs = localfilesystem.LocalFileSystem(pipeline_options)

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

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
      ''')
      assert_that(
          result,
          equal_to([
              beam.Row(label='11ax', conductor=12),
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
      ''')
      assert_that(
          result,
          equal_to([
              beam.Row(label='11ax', conductor=12),
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
          result | AsRows(),
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
          result | AsRows(),
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
          expression: "label.toUpperCase().indexOf('3') == -1 && conductor"
      ''')
      assert_that(
          result | AsRows(),
          equal_to([
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
          result | AsRows(),
          equal_to([
              beam.Row(label='11a', conductor=11, rank=0),
          ]))

  def test_filter_inline_js_file(self):
    data = '''
    function f(x) {
      return x.rank > 0
    }

    function g(x) {
      return x.rank > 1
    }
    '''.replace('    ', '')

    path = os.path.join(self.tmpdir, 'udf.js')
    self.fs.create(path).write(data.encode('utf8'))

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
          result | AsRows(),
          equal_to([
              beam.Row(label='37a', conductor=37, rank=1),
              beam.Row(label='389a', conductor=389, rank=2),
          ]))

  def test_filter_inline_py_file(self):
    data = '''
    def f(x):
      return x.rank > 0

    def g(x):
      return x.rank > 1
    '''.replace('    ', '')

    path = os.path.join(self.tmpdir, 'udf.py')
    self.fs.create(path).write(data.encode('utf8'))

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
          result | AsRows(),
          equal_to([
              beam.Row(label='37a', conductor=37, rank=1),
              beam.Row(label='389a', conductor=389, rank=2),
          ]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
