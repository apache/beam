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
from apache_beam.yaml.readme_test import createTestSuite
from apache_beam.yaml.yaml_transform import YamlTransform

DATA = [
    beam.Row(label='11a', conductor=11, rank=0),
    beam.Row(label='37a', conductor=37, rank=1),
    beam.Row(label='389a', conductor=389, rank=2),
]


class YamlMappingTest(unittest.TestCase):
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()
    self.fs = localfilesystem.LocalFileSystem(pipeline_options)

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def test_basic(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
        type: MapToFields
        input: input
        config:
            language: python
            fields:
              label: label
              isogeny: "label[-1]"
        ''')
      assert_that(
          result,
          equal_to([
              beam.Row(label='11a', isogeny='a'),
              beam.Row(label='37a', isogeny='a'),
              beam.Row(label='389a', isogeny='a'),
          ]))

  def test_drop(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
        type: MapToFields
        input: input
        config:
            fields: {}
            append: true
            drop: [conductor]
        ''')
      assert_that(
          result,
          equal_to([
              beam.Row(label='11a', rank=0),
              beam.Row(label='37a', rank=1),
              beam.Row(label='389a', rank=2),
          ]))

  def test_filter(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
        type: MapToFields
        input: input
        config:
            language: python
            fields:
              label: label
            keep: "rank > 0"
        ''')
      assert_that(
          result, equal_to([
              beam.Row(label='37a'),
              beam.Row(label='389a'),
          ]))

  def test_explode(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create([
          beam.Row(a=2, b='abc', c=.25),
          beam.Row(a=3, b='xy', c=.125),
      ])
      result = elements | YamlTransform(
          '''
        type: MapToFields
        input: input
        config:
            language: python
            append: true
            fields:
              range: "range(a)"
            explode: [range, b]
            cross_product: true
        ''')
      assert_that(
          result,
          equal_to([
              beam.Row(a=2, b='a', c=.25, range=0),
              beam.Row(a=2, b='a', c=.25, range=1),
              beam.Row(a=2, b='b', c=.25, range=0),
              beam.Row(a=2, b='b', c=.25, range=1),
              beam.Row(a=2, b='c', c=.25, range=0),
              beam.Row(a=2, b='c', c=.25, range=1),
              beam.Row(a=3, b='x', c=.125, range=0),
              beam.Row(a=3, b='x', c=.125, range=1),
              beam.Row(a=3, b='x', c=.125, range=2),
              beam.Row(a=3, b='y', c=.125, range=0),
              beam.Row(a=3, b='y', c=.125, range=1),
              beam.Row(a=3, b='y', c=.125, range=2),
          ]))

  def test_map_to_fields_filter_inline_js(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
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
      other: "conductor / 2"
    keep:
      callable: "function filter(x) {return x.rank > 0}"
  ''')
      assert_that(
          result,
          equal_to([
              beam.Row(label='37ax', conductor=38, other=18.5),
              beam.Row(label='389ax', conductor=390, other=194.5),
          ]))

  def test_map_to_fields_filter_inline_py(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
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
      other: 
        expression: "conductor / 2.0"
    keep: 
      callable: "lambda x: x.rank > 0"
  ''')
      assert_that(
          result,
          equal_to([
              beam.Row(label='37ax', conductor=38, other=18.5),
              beam.Row(label='389ax', conductor=390, other=194.5),
          ]))

  def test_filter_inline_js(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
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
      elements = p | beam.Create(DATA)
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
      elements = p | beam.Create(DATA)
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
          result, equal_to([
              beam.Row(label='11a', conductor=11, rank=0),
          ]))

  def test_filter_expression_py(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
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
    file_data = '''
    function f(x) {
      return x.rank > 0
    }
    function g(x) {
      return x.rank > 1
    }
    '''.replace('    ', '')

    path = os.path.join(self.tmpdir, 'udf.js')
    self.fs.create(path).write(file_data.encode('utf8'))

    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
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
    file_data = '''
    def f(x):
      return x.rank > 0
    def g(x):
      return x.rank > 1
    '''.replace('    ', '')

    path = os.path.join(self.tmpdir, 'udf.py')
    self.fs.create(path).write(file_data.encode('utf8'))

    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
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

  def test_map_to_fields_filter_mixed_js(self):
    file_data = '''
        function f(x) {
          return x.rank > 0
        }
        function g(x) {
          return x.rank > 1
        }
        '''.replace('    ', '')

    path = os.path.join(self.tmpdir, 'udf.js')
    self.fs.create(path).write(file_data.encode('utf8'))

    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          f'''
    type: MapToFields
    input: input
    config:
      language: javascript
      fields:
        label:
          expression: "label + 'x'"
        conductor:
          callable: "function conductor_map(x) {{return x.conductor + 1}}"
        other: "conductor / 2"
      keep:
        path: {path}
        name: "f"
    ''')
      assert_that(
          result,
          equal_to([
              beam.Row(label='37ax', conductor=38, other=18.5),
              beam.Row(label='389ax', conductor=390, other=194.5),
          ]))

  def test_map_to_fields_filter_mixed_py(self):
    file_data = '''
        def f(x):
          return x.rank > 0
        def g(x):
          return x.rank > 1
        '''.replace('    ', '')

    path = os.path.join(self.tmpdir, 'udf.py')
    self.fs.create(path).write(file_data.encode('utf8'))

    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          f'''
    type: MapToFields
    input: input
    config:
      language: python
      fields:
        label:
          expression: "label + 'x'"
        conductor:
          callable: "lambda x: x.conductor + 1"
        other: "conductor / 2.0"
      keep:
        path: {path}
        name: "f"
    ''')
      assert_that(
          result,
          equal_to([
              beam.Row(label='37ax', conductor=38, other=18.5),
              beam.Row(label='389ax', conductor=390, other=194.5),
          ]))


YamlMappingDocTest = createTestSuite(
    'YamlMappingDocTest',
    os.path.join(os.path.dirname(__file__), 'yaml_mapping.md'))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
