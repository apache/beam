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
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.yaml.yaml_mapping import py_value_to_js_dict
from apache_beam.yaml.yaml_provider import dicts_to_rows
from apache_beam.yaml.yaml_transform import YamlTransform

try:
  import js2py
except ImportError:
  js2py = None
  logging.warning('js2py is not installed; some tests will be skipped.')


def as_rows():
  return beam.Map(
      lambda named_tuple: dicts_to_rows(py_value_to_js_dict(named_tuple)))


class YamlUDFMappingTest(unittest.TestCase):
  def __init__(self, method_name='runYamlMappingTest'):
    super().__init__(method_name)
    self.data = [
        beam.Row(
            label='11a', conductor=11, row=beam.Row(rank=0, values=[1, 2, 3])),
        beam.Row(
            label='37a', conductor=37, row=beam.Row(rank=1, values=[4, 5, 6])),
        beam.Row(
            label='389a', conductor=389, row=beam.Row(rank=2, values=[7, 8,
                                                                      9])),
    ]

  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()
    self.fs = localfilesystem.LocalFileSystem(pipeline_options)

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  @unittest.skipIf(js2py is None, 'js2py not installed.')
  def test_map_to_fields_filter_inline_js(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle', yaml_experimental_features=['javascript'
                                                                  ])) as p:
      elements = p | beam.Create(self.data)
      result = elements | YamlTransform(
          '''
      type: MapToFields
      config:
        language: javascript
        fields:
          label:
            callable: |
              function label_map(x) {
                return x.label + 'x'
              }
          conductor:
            callable: |
              function conductor_map(x) {
                return x.conductor + 1
              }
          row:
            callable: |
              function row_map(x) {
                x.row.values.push(x.row.rank + 10)
                return x.row
              }
      ''')
      assert_that(
          result,
          equal_to([
              beam.Row(
                  label='11ax',
                  conductor=12,
                  row=beam.Row(rank=0, values=[1, 2, 3, 10])),
              beam.Row(
                  label='37ax',
                  conductor=38,
                  row=beam.Row(rank=1, values=[4, 5, 6, 11])),
              beam.Row(
                  label='389ax',
                  conductor=390,
                  row=beam.Row(rank=2, values=[7, 8, 9, 12])),
          ]))

  def test_map_to_fields_filter_inline_py(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(self.data)
      result = elements | YamlTransform(
          '''
      type: MapToFields
      config:
        language: python
        fields:
          label:
            callable: "lambda x: x.label + 'x'"
          conductor:
            callable: "lambda x: x.conductor + 1"
          sum:
            callable: "lambda x: sum(x.row.values)"
      ''')
      assert_that(
          result,
          equal_to([
              beam.Row(label='11ax', conductor=12, sum=6),
              beam.Row(label='37ax', conductor=38, sum=15),
              beam.Row(label='389ax', conductor=390, sum=24),
          ]))

  @staticmethod
  @unittest.skipIf(
      TestPipeline().get_pipeline_options().view_as(StandardOptions).runner
      is None,
      'Do not run this test on precommit suites.')
  def test_map_to_fields_sql_reserved_keyword():
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      data = [
          beam.Row(label="11a", rank=0),
          beam.Row(label="37a", rank=1),
          beam.Row(label="389a", rank=2),
      ]
      elements = p | beam.Create(data)
      result = elements | YamlTransform(
          '''
      type: MapToFields
      config:
        language: sql
        append: true
        drop: [ rank ]
        fields:
          timestamp: "`rank`"
      ''')
      assert_that(
          result | as_rows(),
          equal_to([
              beam.Row(label='11a', timestamp=0),
              beam.Row(label='37a', timestamp=1),
              beam.Row(label='389a', timestamp=2),
          ]))

  @staticmethod
  @unittest.skipIf(
      TestPipeline().get_pipeline_options().view_as(StandardOptions).runner
      is None,
      'Do not run this test on precommit suites.')
  def test_map_to_fields_sql_reserved_keyword_append():
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      data = [
          beam.Row(label="11a", timestamp=0),
          beam.Row(label="37a", timestamp=1),
          beam.Row(label="389a", timestamp=2),
      ]
      elements = p | beam.Create(data)
      result = elements | YamlTransform(
          '''
    type: MapToFields
    config:
      language: sql
      append: true
      fields:
        label_copy: label
    ''')
      assert_that(
          result | as_rows(),
          equal_to([
              beam.Row(label='11a', timestamp=0, label_copy="11a"),
              beam.Row(label='37a', timestamp=1, label_copy="37a"),
              beam.Row(label='389a', timestamp=2, label_copy="389a"),
          ]))

  @unittest.skipIf(js2py is None, 'js2py not installed.')
  def test_filter_inline_js(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle', yaml_experimental_features=['javascript'
                                                                  ])) as p:
      elements = p | beam.Create(self.data)
      result = elements | YamlTransform(
          '''
      type: Filter
      config:
        language: javascript
        keep:
          callable: |
            function filter(x) {
              return x.row.rank > 0
            }
      ''')
      assert_that(
          result | as_rows(),
          equal_to([
              beam.Row(
                  label='37a',
                  conductor=37,
                  row=beam.Row(rank=1, values=[4, 5, 6])),
              beam.Row(
                  label='389a',
                  conductor=389,
                  row=beam.Row(rank=2, values=[7, 8, 9])),
          ]))

  def test_filter_inline_py(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(self.data)
      result = elements | YamlTransform(
          '''
      type: Filter
      config:
        language: python
        keep:
          callable: "lambda x: x.row.rank > 0"
      ''')
      assert_that(
          result | as_rows(),
          equal_to([
              beam.Row(
                  label='37a',
                  conductor=37,
                  row=beam.Row(rank=1, values=[4, 5, 6])),
              beam.Row(
                  label='389a',
                  conductor=389,
                  row=beam.Row(rank=2, values=[7, 8, 9])),
          ]))

  @unittest.skipIf(js2py is None, 'js2py not installed.')
  def test_filter_expression_js(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle', yaml_experimental_features=['javascript'
                                                                  ])) as p:
      elements = p | beam.Create(self.data)
      result = elements | YamlTransform(
          '''
      type: Filter
      config:
        language: javascript
        keep:
          expression: "label.toUpperCase().indexOf('3') == -1 && row.rank < 1"
      ''')
      assert_that(
          result | as_rows(),
          equal_to([
              beam.Row(
                  label='11a',
                  conductor=11,
                  row=beam.Row(rank=0, values=[1, 2, 3])),
          ]))

  def test_filter_expression_py(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(self.data)
      result = elements | YamlTransform(
          '''
      type: Filter
      config:
        language: python
        keep:
          expression: "'3' not in label"
      ''')
      assert_that(
          result | as_rows(),
          equal_to([
              beam.Row(
                  label='11a',
                  conductor=11,
                  row=beam.Row(rank=0, values=[1, 2, 3])),
          ]))

  @unittest.skipIf(js2py is None, 'js2py not installed.')
  def test_filter_inline_js_file(self):
    data = '''
    function f(x) {
      return x.row.rank > 0
    }

    function g(x) {
      return x.row.rank > 1
    }
    '''.replace('    ', '')

    path = os.path.join(self.tmpdir, 'udf.js')
    self.fs.create(path).write(data.encode('utf8'))

    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle', yaml_experimental_features=['javascript'
                                                                  ])) as p:
      elements = p | beam.Create(self.data)
      result = elements | YamlTransform(
          f'''
        type: Filter
        config:
          language: javascript
          keep:
            path: {path}
            name: "f"
        ''')
      assert_that(
          result | as_rows(),
          equal_to([
              beam.Row(
                  label='37a',
                  conductor=37,
                  row=beam.Row(rank=1, values=[4, 5, 6])),
              beam.Row(
                  label='389a',
                  conductor=389,
                  row=beam.Row(rank=2, values=[7, 8, 9])),
          ]))

  def test_filter_inline_py_file(self):
    data = '''
    def f(x):
      return x.row.rank > 0

    def g(x):
      return x.row.rank > 1
    '''.replace('    ', '')

    path = os.path.join(self.tmpdir, 'udf.py')
    self.fs.create(path).write(data.encode('utf8'))

    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(self.data)
      result = elements | YamlTransform(
          f'''
        type: Filter
        config:
          language: python
          keep:
            path: {path}
            name: "f"
        ''')
      assert_that(
          result | as_rows(),
          equal_to([
              beam.Row(
                  label='37a',
                  conductor=37,
                  row=beam.Row(rank=1, values=[4, 5, 6])),
              beam.Row(
                  label='389a',
                  conductor=389,
                  row=beam.Row(rank=2, values=[7, 8, 9])),
          ]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
