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
import tempfile
import unittest
from typing import Any
from unittest import mock

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.yaml import yaml_ml
from apache_beam.yaml.yaml_transform import YamlTransform

try:
  # pylint: disable=wrong-import-order, wrong-import-position, unused-import
  from apache_beam.ml.transforms import tft
except ImportError:
  tft = None

try:
  import sentence_transformers
except ImportError:
  sentence_transformers = None

TRAIN_DATA = [
    beam.Row(num=0, text='And God said, Let there be light,'),
    beam.Row(num=2, text='And there was light'),
    beam.Row(num=8, text='And God saw the light, that it was good'),
]

TEST_DATA = [
    beam.Row(num=6, text='And God divided the light from the darkness.'),
]


class MLTransformTest(unittest.TestCase):
  @unittest.skipIf(tft is None, 'tensorflow_transform is not installed.')
  def test_ml_transform(self):
    ml_opts = beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle', yaml_experimental_features=['ML'])
    with tempfile.TemporaryDirectory() as tempdir:
      with beam.Pipeline(options=ml_opts) as p:
        elements = p | beam.Create(TRAIN_DATA)
        result = elements | YamlTransform(
            f'''
            type: MLTransform
            config:
              write_artifact_location: {tempdir}
              transforms:
                - type: ScaleTo01
                  config:
                    columns: [num]
                - type: ComputeAndApplyVocabulary
                  config:
                    columns: [text]
                    split_string_by_delimiter: ' ,.'
            ''')
        assert_that(
            # Why is this an array, not a scalar?
            result | beam.Map(lambda x: x.num[0]),
            equal_to([0, .25, 1]))
        assert_that(
            result | beam.Map(lambda x: set(x.text))
            | beam.CombineGlobally(lambda xs: set.union(*xs)),
            equal_to([set(range(13))]),
            label='CheckVocab')

      with beam.Pipeline(options=ml_opts) as p:
        elements = p | beam.Create(TEST_DATA)
        result = elements | YamlTransform(
            f'''
            type: MLTransform
            config:
              read_artifact_location: {tempdir}
            ''')
        assert_that(result | beam.Map(lambda x: x.num[0]), equal_to([.75]))
        assert_that(
            result | beam.Map(lambda x: len(set(x.text))),
            equal_to([5]),
            label='CheckVocab')

  @unittest.skipIf(tft is None, 'tensorflow_transform is not installed.')
  def test_ml_transform_read_with_map_to_fields(self):
    ml_opts = beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle', yaml_experimental_features=['ML'])
    with tempfile.TemporaryDirectory() as tempdir:
      # First, write the artifacts.
      with beam.Pipeline(options=ml_opts) as p:
        elements = p | beam.Create(TRAIN_DATA)
        _ = elements | YamlTransform(
            f'''
            type: MLTransform
            config:
              write_artifact_location: {tempdir}
              transforms:
                - type: ScaleTo01
                  config:
                    columns: [num]
                - type: ComputeAndApplyVocabulary
                  config:
                    columns: [text]
                    split_string_by_delimiter: ' ,.'
            ''')

      # Now, read the artifacts and use MapToFields.
      with beam.Pipeline(options=ml_opts) as p:
        elements = p | beam.Create(TEST_DATA)
        result = elements | YamlTransform(
            f'''
            type: chain
            transforms:
              - type: MLTransform
                config:
                  read_artifact_location: {tempdir}
              - type: MapToFields
                config:
                  language: python
                  fields:
                    num_scaled: "num[0]"
                    text_vocab: text
            ''')

        def check_row(row):
          assert row.num_scaled == 0.75
          assert len(set(row.text_vocab)) == 5
          return row.num_scaled

        assert_that(result | beam.Map(check_row), equal_to([0.75]))

  @unittest.skipIf(
      sentence_transformers is None, 'sentence_transformers is not installed.')
  def test_sentence_transformer_embedding(self):
    SENTENCE_EMBEDDING_DIMENSION = 384
    DATA = [{
        'id': 1, 'log_message': "Error in module A"
    }, {
        'id': 2, 'log_message': "Warning in module B"
    }, {
        'id': 3, 'log_message': "Info in module C"
    }]
    ml_opts = beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle', yaml_experimental_features=['ML'])
    with tempfile.TemporaryDirectory() as tempdir:
      with beam.Pipeline(options=ml_opts) as p:
        elements = p | beam.Create(DATA)
        result = elements | YamlTransform(
            f'''
            type: MLTransform
            config:
              write_artifact_location: {tempdir}
              transforms:
                - type: SentenceTransformerEmbeddings
                  config:
                    model_name: all-MiniLM-L6-v2
                    columns: [log_message]
            ''')

        # Perform a basic check to ensure that embeddings are generated
        # and that the dimension of those embeddings is correct.
        actual_output = result | beam.Map(lambda x: len(x['log_message']))
        assert_that(
            actual_output, equal_to([SENTENCE_EMBEDDING_DIMENSION] * len(DATA)))

  @unittest.skipIf(
      sentence_transformers is None, 'sentence_transformers is not installed.')
  def test_sentence_transformer_embedding_with_beam_rows(self):
    SENTENCE_EMBEDDING_DIMENSION = 384
    DATA = [
        beam.Row(id=1, log_message="Error in module A"),
        beam.Row(id=2, log_message="Warning in module B"),
        beam.Row(id=3, log_message="Info in module C"),
    ]
    ml_opts = beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle', yaml_experimental_features=['ML'])
    with tempfile.TemporaryDirectory() as tempdir:
      with beam.Pipeline(options=ml_opts) as p:
        elements = p | beam.Create(DATA)
        result = elements | YamlTransform(
            f'''
            type: MLTransform
            config:
              write_artifact_location: {tempdir}
              transforms:
                - type: SentenceTransformerEmbeddings
                  config:
                    model_name: all-MiniLM-L6-v2
                    columns: [log_message]
            ''')

        # Perform a basic check to ensure that embeddings are generated
        # and that the dimension of those embeddings is correct.
        actual_output = result | beam.Map(lambda x: len(x.log_message))
        assert_that(
            actual_output, equal_to([SENTENCE_EMBEDDING_DIMENSION] * len(DATA)))

  @unittest.skipIf(
      sentence_transformers is None, 'sentence_transformers is not installed.')
  def test_ml_transform_outputs_schema(self):
    SENTENCE_EMBEDDING_DIMENSION = 384
    ml_opts = beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle', yaml_experimental_features=['ML'])
    with tempfile.TemporaryDirectory() as tempdir:
      with beam.Pipeline(options=ml_opts) as p:
        result = p | YamlTransform(
            f'''
            type: chain
            transforms:
              - type: Create
                config:
                  elements:
                    - {{id: 1, log_message: "Error in module A"}}
                    - {{id: 2, log_message: "Warning in module B"}}
                    - {{id: 3, log_message: "Info in module C"}}
              - type: MLTransform
                config:
                  write_artifact_location: {tempdir}
                  transforms:
                    - type: SentenceTransformerEmbeddings
                      config:
                        model_name: all-MiniLM-L6-v2
                        columns: [log_message]
              - type: MapToFields
                config:
                  language: python
                  fields:
                    id: id
                    embedding: log_message
            ''')

        def check_row(row):
          assert isinstance(row.id, int)
          assert isinstance(row.embedding, list)
          assert len(row.embedding) == SENTENCE_EMBEDDING_DIMENSION
          return row.id

        assert_that(result | beam.Map(check_row), equal_to([1, 2, 3]))

  def test_model_handler_provider(self):
    provider = yaml_ml.ModelHandlerProvider(
        "handler", preprocess={'callable': 'lambda x: x'})
    self.assertEqual(provider.underlying_handler(), "handler")
    self.assertEqual(provider.inference_output_type(), Any)
    self.assertEqual(
        provider._preprocess_fn_internal()(beam.Row(a=1)),
        (beam.Row(a=1), beam.Row(a=1)))
    self.assertEqual(provider._postprocess_fn_internal()(('orig', [1]))[1], [1])
    # Verify error handling, defaults, and config parsing for ModelHandlerProvider.
    with self.assertRaises(ValueError):
      provider.default_preprocess_fn()
    with self.assertRaises(NotImplementedError):
      provider.validate({})
    with self.assertRaises(ValueError):
      provider.parse_processing_transform({
          'callable': 'f', 'path': 'p', 'name': 'n'
      },
                                          'preprocess')
    with self.assertRaises(ValueError):
      provider.parse_processing_transform({'callable': None}, 'preprocess')
    with self.assertRaises(ValueError):
      provider.parse_processing_transform('not_dict', 'preprocess')
    with mock.patch('apache_beam.io.filesystems.FileSystems.open',
                    mock.mock_open(read_data=b'def fn(x):\n return x')):
      _ = provider.parse_processing_transform({
          'path': 'f.py', 'name': 'fn'
      },
                                              'preprocess')
    with self.assertRaises(ValueError):
      yaml_ml.ModelHandlerProvider.create_handler({
          'type': 'Nonexistent', 'config': {}
      })

    @yaml_ml.ModelHandlerProvider.register_handler_type('DummyMLHandler')
    def _create(**config):
      return mock.MagicMock()

    _ = yaml_ml.ModelHandlerProvider.create_handler({
        'type': 'DummyMLHandler', 'config': {}
    })

  def test_vertex_ai_provider(self):
    mock_vertex = mock.MagicMock()
    with mock.patch.dict(
        'sys.modules',
        {'apache_beam.ml.inference.vertex_ai_inference': mock_vertex}):
      mock_vertex.VertexAIModelHandlerJSON = mock.MagicMock()
      p = yaml_ml.VertexAIModelHandlerJSONProvider(
          123,
          project='p',
          location='l',
          preprocess={'callable': 'lambda x: x'})
      p.validate({})
      self.assertIsNotNone(p.inference_output_type())

  def test_huggingface_provider(self):
    mock_hf = mock.MagicMock()
    with mock.patch.dict(
        'sys.modules',
        {'apache_beam.ml.inference.huggingface_inference': mock_hf}):
      mock_hf.HuggingFacePipelineModelHandler = mock.MagicMock()
      p = yaml_ml.HuggingFacePipelineModelHandlerProvider(
          task='t',
          preprocess={'callable': 'lambda x: x'},
          inference_fn={'callable': 'lambda x: x'})
      p.validate({'task': 't'})
      with self.assertRaises(ValueError):
        p.validate({})
      self.assertEqual(p.inference_output_type(), Any)

  def test_run_inference_coverage(self):
    p = beam.Pipeline()
    pcoll = p | 'CreatePcoll' >> beam.Create([beam.Row(a=1)])
    with mock.patch('apache_beam.yaml.options.YamlOptions.check_enabled'):
      with self.assertRaises(ValueError):
        yaml_ml.run_inference("not_dict").expand(pcoll)
      with self.assertRaises(ValueError):
        yaml_ml.run_inference({
            'type': 't', 'config': {}, 'extra': 1
        }).expand(pcoll)
      with self.assertRaises(ValueError):
        yaml_ml.run_inference({'type': 't'}).expand(pcoll)
      with self.assertRaises(NotImplementedError):
        yaml_ml.run_inference({
            'type': 'UnknownType', 'config': {}
        }).expand(pcoll)

    mock_provider = mock.MagicMock()
    mock_provider.underlying_handler.return_value = mock.MagicMock()
    mock_provider.inference_output_type.return_value = Any
    mock_provider._preprocess_fn_internal.return_value = lambda x: x
    mock_provider._postprocess_fn_internal.return_value = lambda x: x
    with mock.patch.dict(yaml_ml.ModelHandlerProvider.handler_types,
                         {'DummyType': mock.MagicMock()}):
      with mock.patch.object(yaml_ml.ModelHandlerProvider,
                             'create_handler',
                             return_value=mock_provider):
        with mock.patch('apache_beam.yaml.yaml_ml.RunInference',
                        return_value=beam.Map(lambda x: (x, 'res'))):
          with beam.Pipeline(
              options=beam.options.pipeline_options.PipelineOptions(
                  yaml_experimental_features=['ML'])) as p:
            result = (p | 'CreateInput' >> beam.Create([beam.Row(a=1)])
                      ) | YamlTransform(
                          '''
                type: RunInference
                config:
                  model_handler:
                    type: DummyType
                    config: {}
                ''')
            assert_that(result, equal_to([beam.Row(a=1, inference='res')]))

  @unittest.skipIf(
      sentence_transformers is None, 'sentence_transformers is not installed.')
  def test_ml_transform_and_config(self):
    with self.assertRaises(ValueError):
      yaml_ml._config_to_obj({})
    with self.assertRaises(ValueError):
      yaml_ml._config_to_obj({'type': 't'})
    with self.assertRaises(ValueError):
      yaml_ml._config_to_obj({'type': 'unknown', 'config': {}})

    pcoll = beam.Pipeline() | 'CreateML' >> beam.Create([beam.Row(a=1)])
    with mock.patch('apache_beam.yaml.yaml_ml.MLTransform', None):
      with self.assertRaises(ValueError):
        yaml_ml.ml_transform().expand(pcoll)

    mock_mlt = mock.MagicMock(return_value=beam.Map(lambda x: x))
    with mock.patch('apache_beam.yaml.yaml_ml.MLTransform', mock_mlt):
      with mock.patch('apache_beam.yaml.yaml_ml._config_to_obj',
                      return_value=mock.MagicMock()):
        with beam.Pipeline(
            options=beam.options.pipeline_options.PipelineOptions(
                yaml_experimental_features=['ML'])) as p:
          result = (p | 'CreateMLInput' >> beam.Create([beam.Row(a=1)])
                    ) | YamlTransform(
                        '''
              type: MLTransform
              config:
                transforms:
                  - type: SentenceEmbeddings
                    config:
                      columns: [a]
              ''')
          assert_that(result, equal_to([beam.Row(a=1)]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
