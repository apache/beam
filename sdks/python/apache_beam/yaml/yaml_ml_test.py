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

import apache_beam as beam
from apache_beam.testing import test_utils
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.yaml.yaml_transform import YamlTransform

try:
  # pylint: disable=wrong-import-order, wrong-import-position, unused-import
  from apache_beam.ml.transforms import tft
except ImportError:
  tft = None

TRAIN_DATA = [
    beam.Row(num=0, text='And God said, Let there be light,'),
    beam.Row(num=2, text='And there was light'),
    beam.Row(num=8, text='And God saw the light, that it was good'),
]

TEST_DATA = [
    beam.Row(num=6, text='And God divided the light from the darkness.'),
]


@unittest.skipIf(tft is None, 'tensorflow_transform is not installed.')
class MLTransformTest(unittest.TestCase):
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


class MLTransformJobSubmissionTest(unittest.TestCase):
  def test_sentence_transformer_embedding_does_not_use_sentence_transformers(
      self):
    # sentence-transformers should only be needed when executing the pipeline
    # (for example, on the workers), not for constructing and submitting it.
    ml_opts = beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle', yaml_experimental_features=['ML'])
    with tempfile.TemporaryDirectory() as tempdir:
      with test_utils.block_imports(
          'sentence_transformers') as attempted_imports:
        pipeline = beam.Pipeline(options=ml_opts)
        _ = pipeline | YamlTransform(
            f'''
            type: chain
            transforms:
              - type: Create
                config:
                  elements:
                    - {{id: 1, log_message: "Error in module A"}}
              - type: MLTransform
                config:
                  write_artifact_location: {tempdir}
                  transforms:
                    - type: SentenceTransformerEmbeddings
                      config:
                        model_name: all-MiniLM-L6-v2
                        columns: [log_message]
            ''')
        # Serializes the pipeline, as done during job submission, and
        # deserializes it again. Deserializing fails if the pipeline references
        # (i.e. requires) a module that can't be imported.
        beam.Pipeline.from_runner_api(
            pipeline.to_runner_api(), pipeline.runner, pipeline.options)
    self.assertEqual(attempted_imports, [])


class RunInferenceJobSubmissionTest(unittest.TestCase):
  def test_huggingface_pipeline_model_handler_does_not_use_ml_frameworks(self):
    # tensorflow, torch and transformers should only be needed when executing
    # the pipeline (for example, on the workers), not for constructing and
    # submitting it.
    # Imports the YAML ML module before blocking imports, since importing it
    # loads all available MLTransform transforms, and some of these (e.g. the
    # TFT based ones) do require tensorflow.
    from apache_beam.yaml import yaml_ml  # pylint: disable=unused-import
    ml_opts = beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle', yaml_experimental_features=['ML'])
    with test_utils.block_imports('tensorflow', 'torch',
                                  'transformers') as attempted_imports:
      pipeline = beam.Pipeline(options=ml_opts)
      _ = pipeline | YamlTransform(
          '''
          type: chain
          transforms:
            - type: Create
              config:
                elements:
                  - {text: "I love Apache Beam!"}
            - type: RunInference
              config:
                model_handler:
                  type: HuggingFacePipelineModelHandler
                  config:
                    task: text-classification
                    preprocess:
                      callable: 'lambda row: row.text'
          ''')
      # Serializes the pipeline, as done during job submission, and
      # deserializes it again. Deserializing fails if the pipeline references
      # (i.e. requires) a module that can't be imported.
      beam.Pipeline.from_runner_api(
          pipeline.to_runner_api(), pipeline.runner, pipeline.options)
    self.assertEqual(attempted_imports, [])

  def test_declarative_providers_lazy_offline_graph_construction(self):
    from apache_beam.yaml import yaml_ml  # pylint: disable=unused-import
    ml_opts = beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle', yaml_experimental_features=['ML'])
    with test_utils.block_imports(
        'tensorflow',
        'torch',
        'transformers',
        'sklearn',
        'google.cloud.aiplatform') as attempted_imports:
      pipeline = beam.Pipeline(options=ml_opts)
      _ = pipeline | YamlTransform(
          '''
          type: chain
          transforms:
            - type: Create
              config:
                elements:
                  - {comment_text: "I love Apache Beam!", feat: 1.5}
            - type: RunInference
              name: HFInference
              config:
                provider: HuggingFacePipeline
                model_config:
                  task: text-classification
                  model: distilbert/distilbert-base-uncased-finetuned-sst-2-english
                  large_model: true
                input_field: comment_text
                output_field: sentiment
                flatten_predictions: true
                min_batch_size: 8
                max_batch_size: 64
            - type: RunInference
              name: VertexInference
              config:
                provider: VertexAI
                model_config:
                  endpoint_id: "1234567890"
                  project: my-gcp-project
                  location: us-central1
                input_field: comment_text
                output_field: vertex_pred
            - type: RunInference
              name: SklearnInference
              config:
                provider: Sklearn
                model_config:
                  model_uri: gs://my-bucket/model.pkl
                input_field: feat
                output_field: sk_pred
          ''')
      beam.Pipeline.from_runner_api(
          pipeline.to_runner_api(), pipeline.runner, pipeline.options)
    self.assertEqual(attempted_imports, [])

  def test_declarative_run_inference_flatten_predictions_and_schema(self):
    from apache_beam.typehints.row_type import RowTypeConstraint
    ml_opts = beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle', yaml_experimental_features=['ML'])
    inputs = [
        beam.Row(id=1, comment_text='Apache Beam is awesome'),
        beam.Row(id=2, comment_text='Bad experience'),
    ]
    with beam.Pipeline(options=ml_opts) as p:
      input_pcoll = p | beam.Create(inputs)
      res = input_pcoll | YamlTransform(
          '''
          type: RunInference
          config:
            provider: Custom
            model_config:
              inference_fn: |
                def infer(batch, model, args=None):
                  return [
                    {"label": "POSITIVE" if "awesome" in x else "NEGATIVE",
                     "score": 0.95 if "awesome" in x else 0.15}
                    for x in batch
                  ]
            input_field: comment_text
            output_field: sentiment
            flatten_predictions: true
          ''')
      self.assertEqual(
          res.element_type,
          RowTypeConstraint.from_fields([
              ('id', int),
              ('comment_text', str),
              ('sentiment_label', str),
              ('sentiment_score', float),
          ]),
      )
      assert_that(
          res,
          equal_to([
              beam.Row(
                  id=1,
                  comment_text='Apache Beam is awesome',
                  sentiment_label='POSITIVE',
                  sentiment_score=0.95),
              beam.Row(
                  id=2,
                  comment_text='Bad experience',
                  sentiment_label='NEGATIVE',
                  sentiment_score=0.15),
          ]),
      )

  def test_run_inference_error_handling_dlq_and_poison_pill_isolation(self):
    ml_opts = beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle', yaml_experimental_features=['ML'])
    inputs = [
        beam.Row(id=1, comment_text='valid one'),
        beam.Row(id=2, comment_text='POISON_PILL'),
        beam.Row(id=3, comment_text='valid two'),
        beam.Row(id=4, comment_text=None),
    ]
    with beam.Pipeline(options=ml_opts) as p:
      input_pcoll = p | beam.Create(inputs)
      result = input_pcoll | YamlTransform(
          '''
          type: composite
          input:
            events: input
          transforms:
            - type: RunInference
              name: ScoreComments
              input: events
              config:
                provider: Custom
                model_config:
                  inference_fn: |
                    def infer(batch, model, args=None):
                      if any("POISON_PILL" in x for x in batch):
                        raise ValueError("Poison pill in batch")
                      return [{"label": "OK", "score": float(len(x))} for x in batch]
                input_field: comment_text
                output_field: sentiment
                flatten_predictions: true
                min_batch_size: 3
                max_batch_size: 4
                error_handling:
                  output: failed_inferences
          output:
            good: ScoreComments
            dlq: ScoreComments.failed_inferences
          ''')
      assert_that(
          result['good'],
          equal_to([
              beam.Row(
                  id=1,
                  comment_text='valid one',
                  sentiment_label='OK',
                  sentiment_score=9.0),
              beam.Row(
                  id=3,
                  comment_text='valid two',
                  sentiment_label='OK',
                  sentiment_score=9.0),
          ]),
          label='CheckGood',
      )
      assert_that(
          result['dlq'] | beam.Map(lambda r: r.element.id),
          equal_to([2, 4]),
          label='CheckDLQ',
      )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
