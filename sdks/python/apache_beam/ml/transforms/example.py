# pylint: skip-file

import argparse
import logging

import apache_beam as beam
from apache_beam.ml.transforms.base import MLTransform
# from apache_beam.ml.transforms.tft import ScaleTo01, ScaleToZScore
from apache_beam.ml.transforms.embeddings.vertex_ai import VertexAITextEmbeddings
# from apache_beam.ml.transforms.embeddings.tensorflow_hub import TensorflowHubTextEmbeddings
# from apache_beam.ml.transforms.embeddings.sentence_transformer import SentenceTransformerEmbeddings
from apache_beam.options.pipeline_options import PipelineOptions

# def run_tensorflow_hub_embedding(pipeline_args):
#   # model_uri = 'https://tfhub.dev/google/LEALLA/LEALLA-small/1'
#   # model_uri =  'https://tfhub.dev/google/sentence-t5/st5-base/1'
#   preprocessor_uri = "https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3"
#   model_uri = "https://tfhub.dev/tensorflow/bert_en_uncased_L-12_H-768_A-12/4"
#   embedding_config = TensorflowHubTextEmbeddings(
#       columns=['text'],
#       hub_url=model_uri,
#       preprocessing_url=preprocessor_uri,
#   )
#   options = PipelineOptions(pipeline_args)
#   with beam.Pipeline(options=options) as p:
#     data = (
#         p | "CreateData" >> beam.Create([{
#             "text": "This is a test"
#         }, {
#             "text": "This is another test"
#         }, {
#             "text": "This is a third test"
#         }, {
#             "text": "This is a fourth test"
#         }, {
#             'text': "Hello word",
#         }]))
#     (
#         data
#         | "MLTransform" >>
#         MLTransform(write_artifact_location=artifact_location).with_transform(
#             embedding_config
#             #  ).with_transform(ScaleTo01(columns=['text'])
#             # ).with_transform(ScaleToZScore(columns=['text'])
#         )
#         | beam.Map(logging.info))


def run_vertex_ai_pipeline(pipeline_args):
  model_name: str = "textembedding-gecko@002"
  # model_name = 'paraphrase-multilingual-mpnet-base-v2'
  test_query_column = "text"
  model_name: str = "textembedding-gecko@002"
  embedding_config = VertexAITextEmbeddings(
      model_name=model_name, columns=[test_query_column])
  options = PipelineOptions(pipeline_args)

  with beam.Pipeline(options=options) as p:
    data = (
        p | "CreateData" >> beam.Create([{
            "text": "This is a test"
            # },
            # {
            #     "text": "This is another test"
            # }, {
            #     "text": "This is a third test"
            # }, {
            #     "text": "This is a fourth test"
            # }, {
            #     'text': "Hello word",
        }] * 100))
    (
        data
        |
        "MLTransform" >> MLTransform(write_artifact_location=artifact_location
                                     ).with_transform(embedding_config)
        # | MLTransform(read_artifact_location=artifact_location)
        # | beam.Map(lambda x: len(x))
        | beam.Map(logging.info))


# def run_sentence_transformer_embedding(pipeline_args):
#     # model_name: str = "all-MiniLM-L6-v2"
#     # model_name = 'paraphrase-multilingual-mpnet-base-v2'
#     model_name = 'neuml/pubmedbert-base-embeddings'
#     embedding_config = SentenceTransformerEmbeddings(
#             model_name=model_name,
#             columns=['text'])
#     options = PipelineOptions(pipeline_args)
#     with beam.Pipeline(options=options) as p:
#         data = (
#             p | "CreateData" >> beam.Create([{
#                 "text": "This is a test"
#             }]))
#         (
#             data
#             | "MLTransform" >> MLTransform(write_artifact_location=artifact_location).
#             with_transform(embedding_config
#                         #    ).with_transform(
#                 # ScaleTo01(columns=['text'])).with_transform(
#                     # ScaleToZScore(columns=['text'])
#                     )
#             | beam.Map(logging.info))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  # artifact_location = 'gs://anandinguva-test/artifacts'
  import tempfile
  artifact_location = '/Users/anandinguva/Desktop/artifacts'
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args()
  run_vertex_ai_pipeline(pipeline_args)
