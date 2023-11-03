# pylint: skip-file
import apache_beam as beam
from apache_beam.ml.transforms.base import MLTransform

from apache_beam.ml.transforms.tft import ScaleTo01
from apache_beam.ml.transforms.embeddings.sentence_transformer import SentenceTransformerEmbeddings

artifact_location = '/tmp/my_artifacts_py'
model_name = 'paraphrase-multilingual-mpnet-base-v2'

with beam.Pipeline() as p:
  embedding_config = SentenceTransformerEmbeddings(model_name, columns=['text'])
  data = (
      p | "CreateData" >> beam.Create([
          # {"text": "This is a test"},
          # {"text": "This is another test"},
          # {"text": "This is a third test"},
          # {"text": "This is a fourth test"}
          {
              'text': "Hello word", 'label': '1'
          }
      ]))

  (
      data
      | "MLTransform" >> MLTransform(write_artifact_location=artifact_location).
      generate_embeddings(embedding_config=embedding_config).with_transform(
          ScaleTo01(columns=['text']))
      # | beam.Map(print)
      # data | "MLTransformInference" >> MLTransform(read_artifact_location=artifact_location)
      | beam.Map(print))
