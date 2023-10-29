# pylint: skip-file

if __name__ == '__main__':
  import apache_beam as beam
  from apache_beam.ml.inference import RunInference
  from apache_beam.ml.transforms.base import TextEmbeddingHandler
  from apache_beam.ml.transforms.embeddings.sentence_transformer import SentenceTransformerEmbeddings

  embedding_config = SentenceTransformerEmbeddings(
      model_uri='sentence-transformers/all-MiniLM-L6-v2')

  # this will be internal to the MLTransform
  # Namee this to SentenceEmbeddingHandler.
  embedding_handler = TextEmbeddingHandler(embedding_config=embedding_config)

  with beam.Pipeline() as p:
    text_data = (
        p
        | "CreateTextData" >> beam.Create([{
            "text": "I love Apache Beam"
        }, {
            "text": "I love Python"
        }]))
    embeddings = (
        text_data
        | "RunInference" >> RunInference(model_handler=embedding_handler))
    embeddings | beam.Map(print)
