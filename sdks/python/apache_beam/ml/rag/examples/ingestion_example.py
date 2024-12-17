import apache_beam as beam

import tempfile

from typing import Any, Dict

from langchain.text_splitter import RecursiveCharacterTextSplitter
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.ml.rag.chunking.langchain import LangChainChunkingProvider
from apache_beam.ml.rag.embeddings.huggingface import HuggingfaceTextEmbeddings
from apache_beam.ml.rag.ingestion.base import VectorDatabaseWriteTransform
from apache_beam.ml.rag.ingestion.bigquery import BigQueryVectorWriterConfig
from transformers import AutoTokenizer
from apache_beam.options.pipeline_options import PipelineOptions

TEMP_LOCATION = "<TEMP_LOCATION>"
BIGQUERY_TABLE = "<BIGQUERY_TABLE>"

huggingface_embedder = HuggingfaceTextEmbeddings(
    model_name="sentence-transformers/all-MiniLM-L6-v2")
tokenizer = AutoTokenizer.from_pretrained(
    "sentence-transformers/all-MiniLM-L6-v2")
splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(
    tokenizer,
    chunk_size=512,
    chunk_overlap=52,
)


def run_pipeline():
  with beam.Pipeline(
      options=PipelineOptions(['--runner=DirectRunner',
                               f'--temp_location={TEMP_LOCATION}',
                               '--expansion_service_port=8888'])) as p:

    # Ingestion
    _ = (
        p
        | beam.Create([{
            'content': 'This is a simple test document. It has multiple sentences. '
            'We will use it to test basic splitting. ' * 20,
            'source': 'simple.txt',
            'language': 'en'
        },
                       {
                           'content': (
                               'The patient arrived at 2 p.m. yesterday. '
                               'Initial assessment was completed. '
                               'Lab results showed normal ranges. '
                               'Follow-up scheduled for next week.' * 10),
                           'source': 'medical.txt',
                           'language': 'en'
                       }])
        |
        MLTransform(write_artifact_location=tempfile.mkdtemp()).with_transform(
            LangChainChunkingProvider(
                text_splitter=splitter,
                document_field="content",
                metadata_fields=["source", "language"
                                 ])).with_transform(huggingface_embedder)
        | VectorDatabaseWriteTransform(
            BigQueryVectorWriterConfig(
                write_config={
                    "table": BIGQUERY_TABLE,
                    "create_disposition": "CREATE_IF_NEEDED",
                    "write_disposition": "WRITE_TRUNCATE",
                })))


if __name__ == '__main__':
  run_pipeline()
