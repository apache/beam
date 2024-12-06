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

import apache_beam as beam

import tempfile

from langchain.text_splitter import RecursiveCharacterTextSplitter
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.ml.rag.embeddings.huggingface import HuggingfaceTextEmbeddings
from transformers import AutoTokenizer
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.ml.rag.types import Chunk, Content
from apache_beam.ml.rag.enrichment.bigquery_vector_search import BigQueryVectorSearchEnrichmentHandler, BigQueryVectorSearchParameters
from apache_beam.transforms.enrichment import Enrichment

PROJECT = "<PROJECT>"
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
  with beam.Pipeline() as p:

    # Enrichment
    _ = (
        p
        | beam.Create([
            Chunk(
                id="simple_query",
                content=Content(text="This is a simple test document."),
                metadata={"language": "en"}),
            Chunk(
                id="medical_query",
                content=Content(text="When did the patient arrive?"),
                metadata={"language": "en"}),
        ])
        | MLTransform(write_artifact_location=tempfile.mkdtemp()).
        with_transform(huggingface_embedder)
        | Enrichment(
            BigQueryVectorSearchEnrichmentHandler(
                project=PROJECT,
                vector_search_parameters=BigQueryVectorSearchParameters(
                    table_name=BIGQUERY_TABLE,
                    embedding_column='embedding',
                    columns=['metadata', 'content'],
                    neighbor_count=3,
                    metadata_restriction_template=(
                        "check_metadata(metadata, 'language','{language}')"))))
        | beam.Map(print))


if __name__ == '__main__':
  run_pipeline()
