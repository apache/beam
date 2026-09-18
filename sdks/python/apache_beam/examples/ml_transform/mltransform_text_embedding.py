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

"""Batch text embedding pipeline using MLTransform.

The pipeline reads text lines, generates sentence-transformer embeddings through
MLTransform, and writes JSONL output to a sharded text sink.
"""

import argparse
import hashlib
import json
import logging
from collections.abc import Iterable
from typing import Any

import apache_beam as beam
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.ml.transforms.embeddings.huggingface import SentenceTransformerEmbeddings
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult

ID_COLUMN = 'id'
RAW_TEXT_COLUMN = 'raw_text'
TEXT_COLUMN = 'text'

DEFAULT_MODEL_NAME = 'sentence-transformers/all-MiniLM-L6-v2'


def _str_to_bool(value: str) -> bool:
  if value.lower() == 'true':
    return True
  if value.lower() == 'false':
    return False
  raise argparse.ArgumentTypeError(
      f'"true" or "false" expected, got "{value}" instead.')


def parse_known_args(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input', required=True, help='Path to the input text file.')
  parser.add_argument(
      '--output',
      required=True,
      help='Output prefix for sharded JSONL embedding results.')
  parser.add_argument(
      '--artifact_location',
      required=True,
      help='Path where MLTransform artifacts are written.')
  parser.add_argument(
      '--model_name',
      default=DEFAULT_MODEL_NAME,
      help='SentenceTransformers model name.')
  parser.add_argument(
      '--min_batch_size',
      type=int,
      default=16,
      help='Minimum Beam inference batch size.')
  parser.add_argument(
      '--max_batch_size',
      type=int,
      default=128,
      help='Maximum Beam inference batch size.')
  parser.add_argument(
      '--model_batch_size',
      type=int,
      default=32,
      help='Batch size passed to SentenceTransformer.encode.')
  parser.add_argument(
      '--device',
      default='CPU',
      choices=['CPU', 'GPU'],
      help='Device used by SentenceTransformers on the worker.')
  parser.add_argument(
      '--large_model',
      type=_str_to_bool,
      default=False,
      help='Whether RunInference should share the model across processes.')
  return parser.parse_known_args(argv)


def text_to_record(line: str) -> Iterable[dict[str, str]]:
  text = line.strip()
  if not text:
    return

  yield {
      ID_COLUMN: hashlib.sha256(text.encode('utf-8')).hexdigest(),
      RAW_TEXT_COLUMN: text,
      TEXT_COLUMN: text,
  }


def _as_dict(row: Any) -> dict[str, Any]:
  if hasattr(row, 'as_dict'):
    return row.as_dict()
  if hasattr(row, '_asdict'):
    return row._asdict()
  return dict(row)


def embedding_to_list(value: Any) -> list[float]:
  if hasattr(value, 'tolist'):
    return value.tolist()
  return [float(item) for item in value]


class FormatEmbeddingOutput(beam.DoFn):
  def __init__(self, model_name: str):
    self.model_name = model_name

  def process(self, row: Any) -> Iterable[str]:
    row = _as_dict(row)
    embedding = embedding_to_list(row[TEXT_COLUMN])
    yield json.dumps({
        ID_COLUMN: row[ID_COLUMN],
        'model_name': self.model_name,
        RAW_TEXT_COLUMN: row[RAW_TEXT_COLUMN],
        'embedding': embedding,
        'embedding_dim': len(embedding) if isinstance(embedding, list) else 0,
    },
                     sort_keys=True)


def run(
    argv=None, save_main_session=True, test_pipeline=None) -> PipelineResult:
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  device = 'cuda' if known_args.device == 'GPU' else 'cpu'
  embedding_transform = SentenceTransformerEmbeddings(
      model_name=known_args.model_name,
      columns=[TEXT_COLUMN],
      min_batch_size=known_args.min_batch_size,
      max_batch_size=known_args.max_batch_size,
      large_model=known_args.large_model,
      load_model_args={'device': device},
      inference_args={
          'batch_size': known_args.model_batch_size,
          'convert_to_numpy': True,
          'show_progress_bar': False,
      })

  pipeline = test_pipeline or beam.Pipeline(options=pipeline_options)
  records = (
      pipeline
      | 'ReadTextLines' >> beam.io.ReadFromText(known_args.input)
      | 'ToEmbeddingRecords' >> beam.FlatMap(text_to_record))
  embedded = (
      records
      | 'MLTransformTextEmbeddings' >> MLTransform(
          write_artifact_location=known_args.artifact_location).with_transform(
              embedding_transform))
  _ = (
      embedded
      | 'FormatOutput' >> beam.ParDo(
          FormatEmbeddingOutput(model_name=known_args.model_name))
      | 'WriteOutput' >> beam.io.WriteToText(
          known_args.output, file_name_suffix='.jsonl'))

  result = pipeline.run()
  if not test_pipeline:
    result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
