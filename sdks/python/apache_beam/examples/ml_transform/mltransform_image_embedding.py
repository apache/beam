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

"""Batch image embedding pipeline using MLTransform.

The pipeline reads image URIs from a text file, decodes images with Pillow,
generates SentenceTransformers image embeddings through MLTransform, and writes
results to BigQuery using batch file loads.
"""

import argparse
import hashlib
import io
import logging
import time
from collections.abc import Iterable
from typing import Any

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.ml.transforms.embeddings.huggingface import SentenceTransformerEmbeddings
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.runner import PipelineResult
from PIL import Image

IMAGE_COLUMN = 'image'
IMAGE_ID_COLUMN = 'image_id'
IMAGE_URI_COLUMN = 'image_uri'

DEFAULT_IMAGE_MODEL_NAME = 'clip-ViT-B-32'
DEFAULT_ACCELERATOR = 'type:nvidia-tesla-t4;count:1;install-nvidia-driver'
DEFAULT_EMBEDDING_MIN_RAM = '16GB'

OUTPUT_TABLE_SCHEMA = {
    'fields': [
        {
            'name': 'image_id', 'type': 'STRING'
        },
        {
            'name': 'image_uri', 'type': 'STRING'
        },
        {
            'name': 'model_name', 'type': 'STRING'
        },
        {
            'name': 'embedding', 'type': 'FLOAT64', 'mode': 'REPEATED'
        },
        {
            'name': 'embedding_dim', 'type': 'INT64'
        },
        {
            'name': 'infer_ms', 'type': 'INT64'
        },
    ]
}


def now_millis() -> int:
  return int(time.time() * 1000)


def sha1_hex(value: str) -> str:
  return hashlib.sha1(value.encode('utf-8')).hexdigest()


def filter_empty_uri(uri: str) -> Iterable[str]:
  uri = uri.strip()
  if uri:
    yield uri


def load_image_from_uri(uri: str) -> bytes:
  with FileSystems.open(uri) as file:
    return file.read()


def decode_pil(image_bytes: bytes) -> Image.Image:
  with Image.open(io.BytesIO(image_bytes)) as image:
    image = image.convert('RGB')
    image.load()
    return image


class ReadImage(beam.DoFn):
  def process(self, uri: str) -> Iterable[dict[str, Any]]:
    image_id = sha1_hex(uri)
    try:
      yield {
          IMAGE_ID_COLUMN: image_id,
          IMAGE_URI_COLUMN: uri,
          IMAGE_COLUMN: decode_pil(load_image_from_uri(uri)),
      }
    except Exception as exc:
      logging.warning(
          'Failed to read or decode image %s (%s): %s', image_id, uri, exc)


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


class FormatImageEmbeddingOutput(beam.DoFn):
  def __init__(self, model_name: str):
    self.model_name = model_name

  def process(self, row: Any) -> Iterable[dict[str, Any]]:
    row = _as_dict(row)
    embedding = embedding_to_list(row[IMAGE_COLUMN])
    yield {
        IMAGE_ID_COLUMN: row[IMAGE_ID_COLUMN],
        IMAGE_URI_COLUMN: row[IMAGE_URI_COLUMN],
        'model_name': self.model_name,
        'embedding': embedding,
        'embedding_dim': len(embedding),
        'infer_ms': now_millis(),
    }


def _str_to_bool(value: str) -> bool:
  if value.lower() == 'true':
    return True
  if value.lower() == 'false':
    return False
  raise argparse.ArgumentTypeError(
      f'"true" or "false" expected, got "{value}" instead.')


def parse_known_args(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument('--mode', default='batch', choices=['batch'])
  parser.add_argument(
      '--input', required=True, help='Path to a text file with image URIs.')
  parser.add_argument(
      '--output_table', required=True, help='BigQuery table for embeddings.')
  parser.add_argument(
      '--publish_to_big_query',
      type=_str_to_bool,
      default=True,
      help='Whether to write embedding rows to BigQuery.')
  parser.add_argument(
      '--artifact_location',
      required=True,
      help='Path where MLTransform artifacts are written.')
  parser.add_argument(
      '--pretrained_model_name',
      default=DEFAULT_IMAGE_MODEL_NAME,
      help='SentenceTransformers image model name.')
  parser.add_argument(
      '--device',
      default='CPU',
      choices=['CPU', 'GPU'],
      help='Device used by SentenceTransformers on the worker.')
  parser.add_argument(
      '--min_batch_size',
      type=int,
      default=8,
      help='Minimum Beam inference batch size.')
  parser.add_argument(
      '--max_batch_size',
      type=int,
      default=64,
      help='Maximum Beam inference batch size.')
  parser.add_argument(
      '--embedding_accelerator',
      default=DEFAULT_ACCELERATOR,
      help='GPU accelerator resource hint for the MLTransform embedding step.')
  parser.add_argument(
      '--embedding_min_ram',
      default=DEFAULT_EMBEDDING_MIN_RAM,
      help='CPU right-fitting min RAM resource hint for the embedding step.')
  return parser.parse_known_args(argv)


def run(
    argv=None, save_main_session=True, test_pipeline=None) -> PipelineResult:
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(StandardOptions).streaming = False

  device = 'cuda' if known_args.device == 'GPU' else 'cpu'
  embedding_transform = SentenceTransformerEmbeddings(
      model_name=known_args.pretrained_model_name,
      columns=[IMAGE_COLUMN],
      image_model=True,
      min_batch_size=known_args.min_batch_size,
      max_batch_size=known_args.max_batch_size,
      load_model_args={'device': device},
      inference_args={
          'convert_to_numpy': True,
          'show_progress_bar': False,
      })

  ml_transform = MLTransform(
      write_artifact_location=known_args.artifact_location).with_transform(
          embedding_transform)
  if known_args.device == 'GPU' and known_args.embedding_accelerator:
    ml_transform = ml_transform.with_resource_hints(
        accelerator=known_args.embedding_accelerator)
  elif known_args.embedding_min_ram:
    ml_transform = ml_transform.with_resource_hints(
        min_ram=known_args.embedding_min_ram)

  pipeline = test_pipeline or beam.Pipeline(options=pipeline_options)
  rows = (
      pipeline
      | 'ReadImageURIs' >> beam.io.ReadFromText(known_args.input)
      | 'FilterEmptyURIs' >> beam.FlatMap(filter_empty_uri)
      | 'ReshuffleBeforeEmbedding' >> beam.Reshuffle()
      | 'ReadImages' >> beam.ParDo(ReadImage()))
  results = (
      rows
      | 'MLTransformImageEmbeddings' >> ml_transform
      | 'FormatOutput' >> beam.ParDo(
          FormatImageEmbeddingOutput(
              model_name=known_args.pretrained_model_name)))

  if known_args.publish_to_big_query:
    _ = (
        results
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            known_args.output_table,
            schema=OUTPUT_TABLE_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method=beam.io.WriteToBigQuery.Method.FILE_LOADS))

  result = pipeline.run()
  if not test_pipeline:
    result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
