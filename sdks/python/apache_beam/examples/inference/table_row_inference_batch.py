#!/usr/bin/env python
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

"""Batch inference pipeline for table rows using RunInference.

This is a simplified batch-only implementation of ML Pipelines #18.
It reads table data from files, runs ML inference, and writes results.

Key Features:
- BATCH PROCESSING ONLY (no streaming complexity)
- Reads from files (JSONL, CSV, or custom)
- Preserves table schema
- Writes to BigQuery or files
- Simple and easy to understand

Example usage:

  # Basic usage with local files
  python table_row_inference_batch.py \
    --input_file=data.jsonl \
    --output_table=project:dataset.table \
    --model_path=model.pkl \
    --feature_columns=feature1,feature2,feature3

  # With Dataflow
  python table_row_inference_batch.py \
    --input_file=gs://bucket/data.jsonl \
    --output_table=project:dataset.table \
    --model_path=gs://bucket/model.pkl \
    --feature_columns=feature1,feature2,feature3 \
    --runner=DataflowRunner \
    --project=PROJECT \
    --region=us-central1 \
    --temp_location=gs://bucket/temp

  # Output to file instead of BigQuery
  python table_row_inference_batch.py \
    --input_file=data.jsonl \
    --output_file=predictions.jsonl \
    --model_path=model.pkl \
    --feature_columns=feature1,feature2,feature3
"""

import argparse
import json
import logging
from collections.abc import Iterable
from typing import Any
from typing import Optional

import apache_beam as beam
import numpy as np
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class BatchTableRowModelHandler(SklearnModelHandlerNumpy):
  """ModelHandler for batch processing of table rows.

  This handler is optimized for batch inference on structured table data.
  It extracts specified feature columns and runs inference in batches.
  """
  def __init__(self, model_uri: str, feature_columns: list[str]):
    """Initialize the batch model handler.

    Args:
      model_uri: Path to the saved model (local or GCS)
      feature_columns: List of column names to use as features
    """
    super().__init__(model_uri=model_uri)
    self.feature_columns = feature_columns
    logging.info(
        f'Initialized BatchTableRowModelHandler with features: {feature_columns}'
    )

  def run_inference(
      self,
      batch: list[beam.Row],
      model: Any,
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Run batch inference on table rows.

    Args:
      batch: List of beam.Row objects with table data
      model: Loaded scikit-learn model
      inference_args: Optional inference arguments (unused)

    Yields:
      PredictionResult for each input row
    """
    features_list = []
    for row in batch:
      row_dict = row._asdict()
      features = [row_dict.get(col, 0.0) for col in self.feature_columns]
      features_list.append(features)

    features_array = np.array(features_list, dtype=np.float32)

    predictions = model.predict(features_array)

    for row, prediction in zip(batch, predictions):
      yield PredictionResult(
          example=row, inference=float(prediction), model_id=self._model_uri)


class FormatBatchOutput(beam.DoFn):
  """Format inference results for batch output."""
  def __init__(self, feature_columns: list[str], include_metadata: bool = True):
    """Initialize formatter.

    Args:
      feature_columns: List of feature column names to include
      include_metadata: Whether to include model_id in output
    """
    self.feature_columns = feature_columns
    self.include_metadata = include_metadata

  def process(
      self, element: tuple[str, PredictionResult]) -> Iterable[dict[str, Any]]:
    """Format a keyed inference result.

    Args:
      element: Tuple of (row_key, PredictionResult)

    Yields:
      Dictionary with formatted output
    """
    key, prediction = element
    row = prediction.example
    row_dict = row._asdict()

    output = {'id': key, 'prediction': prediction.inference}

    if self.include_metadata and prediction.model_id:
      output['model_id'] = prediction.model_id

    for field_name in self.feature_columns:
      output[field_name] = row_dict[field_name]

    yield output


def parse_jsonl_line(line: str, schema_fields: list[str]) -> tuple[str, beam.Row]:
  """Parse a JSONL line to (key, beam.Row) format.

  Args:
    line: JSON string
    schema_fields: Expected field names

  Returns:
    Tuple of (row_id, beam.Row)
  """
  data = json.loads(line)

  row_id = data.get('id', str(hash(line)))

  row_fields = {}
  for field in schema_fields:
    if field in data:
      value = data[field]
      row_fields[field] = float(value) if isinstance(value, (int, float)) else value

  return row_id, beam.Row(**row_fields)


def build_bigquery_schema(feature_columns: list[str]) -> str:
  """Build BigQuery schema for output table.

  Args:
    feature_columns: List of feature column names

  Returns:
    BigQuery schema string
  """
  schema_fields = ['id:STRING', 'prediction:FLOAT', 'model_id:STRING']

  for col in feature_columns:
    schema_fields.append(f'{col}:FLOAT')

  return ','.join(schema_fields)


def run_batch_inference(
    input_file: str,
    model_path: str,
    feature_columns: list[str],
    output_table: Optional[str] = None,
    output_file: Optional[str] = None,
    pipeline_options: Optional[PipelineOptions] = None) -> beam.Pipeline:
  """Run batch inference pipeline.

  Args:
    input_file: Path to input file (JSONL format)
    model_path: Path to saved model
    feature_columns: List of feature column names
    output_table: Optional BigQuery table (PROJECT:DATASET.TABLE)
    output_file: Optional output file path
    pipeline_options: Beam pipeline options

  Returns:
    Executed pipeline
  """
  if not output_table and not output_file:
    raise ValueError('Must specify either output_table or output_file')

  pipeline_options = pipeline_options or PipelineOptions()

  model_handler = BatchTableRowModelHandler(
      model_uri=model_path, feature_columns=feature_columns)

  logging.info(f'Starting batch inference pipeline')
  logging.info(f'  Input: {input_file}')
  logging.info(f'  Model: {model_path}')
  logging.info(f'  Features: {feature_columns}')
  logging.info(
      f'  Output: {output_table if output_table else output_file}')

  with beam.Pipeline(options=pipeline_options) as pipeline:

    input_data = (
        pipeline
        | 'ReadInputFile' >> beam.io.ReadFromText(input_file)
        | 'ParseToRows' >> beam.Map(
            lambda line: parse_jsonl_line(line, feature_columns)))

    predictions = (
        input_data
        | 'RunInference' >> RunInference(KeyedModelHandler(model_handler))
        | 'FormatOutput' >> beam.ParDo(FormatBatchOutput(feature_columns)))

    if output_table:
      schema = build_bigquery_schema(feature_columns)
      _ = (
          predictions
          | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
              output_table,
              schema=schema,
              write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
              method=beam.io.WriteToBigQuery.Method.FILE_LOADS))

    if output_file:
      _ = (
          predictions
          | 'FormatJSON' >> beam.Map(json.dumps)
          | 'WriteToFile' >> beam.io.WriteToText(
              output_file, file_name_suffix='.jsonl', shard_name_template=''))

  logging.info('Batch inference pipeline completed successfully')
  return pipeline


def main(argv=None):
  """Main entry point for the batch inference pipeline."""
  parser = argparse.ArgumentParser(
      description='Batch inference on table rows using RunInference',
      formatter_class=argparse.RawDescriptionHelpFormatter,
      epilog=__doc__)

  parser.add_argument(
      '--input_file',
      required=True,
      help='Input file path (JSONL format). Can be local or GCS path.')

  parser.add_argument(
      '--model_path',
      required=True,
      help='Path to saved model file. Can be local or GCS path.')

  parser.add_argument(
      '--feature_columns',
      required=True,
      help='Comma-separated list of feature column names to extract from input rows.'
  )

  parser.add_argument(
      '--output_table',
      help='BigQuery output table in format PROJECT:DATASET.TABLE')

  parser.add_argument(
      '--output_file',
      help='Output file path (JSONL format). Alternative to output_table.')

  known_args, pipeline_args = parser.parse_known_args(argv)

  if not known_args.output_table and not known_args.output_file:
    parser.error('Must specify either --output_table or --output_file')

  feature_columns = [col.strip() for col in known_args.feature_columns.split(',')]

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  run_batch_inference(
      input_file=known_args.input_file,
      model_path=known_args.model_path,
      feature_columns=feature_columns,
      output_table=known_args.output_table,
      output_file=known_args.output_file,
      pipeline_options=pipeline_options)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
