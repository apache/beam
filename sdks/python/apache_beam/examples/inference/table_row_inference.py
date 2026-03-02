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

"""A pipeline that uses RunInference to perform inference on table rows.

This pipeline demonstrates ML Pipelines #18: handling continuous new table
rows with RunInference using table input models. It reads structured data
(table rows) from a streaming source, performs inference while preserving
the table schema, and writes results to a table output.

The pipeline supports both streaming and batch modes:
- Streaming: Reads from Pub/Sub, applies windowing, writes via streaming inserts
- Batch: Reads from file, processes all data, writes via file loads

Example usage for streaming:
  python table_row_inference.py \
    --mode=streaming \
    --input_subscription=projects/PROJECT/subscriptions/SUBSCRIPTION \
    --output_table=PROJECT:DATASET.TABLE \
    --model_path=gs://BUCKET/model.pkl \
    --feature_columns=feature1,feature2,feature3 \
    --runner=DataflowRunner \
    --project=PROJECT \
    --region=REGION \
    --temp_location=gs://BUCKET/temp

Example usage for batch:
  python table_row_inference.py \
    --mode=batch \
    --input_file=gs://BUCKET/input.jsonl \
    --output_table=PROJECT:DATASET.TABLE \
    --model_path=gs://BUCKET/model.pkl \
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
from apache_beam.ml.inference.sklearn_inference import ModelFileType
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.runner import PipelineResult


class TableRowModelHandler(SklearnModelHandlerNumpy):
  """ModelHandler that processes table rows (beam.Row objects) for inference.

  This handler extends SklearnModelHandlerNumpy to work with structured
  table data represented as beam.Row objects. It extracts specified feature
  columns from the row and converts them to numpy arrays for model input.

  Attributes:
    feature_columns: List of column names to extract as features from input rows
  """
  def __init__(
      self,
      model_uri: str,
      feature_columns: list[str],
      model_file_type: ModelFileType = ModelFileType.PICKLE):
    """Initialize the TableRowModelHandler.

    Args:
      model_uri: Path to the saved model file (local or GCS)
      feature_columns: List of column names to use as model features
      model_file_type: Type of model file (PICKLE or JOBLIB)
    """
    super().__init__(model_uri=model_uri, model_file_type=model_file_type)
    self.feature_columns = feature_columns

  def run_inference(
      self,
      batch: list[beam.Row],
      model: Any,
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Run inference on a batch of beam.Row objects.

    Args:
      batch: List of beam.Row objects containing input features
      model: Loaded sklearn model
      inference_args: Optional additional arguments for inference

    Yields:
      PredictionResult containing the original row and prediction
    """
    features_array = []
    for row in batch:
      row_dict = row._asdict()
      features = [row_dict[col] for col in self.feature_columns]
      features_array.append(features)

    features_array = np.array(features_array, dtype=np.float32)
    predictions = model.predict(features_array)

    for row, prediction in zip(batch, predictions):
      yield PredictionResult(
          example=row, inference=float(prediction), model_id=self._model_uri)


class FormatTableOutput(beam.DoFn):
  """DoFn that formats inference results into table output schema.

  Takes PredictionResult objects from KeyedModelHandler and formats them
  into dictionaries suitable for writing to BigQuery or other table outputs.
  """
  def __init__(self, feature_columns: list[str]):
    self.feature_columns = feature_columns

  def process(
      self, element: tuple[str, PredictionResult]) -> Iterable[dict[str, Any]]:
    """Process a keyed inference result into table output format.

    Args:
      element: Tuple of (row_key, PredictionResult)

    Yields:
      Dictionary with all input fields plus prediction and metadata
    """
    key, prediction = element
    row = prediction.example
    row_dict = row._asdict()
    output = {'row_key': key, 'prediction': prediction.inference}

    if prediction.model_id:
      output['model_id'] = prediction.model_id

    for field_name in self.feature_columns:
      output[f'input_{field_name}'] = row_dict[field_name]

    yield output


def parse_json_to_table_row(
    message: bytes,
    schema_fields: Optional[list[str]] = None) -> tuple[str, beam.Row]:
  """Parse JSON message to (key, beam.Row) format for KeyedModelHandler.

  Args:
    message: JSON-encoded bytes
    schema_fields: Optional list of expected field names

  Returns:
    Tuple of (unique_key, beam.Row with parsed data)
  """
  data = json.loads(message.decode('utf-8'))

  row_key = data.get('id', str(hash(message)))

  row_fields = {}
  for key, value in data.items():
    if key != 'id' and (schema_fields is None or key in schema_fields):
      if isinstance(value, (int, float)):
        row_fields[key] = float(value)
      else:
        row_fields[key] = value

  table_row = beam.Row(**row_fields)
  return row_key, table_row


def build_output_schema(feature_columns: list[str]) -> str:
  """Build BigQuery schema string for output table.

  Args:
    feature_columns: List of feature column names

  Returns:
    BigQuery schema string
  """
  schema_parts = ['row_key:STRING', 'prediction:FLOAT', 'model_id:STRING']

  for col in feature_columns:
    schema_parts.append(f'input_{col}:FLOAT')

  return ','.join(schema_parts)


def parse_known_args(argv):
  """Parse command-line arguments for the pipeline."""
  parser = argparse.ArgumentParser()

  parser.add_argument(
      '--mode',
      default='batch',
      choices=['streaming', 'batch'],
      help='Pipeline mode: streaming or batch')
  parser.add_argument(
      '--input_subscription',
      help='Pub/Sub subscription for streaming mode '
      '(format: projects/PROJECT/subscriptions/SUBSCRIPTION)')
  parser.add_argument(
      '--input_file',
      help='Input file path for batch mode (e.g., gs://bucket/input.jsonl)')
  parser.add_argument(
      '--output_table',
      help='BigQuery output table (format: PROJECT:DATASET.TABLE)')
  parser.add_argument('--model_path', help='Path to saved model file')
  parser.add_argument(
      '--feature_columns', help='Comma-separated list of feature column names')
  parser.add_argument(
      '--window_size_sec',
      type=int,
      default=60,
      help='Window size in seconds for streaming mode (default: 60)')
  parser.add_argument(
      '--trigger_interval_sec',
      type=int,
      default=30,
      help='Trigger interval in seconds for streaming mode (default: 30)')
  parser.add_argument(
      '--input_expand_factor',
      type=int,
      default=1,
      help='In batch mode: repeat each input line this many times to scale up '
      'volume (e.g. 100k lines × 100 = 10M rows). Default 1 = no expansion.')
  return parser.parse_known_args(argv)


def run(
    argv=None, save_main_session=True, test_pipeline=None) -> PipelineResult:
  """Main pipeline execution function.

  Args:
    argv: Command-line arguments
    save_main_session: Whether to save main session for workers
    test_pipeline: Optional test pipeline (for testing)

  Returns:
    PipelineResult from pipeline execution
  """
  known_args, pipeline_args = parse_known_args(argv)

  if known_args.mode == 'streaming' and not known_args.input_subscription:
    raise ValueError('input_subscription is required for streaming mode')
  if known_args.mode == 'batch' and not known_args.input_file:
    raise ValueError('input_file is required for batch mode')

  feature_columns = [
      col.strip() for col in known_args.feature_columns.split(',')
  ]

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(StandardOptions).streaming = (
      known_args.mode == 'streaming')

  model_handler = TableRowModelHandler(
      model_uri=known_args.model_path, feature_columns=feature_columns)

  output_schema = build_output_schema(feature_columns)

  pipeline = test_pipeline or beam.Pipeline(options=pipeline_options)

  if known_args.mode == 'streaming':
    input_data = (
        pipeline
        | 'ReadFromPubSub' >>
        beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
        | 'ParseToTableRows' >>
        beam.Map(lambda msg: parse_json_to_table_row(msg, feature_columns))
        | 'WindowedData' >> beam.WindowInto(
            beam.window.FixedWindows(known_args.window_size_sec),
            trigger=beam.trigger.AfterProcessingTime(
                known_args.trigger_interval_sec),
            accumulation_mode=beam.trigger.AccumulationMode.DISCARDING,
            allowed_lateness=0))
    write_method = beam.io.WriteToBigQuery.Method.STREAMING_INSERTS
  else:
    read_lines = (
        pipeline
        | 'ReadFromFile' >> beam.io.ReadFromText(known_args.input_file))
    expand_factor = getattr(known_args, 'input_expand_factor', 1) or 1
    if expand_factor > 1:
      read_lines = (
          read_lines
          | 'ExpandInput' >> beam.FlatMap(lambda line: [line] * expand_factor))
    input_data = (
        read_lines
        | 'ParseToTableRows' >> beam.Map(
            lambda line: parse_json_to_table_row(
                line.encode('utf-8'), feature_columns)))
    write_method = beam.io.WriteToBigQuery.Method.FILE_LOADS

  write_disposition = (
      beam.io.BigQueryDisposition.WRITE_APPEND if known_args.mode == 'streaming'
      else beam.io.BigQueryDisposition.WRITE_TRUNCATE)
  _ = (
      input_data
      | 'RunInference' >> RunInference(KeyedModelHandler(model_handler))
      | 'FormatOutput' >> beam.ParDo(FormatTableOutput(feature_columns))
      | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
          known_args.output_table,
          schema=output_schema,
          write_disposition=write_disposition,
          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
          method=write_method))

  result = pipeline.run()

  if known_args.mode == 'batch' and not test_pipeline:
    result.wait_until_finish()

  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
