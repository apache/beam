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

"""Categorical encoding pipeline using MLTransform for batch processing.

This pipeline demonstrates MLTransform's ComputeAndApplyVocabulary transform
for categorical feature encoding. It can either read input data from a file
or generate synthetic test data, computes vocabulary on categorical columns,
and converts categorical values to integer indices.

Example usage with input file:
  python mltransform_one_hot_encoding.py \
    --input_file=gs://bucket/input.jsonl \
    --output_file=gs://bucket/output.jsonl \
    --artifact_location=gs://bucket/artifacts \
    --categorical_columns=category \
    --runner=DataflowRunner \
    --project=PROJECT \
    --region=us-central1 \
    --temp_location=gs://bucket/temp

Example usage with synthetic data:
  python mltransform_one_hot_encoding.py \
    --output_file=gs://bucket/output.jsonl \
    --categorical_columns=category \
    --num_records=100000 \
    --runner=DataflowRunner \
    --project=PROJECT \
    --region=us-central1
"""

import argparse
import json
import logging
import tempfile
from typing import Any

import apache_beam as beam
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.ml.transforms.tft import ComputeAndApplyVocabulary
from apache_beam.runners.runner import PipelineResult


def parse_json_line(line: str) -> dict[str, Any]:
  """Parse a JSON line into a dictionary."""
  try:
    return json.loads(line)
  except json.JSONDecodeError as e:
    raise ValueError(f"Failed to parse JSON line: {line[:200]}...") from e


def parse_text_line(line: str,
                    categorical_columns: list[str]) -> dict[str, Any]:
  """Parse plain text line into the first categorical column."""
  text_value = line.strip()
  if not text_value:
    text_value = 'unknown'
  return {categorical_columns[0]: text_value}


def format_json_output(element: Any) -> str:
  """Format output element as JSON string."""
  def to_json_compatible(value: Any) -> Any:
    """Recursively convert non-JSON types (e.g. numpy arrays/scalars)."""
    if isinstance(value, dict):
      return {k: to_json_compatible(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
      return [to_json_compatible(v) for v in value]

    # MLTransform outputs may include numpy scalar/ndarray values.
    if hasattr(value, 'tolist'):
      return to_json_compatible(value.tolist())
    if hasattr(value, 'item'):
      try:
        return to_json_compatible(value.item())
      except (TypeError, ValueError):
        pass
    return value

  if hasattr(element, 'as_dict'):
    return json.dumps(to_json_compatible(element.as_dict()))
  if hasattr(element, '_asdict'):
    return json.dumps(to_json_compatible(element._asdict()))
  return json.dumps(to_json_compatible(dict(element)))


def generate_synthetic_record(index: int,
                              categorical_columns: list[str]) -> dict[str, str]:
  """Generate a deterministic synthetic record with categorical values."""
  categories = [
      'electronics',
      'clothing',
      'food',
      'books',
      'sports',
      'home',
      'toys',
      'health',
      'automotive',
      'garden'
  ]
  colors = [
      'red',
      'blue',
      'green',
      'yellow',
      'black',
      'white',
      'purple',
      'orange',
      'pink',
      'gray'
  ]
  sizes = ['small', 'medium', 'large', 'xlarge', 'tiny', 'huge']

  record = {}
  for col in categorical_columns:
    if col.lower() in ['category', 'type', 'product']:
      record[col] = categories[index % len(categories)]
    elif col.lower() in ['color', 'colour']:
      record[col] = colors[index % len(colors)]
    elif col.lower() in ['size', 'dimension']:
      record[col] = sizes[index % len(sizes)]
    else:
      # Default to categories for unknown columns
      record[col] = categories[index % len(categories)]
  return record


def run(
    argv=None,
    save_main_session=True,
    test_pipeline=None) -> PipelineResult | None:
  """Run the categorical encoding pipeline."""
  known_args, pipeline_args = parse_known_args(argv)

  categorical_columns = [
      col.strip() for col in known_args.categorical_columns.split(',')
  ]

  if not categorical_columns or not categorical_columns[0]:
    raise ValueError("At least one categorical column must be specified")

  if not known_args.output_file:
    raise ValueError("--output_file is required")

  # Create artifact location if not provided
  artifact_location = known_args.artifact_location
  if not artifact_location:
    artifact_location = tempfile.mkdtemp()
    logging.info("Using temporary artifact location: %s", artifact_location)

  pipeline_options = beam.options.pipeline_options.PipelineOptions(
      pipeline_args)
  pipeline_options.view_as(
      beam.options.pipeline_options.SetupOptions
  ).save_main_session = save_main_session

  pipeline = test_pipeline or beam.Pipeline(options=pipeline_options)

  # Use synthetic data or read from file
  if known_args.input_file:
    # Read and parse input data from file
    if known_args.input_format == 'jsonl':
      parse_input_fn = parse_json_line
    else:
      if len(categorical_columns) > 1:
        logging.warning(
            'Input format is "text" but multiple categorical columns are '
            'specified. Only the first column "%s" will be used for parsing.',
            categorical_columns[0])
      parse_input_fn = lambda line: parse_text_line(line, categorical_columns)
    raw_data = (
        pipeline
        | 'ReadFromJSONL' >> beam.io.ReadFromText(known_args.input_file)
        | 'ParseInput' >> beam.Map(parse_input_fn))
  else:
    # Generate synthetic data
    num_records = known_args.num_records or 100000
    logging.info("Generating %d synthetic records", num_records)

    raw_data = (
        pipeline
        | 'GenerateSyntheticIndexes' >> beam.Create(range(num_records))
        | 'BuildSyntheticRecord' >> beam.Map(
            lambda idx: generate_synthetic_record(idx, categorical_columns)))

  # Build MLTransform with ComputeAndApplyVocabulary
  ml_transform = MLTransform(
      write_artifact_location=artifact_location,
  ).with_transform(
      ComputeAndApplyVocabulary(
          columns=categorical_columns, vocab_filename='vocab_onehot'))

  # Apply MLTransform
  transformed_data = (
      raw_data
      | 'ValidateAndFilterColumns' >> beam.Filter(
          lambda element: all(col in element for col in categorical_columns))
      | 'MLTransform' >> ml_transform
      | 'FormatOutput' >> beam.Map(format_json_output))

  # Write output
  _ = (
      transformed_data
      | 'WriteToJSONL' >> beam.io.WriteToText(
          known_args.output_file, file_name_suffix='.jsonl'))

  result = pipeline.run()
  return result


def parse_known_args(argv):
  """Parse command-line arguments."""
  parser = argparse.ArgumentParser(
      description='Categorical encoding pipeline using MLTransform')

  parser.add_argument(
      '--input_file',
      help='Input JSONL file path (local or GCS). '
      'If not provided, synthetic data will be generated.')
  parser.add_argument(
      '--input_format',
      choices=['jsonl', 'text'],
      default='jsonl',
      help='Input file format for --input_file. Use jsonl for JSON lines '
      'or text for plain text lines (default: jsonl).')
  parser.add_argument(
      '--output_file',
      required=True,
      help='Output file prefix for encoded results (JSONL format)')
  parser.add_argument(
      '--artifact_location',
      help='GCS or local path to store MLTransform artifacts '
      '(vocabulary files). If not provided, a temp location is used.')
  parser.add_argument(
      '--categorical_columns',
      required=True,
      help='Comma-separated list of categorical column names to encode')
  parser.add_argument(
      '--num_records',
      type=int,
      default=100000,
      help='Number of synthetic records to generate if --input_file is not '
      'provided (default: 100000)')

  return parser.parse_known_args(argv)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
