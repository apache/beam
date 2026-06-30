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

"""Tests for mltransform_one_hot_encoding pipeline."""

import json
import logging
import os
import tempfile
import unittest
from glob import glob
from typing import Any

try:
  from apache_beam.examples.ml_transform import mltransform_one_hot_encoding
  from apache_beam.testing.test_pipeline import TestPipeline
except ImportError:  # pylint: disable=bare-except
  raise unittest.SkipTest('tensorflow_transform is not installed.')


def create_test_input_data() -> list[dict[str, Any]]:
  """Create sample test data for one-hot encoding."""
  return [
      {
          'category': 'electronics', 'color': 'red', 'size': 'small'
      },
      {
          'category': 'clothing', 'color': 'blue', 'size': 'medium'
      },
      {
          'category': 'electronics', 'color': 'green', 'size': 'large'
      },
      {
          'category': 'food', 'color': 'red', 'size': 'small'
      },
      {
          'category': 'clothing', 'color': 'blue', 'size': 'medium'
      },
  ]


class OneHotEncodingPipelineTest(unittest.TestCase):
  """Unit and integration tests for one-hot encoding pipeline."""
  def setUp(self):
    """Set up test fixtures."""
    self.test_dir = tempfile.mkdtemp()
    self.input_file = os.path.join(self.test_dir, 'input.jsonl')
    self.output_prefix = os.path.join(self.test_dir, 'output')
    self.artifact_location = os.path.join(self.test_dir, 'artifacts')

    # Create test input file
    test_data = create_test_input_data()
    with open(self.input_file, 'w', encoding='utf-8') as f:
      for record in test_data:
        f.write(json.dumps(record) + '\n')

  def tearDown(self):
    """Clean up test fixtures."""
    import shutil
    shutil.rmtree(self.test_dir, ignore_errors=True)

  def test_parse_json_line_valid(self):
    """Test parsing valid JSON lines."""
    line = '{"category": "electronics", "color": "red"}'
    result = mltransform_one_hot_encoding.parse_json_line(line)
    self.assertEqual(result['category'], 'electronics')
    self.assertEqual(result['color'], 'red')

  def test_parse_json_line_invalid(self):
    """Test parsing invalid JSON lines raises ValueError."""
    with self.assertRaises(ValueError):
      mltransform_one_hot_encoding.parse_json_line('not valid json')

  def test_format_json_output_with_row(self):
    """Test formatting beam.Row output as JSON."""
    import apache_beam as beam
    row = beam.Row(category='test', value=123)
    result = mltransform_one_hot_encoding.format_json_output(row)
    parsed = json.loads(result)
    self.assertEqual(parsed['category'], 'test')
    self.assertEqual(parsed['value'], 123)

  def test_format_json_output_with_dict(self):
    """Test formatting dict output as JSON."""
    element = {'category': 'test', 'value': 123}
    result = mltransform_one_hot_encoding.format_json_output(element)
    parsed = json.loads(result)
    self.assertEqual(parsed['category'], 'test')
    self.assertEqual(parsed['value'], 123)

  def test_end_to_end_pipeline_local(self):
    """Integration test running the full pipeline locally."""
    extra_opts = {
        'input_file': self.input_file,
        'output_file': self.output_prefix,
        'artifact_location': self.artifact_location,
        'categorical_columns': 'category,color,size',
    }

    with TestPipeline() as pipeline:
      mltransform_one_hot_encoding.run(
          argv=pipeline.get_full_options_as_args(**extra_opts),
          test_pipeline=pipeline)

    # Verify output shards exist.
    output_files = glob(self.output_prefix + '*.jsonl')
    self.assertTrue(
        output_files, f"Output files not found for: {self.output_prefix}")

    # Verify output content
    lines = []
    for output_file in output_files:
      with open(output_file, 'r', encoding='utf-8') as f:
        lines.extend(line.strip() for line in f if line.strip())

    self.assertEqual(len(lines), 5)

    # Parse and verify structure
    for line in lines:
      record = json.loads(line)
      # Should have original columns plus one-hot encoded versions
      self.assertIn('category', record)
      self.assertIn('color', record)
      self.assertIn('size', record)

  def test_pipeline_with_missing_columns(self):
    """Test pipeline handles records with missing columns gracefully."""
    # Create input with some missing columns
    mixed_data = [
        {
            'category': 'electronics', 'color': 'red', 'size': 'small'
        },
        {
            'category': 'clothing', 'color': 'blue'
        },  # missing size
        {
            'category': 'food'
        },  # missing color and size
    ]

    input_file = os.path.join(self.test_dir, 'mixed_input.jsonl')
    with open(input_file, 'w', encoding='utf-8') as f:
      for record in mixed_data:
        f.write(json.dumps(record) + '\n')

    extra_opts = {
        'input_file': input_file,
        'output_file': os.path.join(self.test_dir, 'mixed_output'),
        'artifact_location': os.path.join(self.test_dir, 'mixed_artifacts'),
        'categorical_columns': 'category,color,size',
    }

    with TestPipeline() as pipeline:
      mltransform_one_hot_encoding.run(
          argv=pipeline.get_full_options_as_args(**extra_opts),
          test_pipeline=pipeline)

    # Only first record should be processed
    output_files = glob(os.path.join(self.test_dir, 'mixed_output*.jsonl'))
    lines = []
    for output_file in output_files:
      with open(output_file, 'r', encoding='utf-8') as f:
        lines.extend(line.strip() for line in f if line.strip())

    self.assertEqual(len(lines), 1)
    record = json.loads(lines[0])
    self.assertEqual(record['category'], 'electronics')

  def test_cli_synthetic_data_no_input(self):
    """Test pipeline works without input file using synthetic data."""
    # Should not raise error when input_file is missing (uses synthetic data)
    with tempfile.TemporaryDirectory() as tmpdir:
      output_file = os.path.join(tmpdir, 'output')
      artifact_location = os.path.join(tmpdir, 'artifacts')

      with TestPipeline() as pipeline:
        # Should work without input_file (uses synthetic data)
        mltransform_one_hot_encoding.run(
            argv=pipeline.get_full_options_as_args(
                output_file=output_file,
                artifact_location=artifact_location,
                categorical_columns='category',
                num_records=100),
            test_pipeline=pipeline)

  def test_cli_validation_missing_output(self):
    """Test CLI argument validation for missing output file."""
    with self.assertRaises(ValueError) as context:
      mltransform_one_hot_encoding.run(
          argv=['--input_file=/tmp/in.jsonl', '--categorical_columns=category'])
    self.assertIn('output_file', str(context.exception).lower())

  def test_cli_validation_empty_columns(self):
    """Test CLI argument validation for empty columns."""
    with self.assertRaises(ValueError) as context:
      mltransform_one_hot_encoding.run(
          argv=[
              '--input_file=/tmp/in.jsonl',
              '--output_file=/tmp/out.jsonl',
              '--categorical_columns='
          ])
    self.assertIn('categorical', str(context.exception).lower())


class OneHotEncodingCLITest(unittest.TestCase):
  """Tests for CLI argument handling."""
  def test_parse_known_args_basic(self):
    """Test basic argument parsing."""
    args, _ = mltransform_one_hot_encoding.parse_known_args([
        '--input_file=/tmp/in.jsonl',
        '--output_file=/tmp/out.jsonl',
        '--categorical_columns=category,color',
    ])
    self.assertEqual(args.input_file, '/tmp/in.jsonl')
    self.assertEqual(args.output_file, '/tmp/out.jsonl')
    self.assertEqual(args.categorical_columns, 'category,color')

  def test_parse_known_args_with_artifact(self):
    """Test argument parsing with artifact location."""
    args, _ = mltransform_one_hot_encoding.parse_known_args([
        '--input_file=gs://bucket/in.jsonl',
        '--output_file=gs://bucket/out',
        '--artifact_location=gs://bucket/artifacts',
        '--categorical_columns=size,color',
    ])
    self.assertEqual(args.artifact_location, 'gs://bucket/artifacts')

  def test_parse_known_args_multiple_columns(self):
    """Test parsing multiple categorical columns."""
    args, _ = mltransform_one_hot_encoding.parse_known_args([
        '--input_file=in.jsonl',
        '--output_file=out.jsonl',
        '--categorical_columns=col1,col2,col3,col4',
    ])
    columns = [c.strip() for c in args.categorical_columns.split(',')]
    self.assertEqual(columns, ['col1', 'col2', 'col3', 'col4'])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
