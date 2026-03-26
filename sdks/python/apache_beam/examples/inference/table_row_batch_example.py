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

"""Complete batch inference example with sample data generation.

This script demonstrates how to use the batch table row inference pipeline
with automatically generated sample data and model.

Usage:
  # Run complete local example
  python table_row_batch_example.py

  # Run with custom parameters
  python table_row_batch_example.py --num_rows=1000 --num_features=5
"""

import argparse
import json
import logging
import os
import pickle
import sys
import tempfile

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_sample_data_and_model(tmpdir, num_rows=100, num_features=3):
  """Create sample model and data for testing.

  Args:
    tmpdir: Temporary directory path
    num_rows: Number of data rows to generate
    num_features: Number of features per row

  Returns:
    Tuple of (model_path, data_path, feature_columns)
  """
  try:
    import numpy as np
    from sklearn.linear_model import LinearRegression
  except ImportError:
    logger.error(
        'sklearn and numpy are required. '
        'Install with: pip install scikit-learn numpy')
    sys.exit(1)

  logger.info('Creating sample model with %s features...', num_features)
  model = LinearRegression()
  X_train = np.random.randn(100, num_features)
  y_train = np.sum(X_train, axis=1) + np.random.randn(100) * 0.1
  model.fit(X_train, y_train)

  model_path = os.path.join(tmpdir, 'model.pkl')
  with open(model_path, 'wb') as f:
    pickle.dump(model, f)
  logger.info('  ✓ Model saved to %s', model_path)

  logger.info('Generating %s sample data rows...', num_rows)
  data_path = os.path.join(tmpdir, 'input_data.jsonl')
  feature_columns = [f'feature{i+1}' for i in range(num_features)]

  with open(data_path, 'w') as f:
    for i in range(num_rows):
      row = {'id': f'row_{i}'}
      for col in feature_columns:
        row[col] = float(np.random.randn())
      f.write(json.dumps(row) + '\n')
  logger.info('  ✓ Data saved to %s', data_path)

  return model_path, data_path, feature_columns


def run_example(num_rows=100, num_features=3):
  """Run complete batch inference example.

  Args:
    num_rows: Number of data rows to generate
    num_features: Number of features per row
  """
  logger.info('=' * 70)
  logger.info('BATCH TABLE ROW INFERENCE - COMPLETE EXAMPLE')
  logger.info('=' * 70)

  with tempfile.TemporaryDirectory() as tmpdir:
    logger.info('\n[Step 1/4] Creating sample model and data...')
    model_path, data_path, feature_columns = create_sample_data_and_model(
        tmpdir, num_rows, num_features)

    logger.info('\n[Step 2/4] Setting up output paths...')
    output_file = os.path.join(tmpdir, 'predictions')
    logger.info('  Output file: %s.jsonl', output_file)

    logger.info('\n[Step 3/4] Running batch inference pipeline...')
    logger.info('  Features: %s', feature_columns)

    cmd = [
        sys.executable,
        'table_row_inference.py',
        '--mode=batch',
        f'--input_file={data_path}',
        f'--model_path={model_path}',
        f'--feature_columns={",".join(feature_columns)}',
        f'--output_file={output_file}',
        '--runner=DirectRunner'
    ]

    logger.info('  Command: %s', ' '.join(cmd))

    import subprocess
    script_dir = os.path.dirname(os.path.abspath(__file__))
    result = subprocess.run(
        cmd, capture_output=True, text=True, check=False, cwd=script_dir)

    if result.returncode != 0:
      logger.error('Pipeline failed!')
      logger.error(result.stderr)
      sys.exit(1)

    logger.info('  ✓ Pipeline completed successfully!')

    logger.info('\n[Step 4/4] Viewing results...')
    output_path = '%s.jsonl' % output_file

    if os.path.exists(output_path):
      with open(output_path, 'r') as f:
        lines = f.readlines()

      logger.info('  Total predictions: %s', len(lines))
      logger.info('\n  Sample predictions (first 5):')

      for i, line in enumerate(lines[:5]):
        prediction = json.loads(line)
        feats = [prediction[f'input_{f}'] for f in feature_columns][:3]
        logger.info(
            '    %s. Key: %10s | Prediction: %8.4f | Features: %s...',
            i + 1,
            prediction['row_key'],
            prediction['prediction'],
            feats)

      logger.info('\n  Full output saved to: %s', output_path)
    else:
      logger.warning('  Output file not found: %s', output_path)

  logger.info('\n' + '=' * 70)
  logger.info('EXAMPLE COMPLETED SUCCESSFULLY!')
  logger.info('=' * 70)
  logger.info('\n📖 Next steps:')
  logger.info('  1. Review the generated predictions above')
  logger.info(
      '  2. Try with your own data: '
      'python table_row_inference.py --help')
  logger.info('  3. Deploy to Dataflow with --runner=DataflowRunner')
  logger.info('\n✨ You now understand batch table row inference!')


def main():
  parser = argparse.ArgumentParser(
      description='Run batch table row inference example')
  parser.add_argument(
      '--num_rows',
      type=int,
      default=100,
      help='Number of data rows to generate (default: 100)')
  parser.add_argument(
      '--num_features',
      type=int,
      default=3,
      help='Number of features per row (default: 3)')

  args = parser.parse_args()

  run_example(num_rows=args.num_rows, num_features=args.num_features)


if __name__ == '__main__':
  main()
