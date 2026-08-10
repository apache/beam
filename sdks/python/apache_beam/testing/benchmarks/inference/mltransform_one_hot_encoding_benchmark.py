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
# pytype: skip-file

"""Benchmark test for MLTransform One-Hot Encoding pipeline.

This benchmark measures the performance of MLTransform for one-hot encoding
categorical features, including throughput, latency, and cost metrics on
Dataflow.
"""

import logging
from datetime import datetime
from typing import Optional

from apache_beam.examples.ml_transform import mltransform_one_hot_encoding
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.testing.load_tests.dataflow_cost_benchmark import DataflowCostBenchmark
from apache_beam.testing.load_tests.load_test import LoadTestOptions


class MLTransformOneHotEncodingOptions(
    LoadTestOptions,
    StandardOptions,
    GoogleCloudOptions,
    WorkerOptions,
    DebugOptions,
    SetupOptions,
):
  """Pipeline options for MLTransform One-Hot Encoding benchmark."""
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--input_file',
        default='',
        help='Input JSONL/text file path for benchmark data.')
    parser.add_argument(
        '--input_format',
        choices=['jsonl', 'text'],
        default='jsonl',
        help='Input file format for input_file: jsonl or text.')
    parser.add_argument(
        '--output_file',
        required=True,
        help='Output file path for encoded results')
    parser.add_argument(
        '--artifact_location',
        required=True,
        help='GCS path to store MLTransform artifacts')
    parser.add_argument(
        '--categorical_columns',
        default='category',
        help='Comma-separated list of categorical column names to encode')
    parser.add_argument(
        '--num_records',
        type=int,
        default=100000,
        help='Number of synthetic records to generate')


class MLTransformOneHotEncodingBenchmarkTest(DataflowCostBenchmark):
  """Benchmark for MLTransform One-Hot Encoding on Dataflow.

  This benchmark measures:
  - Throughput: Elements processed per second
  - Latency: Time to process input records
  - Cost: Estimated cost on Dataflow

  The pcollection is chosen to capture the output of the MLTransform
  step where one-hot encoding is applied.
  """
  options_class = MLTransformOneHotEncodingOptions

  def __init__(self):
    self.metrics_namespace = 'BeamML_MLTransform'
    # Use the output of MLTransform step for throughput measurement
    # This captures the processed data after vocabulary encoding
    super().__init__(
        metrics_namespace=self.metrics_namespace,
        is_streaming=False,
        pcollection='FormatOutput.out0')

  def test(self):
    """Execute the one-hot encoding pipeline for benchmarking."""
    extra_opts = {}

    extra_opts['output_file'] = self.pipeline.get_option('output_file')
    extra_opts['artifact_location'] = self.pipeline.get_option(
        'artifact_location')
    extra_opts['categorical_columns'] = (
        self.pipeline.get_option('categorical_columns') or 'category')

    input_file = self.pipeline.get_option('input_file')
    if input_file:
      extra_opts['input_file'] = input_file
      extra_opts['input_format'] = (
          self.pipeline.get_option('input_format') or 'jsonl')
    else:
      # Handle synthetic data generation
      num_records = self.pipeline.get_option('num_records')
      if num_records:
        extra_opts['num_records'] = int(num_records)

    self.result = mltransform_one_hot_encoding.run(
        self.pipeline.get_full_options_as_args(**extra_opts),
        test_pipeline=self.pipeline)

  def _get_throughput_metrics(
      self,
      project: str,
      job_id: str,
      start_time: datetime,
      end_time: datetime,
      pcollection_name: Optional[str] = None,
  ) -> dict[str, float]:
    """Get throughput metrics with runner-v2-friendly fallbacks."""
    return self._get_throughput_metrics_with_pcollection_fallback(
        project,
        job_id,
        start_time,
        end_time,
        pcollection_candidates=[
            self.pcollection,
            'MLTransform.out0',
            'FormatOutput.out0',
        ],
        pcollection_name=pcollection_name)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  MLTransformOneHotEncodingBenchmarkTest().run()
