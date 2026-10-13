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

"""Benchmark test for table row inference pipeline.

This benchmark measures the performance of RunInference with continuous
table row inputs, including throughput, latency, and cost metrics.
"""

import logging

from apache_beam.examples.inference import table_row_inference
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.testing.load_tests.dataflow_cost_benchmark import DataflowCostBenchmark
from apache_beam.testing.load_tests.load_test import LoadTestOptions


class TableRowInferenceOptions(
    LoadTestOptions,
    StandardOptions,
    GoogleCloudOptions,
    WorkerOptions,
    DebugOptions,
    SetupOptions,
):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--mode', default='batch')
    parser.add_argument('--input_subscription', default='')
    parser.add_argument('--input_file', default='')
    parser.add_argument('--output_table')
    parser.add_argument('--model_path')
    parser.add_argument('--feature_columns')
    parser.add_argument('--window_size_sec', type=int, default=60)
    parser.add_argument('--trigger_interval_sec', type=int, default=30)
    parser.add_argument('--input_expand_factor', type=int, default=1)


class TableRowInferenceBenchmarkTest(DataflowCostBenchmark):
  """Benchmark for continuous table row inference with RunInference.

  This benchmark measures:
  - Mean Inference Batch Size: Average batch size for inference
  - Mean Inference Batch Latency: Average time per batch inference
  - Mean Load Model Latency: Time to load the model
  - Throughput: Elements processed per second
  - Cost: Estimated cost on Dataflow
  """
  options_class = TableRowInferenceOptions

  def __init__(self):
    self.metrics_namespace = 'BeamML_TableInference'
    super().__init__(
        metrics_namespace=self.metrics_namespace,
        is_streaming=False,
        pcollection='RunInference/BeamML_RunInference_Postprocess-0.out0')
    self.opts = self.pipeline.get_pipeline_options().view_as(
        TableRowInferenceOptions)
    mode = self.opts.mode or 'batch'
    self.is_streaming = mode == 'streaming'
    if self.is_streaming:
      self.subscription = self.opts.input_subscription

  def test(self):
    """Execute the table row inference pipeline for benchmarking."""
    mode = self.opts.mode or 'batch'
    extra_opts = {'mode': mode}

    if mode == 'streaming':
      if self.opts.input_subscription:
        extra_opts['input_subscription'] = self.opts.input_subscription
      extra_opts['window_size_sec'] = (
          self.opts.window_size_sec
          if self.opts.window_size_sec is not None else 60)
      extra_opts['trigger_interval_sec'] = (
          self.opts.trigger_interval_sec
          if self.opts.trigger_interval_sec is not None else 30)
    elif self.opts.input_file:
      extra_opts['input_file'] = self.opts.input_file

    if self.opts.output_table:
      extra_opts['output_table'] = self.opts.output_table
    if self.opts.model_path:
      extra_opts['model_path'] = self.opts.model_path
    if self.opts.feature_columns:
      extra_opts['feature_columns'] = self.opts.feature_columns

    self.result = table_row_inference.run(
        self.pipeline.get_full_options_as_args(**extra_opts),
        test_pipeline=self.pipeline)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  TableRowInferenceBenchmarkTest().run()
