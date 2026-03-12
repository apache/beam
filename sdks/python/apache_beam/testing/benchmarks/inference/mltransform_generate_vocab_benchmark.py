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

"""Benchmark test for the batch-only MLTransform Generate Vocab pipeline."""

import logging

from apache_beam.examples.ml_transform import mltransform_generate_vocab
from apache_beam.testing.load_tests.dataflow_cost_benchmark import DataflowCostBenchmark


class MLTransformGenerateVocabBenchmarkTest(DataflowCostBenchmark):
  """Runs the MLTransform vocab generation pipeline as a cost benchmark."""
  def __init__(self):
    # Namespace used by benchmark dashboards.
    self.metrics_namespace = 'BeamML_MLTransformVocab'
    # Monitor token throughput after filtering empty values.
    super().__init__(
        metrics_namespace=self.metrics_namespace,
        is_streaming=False,
        pcollection='DropEmptyTokens.out0')

  def test(self):
    """Execute the MLTransform vocab generation pipeline for benchmarking."""
    extra_opts = {
        'input_file': self.pipeline.get_option('input_file'),
        'output_vocab': self.pipeline.get_option('output_vocab'),
        'artifact_location': self.pipeline.get_option('artifact_location'),
        'columns': self.pipeline.get_option('columns'),
        'vocab_size': self.pipeline.get_option('vocab_size'),
        'min_frequency': self.pipeline.get_option('min_frequency'),
        'lowercase': self.pipeline.get_option('lowercase'),
        'tokenizer': self.pipeline.get_option('tokenizer'),
        'oov_token': self.pipeline.get_option('oov_token'),
        'input_expand_factor': self.pipeline.get_option('input_expand_factor'),
    }

    self.result = mltransform_generate_vocab.run(
        self.pipeline.get_full_options_as_args(**extra_opts))


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  MLTransformGenerateVocabBenchmarkTest().run()
