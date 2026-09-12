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

import logging
from datetime import datetime
from typing import Optional

from apache_beam.examples.ml_transform import mltransform_image_embedding
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.testing.load_tests import dataflow_cost_consts as costs
from apache_beam.testing.load_tests.dataflow_cost_benchmark import DataflowCostBenchmark
from apache_beam.testing.load_tests.load_test import LoadTestOptions


class MLTransformImageEmbeddingOptions(
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
    parser.add_argument('--input', default='')
    parser.add_argument('--input_file', default='')
    parser.add_argument('--output_table', default='')
    parser.add_argument('--artifact_location', default='')
    parser.add_argument(
        '--pretrained_model_name',
        default=mltransform_image_embedding.DEFAULT_IMAGE_MODEL_NAME)
    parser.add_argument('--device', default='CPU')
    parser.add_argument('--min_batch_size', type=int, default=8)
    parser.add_argument('--max_batch_size', type=int, default=64)
    parser.add_argument(
        '--embedding_accelerator',
        default=mltransform_image_embedding.DEFAULT_ACCELERATOR)
    parser.add_argument(
        '--embedding_min_ram',
        default=mltransform_image_embedding.DEFAULT_EMBEDDING_MIN_RAM)


class MLTransformImageEmbeddingBenchmarkTest(DataflowCostBenchmark):
  options_class = MLTransformImageEmbeddingOptions

  def __init__(self):
    self.metrics_namespace = 'BeamML_MLTransform'
    super().__init__(
        metrics_namespace=self.metrics_namespace,
        pcollection='FormatOutput.out0')
    self.opts = self.pipeline.get_pipeline_options().view_as(
        MLTransformImageEmbeddingOptions)
    if self.opts.device == 'GPU':
      self.gpu = costs.Accelerator.T4
      self.multi_worker_pool = True

  def _get_throughput_metrics(
      self,
      project: str,
      job_id: str,
      start_time: datetime,
      end_time: datetime,
      pcollection_name: Optional[str] = None,
  ) -> dict[str, float]:
    return self._get_throughput_metrics_with_pcollection_fallback(
        project,
        job_id,
        start_time,
        end_time,
        pcollection_candidates=[
            self.pcollection,
            'MLTransformImageEmbeddings.out0',
            'MLTransformImageEmbeddings/RunInference.out0',
            'MLTransformImageEmbeddings/RunInference/'
            'BeamML_RunInference_Postprocess-0.out0',
            'WriteToBigQuery/BigQueryBatchFileLoads/TriggerLoadJobs.out0',
        ],
        pcollection_name=pcollection_name)

  def test(self):
    input_path = self.opts.input or self.opts.input_file
    if not input_path:
      raise RuntimeError('Please provide --input or --input_file.')
    if not self.opts.output_table:
      raise RuntimeError('Please provide --output_table.')
    if not self.opts.artifact_location:
      raise RuntimeError('Please provide --artifact_location.')

    extra_opts = {
        'mode': self.opts.mode,
        'input': input_path,
        'output_table': self.opts.output_table,
        'artifact_location': self.opts.artifact_location,
        'pretrained_model_name': self.opts.pretrained_model_name,
        'device': self.opts.device,
        'min_batch_size': self.opts.min_batch_size,
        'max_batch_size': self.opts.max_batch_size,
        'embedding_accelerator': self.opts.embedding_accelerator,
        'embedding_min_ram': self.opts.embedding_min_ram,
    }

    self.result = mltransform_image_embedding.run(
        self.pipeline.get_full_options_as_args(**extra_opts),
        test_pipeline=self.pipeline)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  MLTransformImageEmbeddingBenchmarkTest().run()
