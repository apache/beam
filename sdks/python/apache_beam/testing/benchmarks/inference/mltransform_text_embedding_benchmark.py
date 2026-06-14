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

from google.cloud import monitoring_v3
from google.protobuf.duration_pb2 import Duration

from apache_beam.examples.ml_transform import mltransform_text_embedding
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.testing.load_tests.dataflow_cost_benchmark import DataflowCostBenchmark
from apache_beam.testing.load_tests.load_test import LoadTestOptions


class MLTransformTextEmbeddingOptions(
    LoadTestOptions,
    StandardOptions,
    GoogleCloudOptions,
    WorkerOptions,
    DebugOptions,
    SetupOptions,
):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--input', default='')
    parser.add_argument('--input_file', default='')
    parser.add_argument('--output', default='')
    parser.add_argument('--artifact_location', default='')
    parser.add_argument(
        '--model_name', default=mltransform_text_embedding.DEFAULT_MODEL_NAME)
    parser.add_argument('--min_batch_size', type=int, default=16)
    parser.add_argument('--max_batch_size', type=int, default=128)
    parser.add_argument('--model_batch_size', type=int, default=32)
    parser.add_argument('--device', default='CPU')
    parser.add_argument('--large_model', default='false')


class MLTransformTextEmbeddingBenchmarkTest(DataflowCostBenchmark):
  options_class = MLTransformTextEmbeddingOptions

  def __init__(self):
    self.metrics_namespace = 'BeamML_MLTransform'
    super().__init__(
        metrics_namespace=self.metrics_namespace,
        pcollection='FormatOutput.out0')
    self.opts = self.pipeline.get_pipeline_options().view_as(
        MLTransformTextEmbeddingOptions)

  def _get_throughput_metrics(
      self,
      project: str,
      job_id: str,
      start_time: str,
      end_time: str,
      pcollection_name: str | None = None) -> dict[str, float]:
    pcollection_candidates = [
        pcollection_name or self.pcollection,
        'MLTransformTextEmbeddings.out0',
        'MLTransformTextEmbeddings/RunInference.out0',
        'MLTransformTextEmbeddings/RunInference/'
        'BeamML_RunInference_Postprocess-0.out0',
        'WriteOutput/Write/WriteImpl/FinalizeWrite.out0',
    ]
    seen = set()
    for candidate in pcollection_candidates:
      if not candidate or candidate in seen:
        continue
      seen.add(candidate)
      metrics = super()._get_throughput_metrics(
          project, job_id, start_time, end_time, candidate)
      if (metrics.get('AvgThroughputBytes', 0) > 0 or
          metrics.get('AvgThroughputElements', 0) > 0):
        logging.info('Using throughput metrics for PCollection %s', candidate)
        return metrics

    logging.warning(
        'No PCollection-level throughput metrics found for candidates %s. '
        'Falling back to job-level Dataflow throughput metrics.',
        pcollection_candidates)
    return self._get_job_level_throughput_metrics(
        project, job_id, start_time, end_time)

  def _get_job_level_throughput_metrics(
      self, project: str, job_id: str, start_time: str,
      end_time: str) -> dict[str, float]:
    interval = monitoring_v3.TimeInterval(
        start_time=start_time, end_time=end_time)
    aggregation = monitoring_v3.Aggregation(
        alignment_period=Duration(seconds=60),
        per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_MEAN)
    requests = {
        'Bytes': monitoring_v3.ListTimeSeriesRequest(
            name=f'projects/{project}',
            filter=f'metric.type='
            f'"dataflow.googleapis.com/job/estimated_byte_count" '
            f'AND metric.labels.job_id="{job_id}"',
            interval=interval,
            aggregation=aggregation),
        'Elements': monitoring_v3.ListTimeSeriesRequest(
            name=f'projects/{project}',
            filter=f'metric.type="dataflow.googleapis.com/job/element_count" '
            f'AND metric.labels.job_id="{job_id}"',
            interval=interval,
            aggregation=aggregation),
    }
    metrics = {}
    for key, request in requests.items():
      values = [
          point.value.double_value
          for series in self.monitoring_client.list_time_series(
              request=request) for point in series.points
      ]
      metrics[f'AvgThroughput{key}'] = sum(values) / len(
          values) if values else 0.0
    return metrics

  def test(self):
    input_path = self.opts.input or self.opts.input_file
    if not input_path:
      raise RuntimeError('Please provide --input or --input_file.')
    if not self.opts.output:
      raise RuntimeError('Please provide --output.')
    if not self.opts.artifact_location:
      raise RuntimeError('Please provide --artifact_location.')

    extra_opts = {
        'input': input_path,
        'output': self.opts.output,
        'artifact_location': self.opts.artifact_location,
        'model_name': self.opts.model_name,
        'min_batch_size': self.opts.min_batch_size,
        'max_batch_size': self.opts.max_batch_size,
        'model_batch_size': self.opts.model_batch_size,
        'device': self.opts.device,
        'large_model': self.opts.large_model,
    }

    self.result = mltransform_text_embedding.run(
        self.pipeline.get_full_options_as_args(**extra_opts),
        test_pipeline=self.pipeline)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  MLTransformTextEmbeddingBenchmarkTest().run()
