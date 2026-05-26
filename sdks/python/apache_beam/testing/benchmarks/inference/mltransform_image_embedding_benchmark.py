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
from typing import Optional

from google.cloud import monitoring_v3
from google.protobuf.duration_pb2 import Duration

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

  def _get_worker_time_interval(
      self, job_id: str) -> tuple[Optional[str], Optional[str]]:
    """GPU jobs use separate CPU and GPU worker pools.

    The shared base class stops paging logs after the first start/stop pair,
    which can end the monitoring window before the GPU pool finishes. Scan all
    pages and use the earliest start and latest stop for GPU runs only.
    """
    if self.opts.device != 'GPU':
      return super()._get_worker_time_interval(job_id)

    start_time, end_time = None, None
    page_token = None
    message_count = 0
    last_message_time = None
    while True:
      messages, page_token = self.dataflow_client.list_messages(
          job_id=job_id,
          start_time=None,
          end_time=None,
          page_token=page_token,
          minimum_importance='JOB_MESSAGE_DEBUG')
      for message in messages:
        message_count += 1
        text = getattr(message, 'messageText', None) or getattr(
            message, 'message_text', None)
        if getattr(message, 'time', None):
          last_message_time = message.time
        if not text:
          continue
        if self.WORKER_START_PATTERN.search(text):
          if start_time is None or message.time < start_time:
            start_time = message.time
          logging.info('Matched WORKER_START_PATTERN: %r', text)
        if self.WORKER_STOP_PATTERN.search(text):
          if end_time is None or message.time > end_time:
            end_time = message.time
          logging.info('Matched WORKER_STOP_PATTERN: %r', text)
      if not page_token:
        break
    if start_time and not end_time and last_message_time:
      end_time = last_message_time
      logging.info(
          'Using last job message time as end_time for GPU job: %s', end_time)
    if not start_time or not end_time:
      logging.warning(
          'Could not determine GPU worker time interval. '
          'start_time=%s, end_time=%s, total messages=%d',
          start_time,
          end_time,
          message_count)
    return start_time, end_time

  def _get_throughput_metrics(
      self,
      project: str,
      job_id: str,
      start_time: str,
      end_time: str,
      pcollection_name: str | None = None) -> dict[str, float]:
    pcollection_candidates = [
        pcollection_name or self.pcollection,
        'MLTransformImageEmbeddings.out0',
        'MLTransformImageEmbeddings/RunInference.out0',
        'MLTransformImageEmbeddings/RunInference/'
        'BeamML_RunInference_Postprocess-0.out0',
        'WriteToBigQuery/BigQueryBatchFileLoads/TriggerLoadJobs.out0',
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
