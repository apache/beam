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

# pytype: skip-file

import logging
import re
import time
from datetime import datetime
from typing import Any
from typing import Optional

from google.cloud import monitoring_v3
from google.protobuf.duration_pb2 import Duration

import apache_beam.testing.load_tests.dataflow_cost_consts as costs
from apache_beam.metrics.execution import MetricResult
from apache_beam.runners.dataflow.dataflow_runner import DataflowPipelineResult
from apache_beam.runners.dataflow.internal.apiclient import DataflowApplicationClient
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.load_tests.load_test import LoadTest


class DataflowCostBenchmark(LoadTest):
  """Base class for Dataflow performance tests which export metrics to
  external databases: BigQuery or/and InfluxDB. Calculates the expected cost
  for running the job on Dataflow in region us-central1.

  Refer to :class:`~apache_beam.testing.load_tests.LoadTestOptions` for more
  information on the required pipeline options.

  If using InfluxDB with Basic HTTP authentication enabled, provide the
  following environment options: `INFLUXDB_USER` and `INFLUXDB_USER_PASSWORD`.

  If the hardware configuration for the job includes use of a GPU, please 
  specify the version in use with the Accelerator enumeration. This is used to
  calculate the cost of the job later, as different accelerators have different
  billing rates per hour of use.
  """

  WORKER_START_PATTERN = re.compile(
      r'^All workers have finished the startup processes and '
      r'began to receive work requests.*$')
  WORKER_STOP_PATTERN = re.compile(r'^Stopping worker pool.*$')

  def __init__(
      self,
      metrics_namespace: Optional[str] = None,
      is_streaming: bool = False,
      gpu: Optional[costs.Accelerator] = None,
      pcollection: str = 'ProcessOutput.out0'):
    """
    Initializes DataflowCostBenchmark.

    Args:
        metrics_namespace (Optional[str]): Namespace for metrics.
        is_streaming (bool): Whether the pipeline is streaming or batch.
        gpu (Optional[costs.Accelerator]): Optional GPU type.
        pcollection (str): PCollection name to monitor throughput.
    """
    self.is_streaming = is_streaming
    self.gpu = gpu
    self.pcollection = pcollection
    super().__init__(metrics_namespace=metrics_namespace)
    self.dataflow_client = DataflowApplicationClient(
        self.pipeline.get_pipeline_options())
    self.monitoring_client = monitoring_v3.MetricServiceClient()

  def run(self) -> None:
    try:
      self.test()
      if not hasattr(self, 'result'):
        self.result = self.pipeline.run()
        state = self.result.wait_until_finish(duration=self.timeout_ms)
        assert state != PipelineState.FAILED

      logging.info(
          'Pipeline complete, sleeping for 4 minutes to allow resource '
          'metrics to populate.')
      time.sleep(240)

      self.extra_metrics = self._retrieve_cost_metrics(self.result)
      additional_metrics = self._get_additional_metrics(self.result)
      self.extra_metrics.update(additional_metrics)

      logging.info(self.extra_metrics)
      self._metrics_monitor.publish_metrics(self.result, self.extra_metrics)
    finally:
      self.cleanup()

  def _retrieve_cost_metrics(self,
                             result: DataflowPipelineResult) -> dict[str, Any]:
    """Calculates estimated cost based on pipeline resource usage."""
    job_id = result.job_id()
    metrics = result.metrics().all_metrics(job_id)
    metrics_dict = self._process_metrics_list(metrics)

    cost = 0.0
    if self.is_streaming:
      cost += metrics_dict.get(
          "TotalVcpuTime", 0.0) / 3600 * costs.VCPU_PER_HR_STREAMING
      cost += metrics_dict.get(
          "TotalMemoryUsage", 0.0) / 1000 / 3600 * costs.MEM_PER_GB_HR_STREAMING
      cost += metrics_dict.get(
          "TotalStreamingDataProcessed", 0.0) * costs.SHUFFLE_PER_GB_STREAMING
    else:
      cost += metrics_dict.get(
          "TotalVcpuTime", 0.0) / 3600 * costs.VCPU_PER_HR_BATCH
      cost += metrics_dict.get(
          "TotalMemoryUsage", 0.0) / 1000 / 3600 * costs.MEM_PER_GB_HR_BATCH
      cost += metrics_dict.get(
          "TotalStreamingDataProcessed", 0.0) * costs.SHUFFLE_PER_GB_BATCH

    if self.gpu:
      rate = costs.ACCELERATOR_TO_COST[self.gpu]
      cost += metrics_dict.get("TotalGpuTime", 0.0) / 3600 * rate

    cost += metrics_dict.get("TotalPdUsage", 0.0) / 3600 * costs.PD_PER_GB_HR
    cost += metrics_dict.get(
        "TotalSsdUsage", 0.0) / 3600 * costs.PD_SSD_PER_GB_HR

    metrics_dict["EstimatedCost"] = cost
    return metrics_dict

  def _process_metrics_list(self,
                            metrics: list[MetricResult]) -> dict[str, Any]:
    system_metrics = {}
    for entry in metrics:
      metric_key = entry.key
      metric = metric_key.metric
      if metric_key.step == '' and metric.namespace == 'dataflow/v1b3':
        system_metrics[metric.name] = entry.committed or 0.0
    return system_metrics

  def _get_worker_time_interval(
      self, job_id: str) -> tuple[Optional[str], Optional[str]]:
    """Extracts worker start and stop times from job messages."""
    messages, _ = self.dataflow_client.list_messages(
      job_id=job_id,
      start_time=None,
      end_time=None,
      minimum_importance='JOB_MESSAGE_DETAILED')

    start_time, end_time = None, None
    for message in messages:
      text = message.messageText
      if text:
        if self.WORKER_START_PATTERN.match(text):
          start_time = message.time
        if self.WORKER_STOP_PATTERN.match(text):
          end_time = message.time

    return start_time, end_time

  def _get_throughput_metrics(
      self, project: str, job_id: str, start_time: str,
      end_time: str) -> dict[str, float]:
    interval = monitoring_v3.TimeInterval(
        start_time=start_time, end_time=end_time)
    aggregation = monitoring_v3.Aggregation(
        alignment_period=Duration(seconds=60),
        per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_MEAN)

    requests = {
        "Bytes": monitoring_v3.ListTimeSeriesRequest(
            name=f"projects/{project}",
            filter=f'metric.type='
            f'"dataflow.googleapis.com/job/estimated_bytes_produced_count" '
            f'AND metric.labels.job_id='
            f'"{job_id}" AND metric.labels.pcollection="{self.pcollection}"',
            interval=interval,
            aggregation=aggregation),
        "Elements": monitoring_v3.ListTimeSeriesRequest(
            name=f"projects/{project}",
            filter=f'metric.type="dataflow.googleapis.com/job/element_count" '
            f'AND metric.labels.job_id="{job_id}" '
            f'AND metric.labels.pcollection="{self.pcollection}"',
            interval=interval,
            aggregation=aggregation)
    }

    metrics = {}
    for key, req in requests.items():
      time_series = self.monitoring_client.list_time_series(request=req)
      values = [
          point.value.double_value for series in time_series
          for point in series.points
      ]
      metrics[f"AvgThroughput{key}"] = sum(values) / len(
          values) if values else 0.0

    return metrics

  def _get_job_runtime(self, start_time: str, end_time: str) -> float:
    """Calculates the job runtime duration in seconds."""
    start_dt = datetime.fromisoformat(start_time[:-1])
    end_dt = datetime.fromisoformat(end_time[:-1])
    return (end_dt - start_dt).total_seconds()

  def _get_additional_metrics(self,
                              result: DataflowPipelineResult) -> dict[str, Any]:
    job_id = result.job_id()
    job = self.dataflow_client.get_job(job_id)
    project = job.projectId
    start_time, end_time = self._get_worker_time_interval(job_id)
    if not start_time or not end_time:
      logging.warning('Could not find valid worker start/end times.')
      return {}

    throughput_metrics = self._get_throughput_metrics(
        project, job_id, start_time, end_time)
    return {
        **throughput_metrics,
        "JobRuntimeSeconds": self._get_job_runtime(start_time, end_time),
    }
