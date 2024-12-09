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
import time
from typing import Any
from typing import Optional

import apache_beam.testing.load_tests.dataflow_cost_consts as costs
from apache_beam.metrics.execution import MetricResult
from apache_beam.runners.dataflow.dataflow_runner import DataflowPipelineResult
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
  def __init__(
      self,
      metrics_namespace: Optional[str] = None,
      is_streaming: bool = False,
      gpu: Optional[costs.Accelerator] = None):
    self.is_streaming = is_streaming
    self.gpu = gpu
    super().__init__(metrics_namespace=metrics_namespace)

  def run(self):
    try:
      self.test()
      if not hasattr(self, 'result'):
        self.result = self.pipeline.run()
        # Defaults to waiting forever unless timeout has been set
        state = self.result.wait_until_finish(duration=self.timeout_ms)
        assert state != PipelineState.FAILED
      logging.info(
          'Pipeline complete, sleeping for 4 minutes to allow resource '
          'metrics to populate.')
      time.sleep(240)
      self.extra_metrics = self._retrieve_cost_metrics(self.result)
      self._metrics_monitor.publish_metrics(self.result, self.extra_metrics)
    finally:
      self.cleanup()

  def _retrieve_cost_metrics(self,
                             result: DataflowPipelineResult) -> dict[str, Any]:
    job_id = result.job_id()
    metrics = result.metrics().all_metrics(job_id)
    metrics_dict = self._process_metrics_list(metrics)
    logging.info(metrics_dict)
    cost = 0.0
    if (self.is_streaming):
      cost += metrics_dict.get(
          "TotalVcpuTime", 0.0) / 3600 * costs.VCPU_PER_HR_STREAMING
      cost += (
          metrics_dict.get("TotalMemoryUsage", 0.0) /
          1000) / 3600 * costs.MEM_PER_GB_HR_STREAMING
      cost += metrics_dict.get(
          "TotalStreamingDataProcessed", 0.0) * costs.SHUFFLE_PER_GB_STREAMING
    else:
      cost += metrics_dict.get(
          "TotalVcpuTime", 0.0) / 3600 * costs.VCPU_PER_HR_BATCH
      cost += (
          metrics_dict.get("TotalMemoryUsage", 0.0) /
          1000) / 3600 * costs.MEM_PER_GB_HR_BATCH
      cost += metrics_dict.get(
          "TotalStreamingDataProcessed", 0.0) * costs.SHUFFLE_PER_GB_BATCH
    if (self.gpu):
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
        if entry.committed is None:
          entry.committed = 0.0
        system_metrics[metric.name] = entry.committed
    return system_metrics
