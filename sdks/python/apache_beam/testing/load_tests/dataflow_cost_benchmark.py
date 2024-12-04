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

import time
from apache_beam.testing.load_tests.dataflow_cost_consts import *

from typing import Optional

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
  """
  def __init__(
      self,
      metrics_namespace: Optional[str] = None,
      is_streaming: bool = False):
    self.is_streaming = is_streaming
    super().__init__(metrics_namespace=metrics_namespace)

  def run(self):
    try:
      self.test()
      if not hasattr(self, 'result'):
        self.result = self.pipeline.run()
        # Defaults to waiting forever unless timeout has been set
        state = self.result.wait_until_finish(duration=self.timeout_ms)
        assert state != PipelineState.FAILED
      # Match sleep in Java implementation for resource metrics to populate
      time.sleep(240)
      self.extra_metrics = self._retrieve_cost_metrics(self.result)
      self._metrics_monitor.publish_metrics(self.result, self.extra_metrics)
    finally:
      self.cleanup()

  def _retrive_cost_metrics(self,
                            result: DataflowPipelineResult) -> dict[str, float]:
    job_id = result.job_id()
    metrics = result.metrics().all_metrics(job_id)
    print(metrics)
    cost = 0.0
    if (self.is_streaming):
      cost += metrics.get("TotalVcpuTime") / 3600 * VCPU_PER_HR_STREAMING
      cost += (
          metrics.get("TotalMemoryUsage") /
          1000) / 3600 * MEM_PER_GB_HR_STREAMING
      cost += metrics.get(
          "TotalStreamingDataProcessed") * SHUFFLE_PER_GB_STREAMING
    else:
      cost += metrics.get("TotalVcpuTime") / 3600 * VCPU_PER_HR_BATCH
      cost += (
          metrics.get("TotalMemoryUsage") / 1000) / 3600 * MEM_PER_GB_HR_BATCH
      cost += metrics.get("TotalStreamingDataProcessed") * SHUFFLE_PER_GB_BATCH
    cost += metrics.get("TotalPdUsage") / 3600 * PD_PER_GB_HR
    cost += metrics.get("TotalSsdUsage") / 3600 * PD_SSD_PER_GB_HR
    return {"EstimatedCost", cost}
