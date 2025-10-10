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

from apache_beam.examples.inference import vllm_gemma_batch
from apache_beam.testing.load_tests.dataflow_cost_benchmark import DataflowCostBenchmark


class VllmGemmaBenchmarkTest(DataflowCostBenchmark):
  def __init__(self):
    self.metrics_namespace = "BeamML_vLLM"
    super().__init__(
        metrics_namespace=self.metrics_namespace,
        pcollection="WriteBQ.out0",
    )

  def test(self):
    # The perf-test framework passes --input_file,
    # but the pipeline expects --input.
    extra_opts = {"input": self.pipeline.get_option("input_file")}

    self.result = vllm_gemma_batch.run(
        self.pipeline.get_full_options_as_args(**extra_opts),
        test_pipeline=self.pipeline)


if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  VllmGemmaBenchmarkTest().run()
