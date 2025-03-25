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

import logging

from apache_beam.examples.inference import pytorch_language_modeling
from apache_beam.testing.load_tests.dataflow_cost_benchmark import DataflowCostBenchmark


class PytorchLanguageModelingBenchmarkTest(DataflowCostBenchmark):
  def __init__(self):
    # TODO (https://github.com/apache/beam/issues/23008):
    #  make get_namespace() method in RunInference static
    self.metrics_namespace = 'BeamML_PyTorch'
    super().__init__(metrics_namespace=self.metrics_namespace)

  def test(self):
    extra_opts = {}
    extra_opts['input'] = self.pipeline.get_option('input_file')
    self.result = pytorch_language_modeling.run(
        self.pipeline.get_full_options_as_args(**extra_opts),
        test_pipeline=self.pipeline)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  PytorchLanguageModelingBenchmarkTest().run()
