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

from apache_beam.examples.inference import tensorflow_mnist_classification
from apache_beam.testing.load_tests.dataflow_cost_benchmark import DataflowCostBenchmark


class TensorflowMNISTClassificationCostBenchmark(DataflowCostBenchmark):
  def __init__(self):
    super().__init__()

  def test(self):
    extra_opts = {}
    extra_opts['input'] = self.pipeline.get_option('input_file')
    extra_opts['output'] = self.pipeline.get_option('output_file')
    extra_opts['model_path'] = self.pipeline.get_option('model')
    tensorflow_mnist_classification.run(
        self.pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  TensorflowMNISTClassificationCostBenchmark().run()
