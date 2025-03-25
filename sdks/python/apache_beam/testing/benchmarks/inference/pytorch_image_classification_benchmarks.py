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

from apache_beam.examples.inference import pytorch_image_classification
from apache_beam.testing.load_tests.dataflow_cost_benchmark import DataflowCostBenchmark
from torchvision import models

_PERF_TEST_MODELS = ['resnet50', 'resnet101', 'resnet152']
_PRETRAINED_MODEL_MODULE = 'torchvision.models'


class PytorchVisionBenchmarkTest(DataflowCostBenchmark):
  def __init__(self):
    # TODO (https://github.com/apache/beam/issues/23008)
    #  make get_namespace() method in RunInference static
    self.metrics_namespace = 'BeamML_PyTorch'
    super().__init__(
        metrics_namespace=self.metrics_namespace,
        pcollection='PyTorchRunInference/BeamML_RunInference_Postprocess-0.out0'
    )

  def test(self):
    pretrained_model_name = self.pipeline.get_option('pretrained_model_name')
    if not pretrained_model_name:
      raise RuntimeError(
          'Please provide a pretrained torch model name.'
          ' Model name must be from the module torchvision.models')
    if pretrained_model_name == _PERF_TEST_MODELS[0]:
      model_class = models.resnet50
    elif pretrained_model_name == _PERF_TEST_MODELS[1]:
      model_class = models.resnet101
    elif pretrained_model_name == _PERF_TEST_MODELS[2]:
      model_class = models.resnet152
    else:
      raise NotImplementedError

    # model_params are same for all the models. But this may change if we add
    # different models.
    model_params = {'num_classes': 1000, 'pretrained': False}

    extra_opts = {}
    extra_opts['input'] = self.pipeline.get_option('input_file')
    device = self.pipeline.get_option('device')
    self.result = pytorch_image_classification.run(
        self.pipeline.get_full_options_as_args(**extra_opts),
        model_class=model_class,
        model_params=model_params,
        test_pipeline=self.pipeline,
        device=device)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  PytorchVisionBenchmarkTest().run()
