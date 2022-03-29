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

from collections import OrderedDict
import os
import shutil
import tempfile
import unittest

import numpy as np
import torch

import apache_beam as beam
from apache_beam.ml.inference import base
from apache_beam.ml.inference.pytorch_impl import PytorchModelLoader
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class PytorchLinearRegression(torch.nn.Module):
  def __init__(self, inputSize, outputSize):
    super().__init__()
    self.linear = torch.nn.Linear(inputSize, outputSize)

  def forward(self, x):
    out = self.linear(x)
    return out


class RunInferenceBaseTest(unittest.TestCase):
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def test_simple_single_feature(self):
    with TestPipeline() as pipeline:
      examples = torch.from_numpy(np.array([1, 5, 3, 10]).reshape(-1, 1))
      expected = [example * 2.0 + 0.5 for example in examples]

      state_dict = OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                                ('linear.bias', torch.Tensor([0.5]))])

      path = os.path.join(self.tmpdir, 'my_state_dict_path')
      torch.save(state_dict, path)

      input_dim = 1
      output_dim = 1

      model_loader = PytorchModelLoader(
          input_dim=input_dim,
          state_dict_path=path,
          model_class=PytorchLinearRegression(input_dim, output_dim))

      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | base.RunInference(model_loader)
      assert_that(actual, equal_to(expected))

  def test_simple_multiple_features(self):
    pass
