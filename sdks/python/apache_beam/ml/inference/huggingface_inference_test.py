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

import contextlib
import importlib.util
import shutil
import sys
import tempfile
import types
import unittest
from collections.abc import Iterable
from collections.abc import Sequence
from typing import Any
from typing import Optional
from typing import Union

import pytest
from parameterized import parameterized

import apache_beam as beam
from apache_beam.ml.inference import huggingface_inference
from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.huggingface_inference import HuggingFaceModelHandlerTensor
from apache_beam.ml.inference.huggingface_inference import HuggingFacePipelineModelHandler
from apache_beam.testing import test_utils

# pylint: disable=ungrouped-imports
try:
  import tensorflow as tf
  import torch
  from transformers import AutoModel

  from apache_beam.ml.inference.tensorflow_inference_test import FakeTFTensorModel
  from apache_beam.ml.inference.tensorflow_inference_test import _compare_tensor_prediction_result
except (ImportError, unittest.SkipTest):
  # tensorflow_inference_test raises SkipTest if its dependencies are missing.
  tf = None

_ML_FRAMEWORKS = ('tensorflow', 'torch', 'transformers')


def fake_inference_fn_tensor(
    batch: Sequence[Union['tf.Tensor', 'torch.Tensor']],
    model: Any,
    device,
    inference_args: dict[str, Any],
    model_id: Optional[str] = None) -> Iterable[PredictionResult]:
  predictions = model.predict(batch, **inference_args)
  return utils._convert_to_result(batch, predictions, model_id)


class FakeTorchModel:
  def predict(self, input: 'torch.Tensor'):
    return input


@pytest.mark.uses_transformers
@unittest.skipIf(tf is None, 'Transformers dependencies are not installed.')
class HuggingFaceInferenceTest(unittest.TestCase):
  def setUp(self) -> None:
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self) -> None:
    shutil.rmtree(self.tmpdir)

  def test_predict_tensor(self):
    fake_model = FakeTFTensorModel()
    inference_runner = HuggingFaceModelHandlerTensor(
        model_uri='unused',
        model_class=AutoModel,
        inference_fn=fake_inference_fn_tensor)
    batched_examples = [tf.constant([1]), tf.constant([10]), tf.constant([100])]
    expected_predictions = [
        PredictionResult(ex, pred) for ex, pred in zip(
            batched_examples,
            [tf.math.multiply(n, 10) for n in batched_examples])
    ]

    inferences = inference_runner.run_inference(batched_examples, fake_model)
    for actual, expected in zip(inferences, expected_predictions):
      self.assertTrue(_compare_tensor_prediction_result(actual, expected))

  def test_predict_tensor_with_inference_args(self):
    fake_model = FakeTFTensorModel()
    inference_runner = HuggingFaceModelHandlerTensor(
        model_uri='unused',
        model_class=AutoModel,
        inference_fn=fake_inference_fn_tensor,
        inference_args={"add": True})
    batched_examples = [tf.constant([1]), tf.constant([10]), tf.constant([100])]
    expected_predictions = [
        PredictionResult(ex, pred) for ex, pred in zip(
            batched_examples, [
                tf.math.add(tf.math.multiply(n, 10), 10)
                for n in batched_examples
            ])
    ]

    inferences = inference_runner.run_inference(
        batched_examples, fake_model, inference_args={"add": True})

    for actual, expected in zip(inferences, expected_predictions):
      self.assertTrue(_compare_tensor_prediction_result(actual, expected))

  def test_framework_detection_torch(self):
    fake_model = FakeTorchModel()
    inference_runner = HuggingFaceModelHandlerTensor(
        model_uri='unused',
        model_class=AutoModel,
        inference_fn=fake_inference_fn_tensor)
    batched_examples = [torch.tensor(1), torch.tensor(10), torch.tensor(100)]
    inference_runner.run_inference(batched_examples, fake_model)
    self.assertEqual(inference_runner._framework, "pt")

  def test_framework_detection_tensorflow(self):
    fake_model = FakeTFTensorModel()
    inference_runner = HuggingFaceModelHandlerTensor(
        model_uri='unused',
        model_class=AutoModel,
        inference_fn=fake_inference_fn_tensor)
    batched_examples = [tf.constant(1), tf.constant(10), tf.constant(100)]
    inference_runner.run_inference(batched_examples, fake_model)
    self.assertEqual(inference_runner._framework, 'tf')

  def test_convert_to_result_batch_processing(self):
    """Test that utils._convert_to_result correctly handles 
    batches with multiple elements."""

    # Test case that reproduces the bug: batch size > 1
    batch = ["input1", "input2"]
    predictions = [{
        "translation_text": "output1"
    }, {
        "translation_text": "output2"
    }]

    results = list(utils._convert_to_result(batch, predictions))

    # Should return 2 results, not 1
    self.assertEqual(
        len(results), 2, "Should return one result per batch element")

    # Check that each result has the correct input and output
    self.assertEqual(results[0].example, "input1")
    self.assertEqual(results[0].inference, {"translation_text": "output1"})
    self.assertEqual(results[1].example, "input2")
    self.assertEqual(results[1].inference, {"translation_text": "output2"})


class _FakePipeline:
  """A stand-in for a transformers Pipeline, created by a fake pipeline()."""
  def __init__(self, **kwargs):
    self.kwargs = kwargs


def _fake_transformers_module():
  module = types.ModuleType('transformers')
  module.pipeline = _FakePipeline
  return module


def _fake_torch_module(cuda_available):
  module = types.ModuleType('torch')
  module.cuda = types.SimpleNamespace(is_available=lambda: cuda_available)
  return module


@contextlib.contextmanager
def _fake_modules(**fake_modules):
  """Replaces the given modules with fake ones."""
  # Also hides the real modules, if already imported, and restores them
  # afterwards.
  with test_utils.block_imports(*fake_modules):
    sys.modules.update(fake_modules)
    try:
      yield
    finally:
      for name in fake_modules:
        del sys.modules[name]


class HuggingFaceJobSubmissionTest(unittest.TestCase):
  """Tests that ML frameworks are only used when executing pipelines.

  tensorflow, torch and transformers should only be needed for loading models
  and running inference when the pipeline is executed (for example, on the
  workers), but not for constructing and submitting the pipeline. These tests
  don't require any of them to be installed.
  """
  def test_importing_module_does_not_import_ml_frameworks(self):
    # Executes the module in a new module object, i.e. without replacing the
    # already imported module.
    spec = importlib.util.find_spec(huggingface_inference.__name__)
    with test_utils.block_imports(*_ML_FRAMEWORKS) as attempted_imports:
      spec.loader.exec_module(importlib.util.module_from_spec(spec))
    self.assertEqual(attempted_imports, [])

  @parameterized.expand([('default', None), ('gpu', 'GPU'), ('cpu', 'CPU')])
  def test_pipeline_job_submission_does_not_use_ml_frameworks(
      self, unused_name, device):
    with test_utils.block_imports(*_ML_FRAMEWORKS) as attempted_imports:
      model_handler = HuggingFacePipelineModelHandler(
          task='text-classification',
          model='distilbert-base-uncased-finetuned-sst-2-english',
          device=device)
      pipeline = beam.Pipeline()
      _ = (
          pipeline
          | beam.Create(['I love Apache Beam!'])
          | RunInference(model_handler))
      # Serializes the pipeline, as done during job submission, and
      # deserializes it again. Deserializing fails if the pipeline references
      # (i.e. requires) a module that can't be imported.
      beam.Pipeline.from_runner_api(
          pipeline.to_runner_api(), pipeline.runner, pipeline.options)
    self.assertEqual(attempted_imports, [])

  @parameterized.expand([
      ('default_with_gpu', None, True, 'cuda:0'),
      ('default_without_gpu', None, False, 'cpu'),
      ('gpu_with_gpu', 'GPU', True, 'cuda:0'),
      ('gpu_without_gpu', 'GPU', False, 'cpu'),
      ('cpu_with_gpu', 'CPU', True, 'cpu'),
  ])
  def test_pipeline_device_is_determined_when_loading_model(
      self, unused_name, device, cuda_available, expected_device):
    # Whether a GPU is available is determined on the workers that load the
    # model, not on the machine that constructs the model handler (and submits
    # the pipeline). So the GPU availability is inverted for the latter.
    with _fake_modules(torch=_fake_torch_module(not cuda_available)):
      model_handler = HuggingFacePipelineModelHandler(
          task='text-classification', device=device)
    with _fake_modules(transformers=_fake_transformers_module(),
                       torch=_fake_torch_module(cuda_available)):
      pipeline = model_handler.load_model()
    self.assertEqual(
        pipeline.kwargs, {
            'task': 'text-classification',
            'model': '',
            'device': expected_device
        })

  def test_pipeline_load_pipeline_args_device_is_used(self):
    model_handler = HuggingFacePipelineModelHandler(
        task='text-classification', load_pipeline_args={'device': 'cuda:1'})
    with _fake_modules(transformers=_fake_transformers_module()):
      with test_utils.block_imports('torch') as attempted_imports:
        pipeline = model_handler.load_model()
    self.assertEqual(pipeline.kwargs['device'], 'cuda:1')
    self.assertEqual(attempted_imports, [])

  def test_pipeline_invalid_device(self):
    with self.assertRaisesRegex(ValueError, 'Invalid device value'):
      HuggingFacePipelineModelHandler(task='text-classification', device='TPU')

  def test_pipeline_device_specified_twice(self):
    with self.assertRaisesRegex(ValueError,
                                'specified in `load_pipeline_args`'):
      HuggingFacePipelineModelHandler(
          task='text-classification',
          device='GPU',
          load_pipeline_args={'device': 'cuda:0'})

  def test_pipeline_loading_model_requires_transformers(self):
    model_handler = HuggingFacePipelineModelHandler(
        task='text-classification', device='CPU')
    with test_utils.block_imports('transformers'):
      with self.assertRaisesRegex(ImportError, 'transformers is required'):
        model_handler.load_model()

  def test_framework_detection_does_not_import_tensorflow(self):
    with test_utils.block_imports('tensorflow') as attempted_imports:
      self.assertFalse(huggingface_inference._is_tensorflow_tensor([1]))
    self.assertEqual(attempted_imports, [])


if __name__ == '__main__':
  unittest.main()
