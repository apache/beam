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

import unittest
from unittest.mock import Mock, MagicMock, patch

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Protect against environments where tritonserver library is not available.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from apache_beam.ml.inference.base import PredictionResult, RunInference
  from apache_beam.ml.inference.TritonModelHandler import TritonModelHandler
  from apache_beam.ml.inference.TritonModelHandler import TritonModelWrapper
except ImportError:
  raise unittest.SkipTest('Triton dependencies are not installed')


class TritonModelHandlerTest(unittest.TestCase):
  def test_handler_initialization(self):
    """Test that handler initializes correctly with required parameters."""
    handler = TritonModelHandler(
        model_repository="/workspace/models", model_name="test_model")
    self.assertEqual(handler._model_repository, "/workspace/models")
    self.assertEqual(handler._model_name, "test_model")
    self.assertEqual(handler._input_tensor_name, "INPUT")
    self.assertEqual(handler._output_tensor_name, "OUTPUT")

  def test_handler_custom_tensor_names(self):
    """Test handler with custom tensor names."""
    handler = TritonModelHandler(
        model_repository="/workspace/models",
        model_name="test_model",
        input_tensor_name="custom_input",
        output_tensor_name="custom_output")
    self.assertEqual(handler._input_tensor_name, "custom_input")
    self.assertEqual(handler._output_tensor_name, "custom_output")

  def test_handler_missing_tritonserver(self):
    """Test that handler raises ImportError if tritonserver is not available."""
    with patch('apache_beam.ml.inference.TritonModelHandler.tritonserver',
               None):
      with self.assertRaises(ImportError):
        TritonModelHandler(
            model_repository="/workspace/models", model_name="test_model")

  @patch('apache_beam.ml.inference.TritonModelHandler.tritonserver')
  def test_load_model_success(self, mock_tritonserver):
    """Test successful model loading."""
    mock_server = Mock()
    mock_model = Mock()
    mock_tritonserver.Server.return_value = mock_server
    mock_server.model.return_value = mock_model

    handler = TritonModelHandler(
        model_repository="/workspace/models", model_name="test_model")
    wrapper = handler.load_model()

    self.assertIsInstance(wrapper, TritonModelWrapper)
    self.assertEqual(wrapper.server, mock_server)
    self.assertEqual(wrapper.model, mock_model)
    mock_server.start.assert_called_once()
    mock_server.model.assert_called_once_with("test_model")

  @patch('apache_beam.ml.inference.TritonModelHandler.tritonserver')
  def test_load_model_server_start_fails(self, mock_tritonserver):
    """Test model loading when server fails to start."""
    mock_tritonserver.Server.side_effect = Exception("Server start failed")

    handler = TritonModelHandler(
        model_repository="/workspace/models", model_name="test_model")

    with self.assertRaises(RuntimeError) as context:
      handler.load_model()
    self.assertIn("Failed to start Triton server", str(context.exception))

  @patch('apache_beam.ml.inference.TritonModelHandler.tritonserver')
  def test_load_model_model_not_found(self, mock_tritonserver):
    """Test model loading when model is not found."""
    mock_server = Mock()
    mock_tritonserver.Server.return_value = mock_server
    mock_server.model.return_value = None

    handler = TritonModelHandler(
        model_repository="/workspace/models", model_name="test_model")

    with self.assertRaises(RuntimeError) as context:
      handler.load_model()
    self.assertIn("Model 'test_model' not found", str(context.exception))
    mock_server.stop.assert_called_once()

  @patch('apache_beam.ml.inference.TritonModelHandler.tritonserver')
  def test_run_inference_success(self, mock_tritonserver):
    """Test successful inference."""
    # Setup mocks
    mock_model = Mock()
    mock_server = Mock()
    wrapper = TritonModelWrapper(server=mock_server, model=mock_model)

    mock_response = Mock()
    mock_output = Mock()
    mock_output.to_string_array.return_value.tolist.return_value = [
        '{"result": 1}', '{"result": 2}'
    ]
    mock_response.outputs = {"OUTPUT": mock_output}
    mock_model.infer.return_value = [mock_response]

    handler = TritonModelHandler(
        model_repository="/workspace/models", model_name="test_model")

    batch = ["input1", "input2"]
    results = list(handler.run_inference(batch, wrapper))

    self.assertEqual(len(results), 2)
    self.assertIsInstance(results[0], PredictionResult)
    mock_model.infer.assert_called_once_with(inputs={"INPUT": batch})

  @patch('apache_beam.ml.inference.TritonModelHandler.tritonserver')
  def test_run_inference_with_custom_tensor_names(self, mock_tritonserver):
    """Test inference with custom tensor names via inference_args."""
    mock_model = Mock()
    mock_server = Mock()
    wrapper = TritonModelWrapper(server=mock_server, model=mock_model)

    mock_response = Mock()
    mock_output = Mock()
    mock_output.to_string_array.return_value.tolist.return_value = [
        '{"result": 1}'
    ]
    mock_response.outputs = {"custom_out": mock_output}
    mock_model.infer.return_value = [mock_response]

    handler = TritonModelHandler(
        model_repository="/workspace/models", model_name="test_model")

    batch = ["input1"]
    inference_args = {
        'input_tensor_name': 'custom_in', 'output_tensor_name': 'custom_out'
    }
    results = list(handler.run_inference(batch, wrapper, inference_args))

    self.assertEqual(len(results), 1)
    mock_model.infer.assert_called_once_with(inputs={"custom_in": batch})

  @patch('apache_beam.ml.inference.TritonModelHandler.tritonserver')
  def test_run_inference_fails(self, mock_tritonserver):
    """Test inference failure handling."""
    mock_model = Mock()
    mock_server = Mock()
    wrapper = TritonModelWrapper(server=mock_server, model=mock_model)
    mock_model.infer.side_effect = Exception("Inference error")

    handler = TritonModelHandler(
        model_repository="/workspace/models", model_name="test_model")

    batch = ["input1"]
    with self.assertRaises(RuntimeError) as context:
      list(handler.run_inference(batch, wrapper))
    self.assertIn("Triton inference failed", str(context.exception))

  @patch('apache_beam.ml.inference.TritonModelHandler.tritonserver')
  def test_run_inference_output_not_found(self, mock_tritonserver):
    """Test error when expected output tensor is not in response."""
    mock_model = Mock()
    mock_server = Mock()
    wrapper = TritonModelWrapper(server=mock_server, model=mock_model)

    mock_response = Mock()
    mock_response.outputs = {}  # Empty outputs
    mock_model.infer.return_value = [mock_response]

    handler = TritonModelHandler(
        model_repository="/workspace/models", model_name="test_model")

    batch = ["input1"]
    with self.assertRaises(RuntimeError) as context:
      list(handler.run_inference(batch, wrapper))
    self.assertIn("Output tensor 'OUTPUT' not found", str(context.exception))

  @patch('apache_beam.ml.inference.TritonModelHandler.tritonserver')
  def test_custom_parse_function(self, mock_tritonserver):
    """Test using a custom output parsing function."""
    def custom_parser(outputs, output_name):
      return ["custom_parsed_result"]

    mock_model = Mock()
    mock_server = Mock()
    wrapper = TritonModelWrapper(server=mock_server, model=mock_model)

    mock_response = Mock()
    mock_response.outputs = {"OUTPUT": Mock()}
    mock_model.infer.return_value = [mock_response]

    handler = TritonModelHandler(
        model_repository="/workspace/models",
        model_name="test_model",
        parse_output_fn=custom_parser)

    batch = ["input1"]
    results = list(handler.run_inference(batch, wrapper))

    self.assertEqual(len(results), 1)
    self.assertEqual(results[0].inference, "custom_parsed_result")

  def test_get_metrics_namespace(self):
    """Test metrics namespace."""
    handler = TritonModelHandler(
        model_repository="/workspace/models", model_name="test_model")
    self.assertEqual(handler.get_metrics_namespace(), "BeamML_Triton")

  def test_wrapper_cleanup(self):
    """Test that TritonModelWrapper cleans up server on deletion."""
    mock_server = Mock()
    mock_model = Mock()

    wrapper = TritonModelWrapper(server=mock_server, model=mock_model)
    wrapper.__del__()

    mock_server.stop.assert_called_once()


if __name__ == '__main__':
  unittest.main()
