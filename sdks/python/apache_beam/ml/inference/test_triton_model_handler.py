import unittest
from unittest.mock import MagicMock, patch
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.vertex_ai_inference import VertexAITritonModelHandler
import json
from google.cloud import aiplatform
from google.cloud.aiplatform.gapic import PredictionServiceClient

class TestVertexAITritonModelHandler(unittest.TestCase):
    def setUp(self):
        """Initialize the handler with test parameters before each test."""
        self.handler = VertexAITritonModelHandler(
            project_id="test-project",
            region="us-central1",
            endpoint_name="test-endpoint",
            name="input__0",
            location="us-central1",
            datatype="FP32",
            private=False,
        )
        self.handler.throttler = MagicMock()
        self.handler.throttler.throttle_request = MagicMock(return_value=False)
        self.handler.throttled_secs = MagicMock()

    def test_load_model_public_endpoint(self):
        """Test loading a public endpoint and verifying deployed models."""
        mock_endpoint = MagicMock(spec=aiplatform.Endpoint)
        mock_endpoint.list_models.return_value = [MagicMock()]
        with patch('google.cloud.aiplatform.Endpoint', return_value=mock_endpoint):
            model = self.handler.load_model()
            self.assertEqual(model, mock_endpoint)
            mock_endpoint.list_models.assert_called_once()

    def test_load_model_no_deployed_models(self):
        """Test that an endpoint with no deployed models raises ValueError."""
        mock_endpoint = MagicMock(spec=aiplatform.Endpoint)
        mock_endpoint.list_models.return_value = []
        with patch('google.cloud.aiplatform.Endpoint', return_value=mock_endpoint):
            with self.assertRaises(ValueError) as cm:
                self.handler.load_model()
            self.assertIn("no models deployed", str(cm.exception))

    def test_get_request_payload_scalar(self):
        """Test payload construction for a batch of scalar inputs."""
        batch = [1.0, 2.0, 3.0]
        expected_payload = {
            "inputs": [
                {
                    "name": "input__0",
                    "shape": [3, 1],
                    "datatype": "FP32",
                    "data": [1.0, 2.0, 3.0],
                }
            ]
        }
        model = MagicMock(resource_name="test-resource")
        mock_client = MagicMock(spec=PredictionServiceClient)
        mock_response = MagicMock()
        mock_response.data.decode.return_value = json.dumps({"outputs": [{"data": [0.5, 0.6, 0.7]}]})
        mock_client.raw_predict.return_value = mock_response

        with patch('google.cloud.aiplatform.gapic.PredictionServiceClient', return_value=mock_client):
            self.handler.get_request(batch, model, throttle_delay_secs=5, inference_args=None)
            request = mock_client.raw_predict.call_args[1]["request"]
            self.assertEqual(json.loads(request.http_body.data.decode("utf-8")), expected_payload)

    def test_run_inference_parse_response(self):
        """Test parsing of a Triton response into PredictionResult objects."""
        batch = [1.0, 2.0]
        mock_response = {
            "outputs": [
                {
                    "name": "output__0",
                    "shape": [2, 1],
                    "datatype": "FP32",
                    "data": [0.5, 0.6],
                }
            ]
        }
        model = MagicMock(resource_name="test-resource", deployed_model_id="model-123")
        with patch.object(self.handler, 'get_request', return_value=mock_response):
            results = self.handler.run_inference(batch, model)
            expected_results = [
                PredictionResult(example=1.0, inference=[0.5], model_id="model-123"),
                PredictionResult(example=2.0, inference=[0.6], model_id="model-123"),
            ]
            self.assertEqual(list(results), expected_results)

    def test_run_inference_empty_batch(self):
        """Test that an empty batch returns an empty list."""
        batch = []
        model = MagicMock()
        results = self.handler.run_inference(batch, model)
        self.assertEqual(list(results), [])

    def test_run_inference_malformed_response(self):
        """Test that a malformed response raises an error."""
        batch = [1.0]
        mock_response = {"unexpected": "data"}
        model = MagicMock()
        with patch.object(self.handler, 'get_request', return_value=mock_response):
            with self.assertRaises(ValueError) as cm:
                list(self.handler.run_inference(batch, model))
            self.assertIn("no outputs found", str(cm.exception))

    def test_throttling_delays_request(self):
        """Test that the handler delays requests when throttled."""
        batch = [1.0]
        model = MagicMock(resource_name="test-resource")
        self.handler.throttler.throttle_request = MagicMock(side_effect=[True, False])
        mock_response = {"outputs": [{"data": [0.5]}]}

        with patch('time.sleep') as mock_sleep:
            with patch('google.cloud.aiplatform.gapic.PredictionServiceClient') as mock_client:
                mock_client.return_value.raw_predict.return_value.data.decode.return_value = json.dumps(mock_response)
                self.handler.run_inference(batch, model)
                mock_sleep.assert_called_with(5)
                self.handler.throttled_secs.inc.assert_called_with(5)

if __name__ == "__main__":
    unittest.main()