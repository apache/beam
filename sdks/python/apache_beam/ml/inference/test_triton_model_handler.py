import unittest
from unittest.mock import patch, MagicMock, ANY, call
import json
from google.cloud import aiplatform
from apache_beam.ml.inference.vertex_ai_inference import VertexAITritonModelHandler
from apache_beam.ml.inference import utils 
from apache_beam.ml.inference.base import PredictionResult
import numpy as np
import base64 


@patch('google.cloud.aiplatform.init')
@patch('google.auth.default', return_value=(MagicMock(), "test-project-id"))
class TestVertexAITritonModelHandler(unittest.TestCase):
    """Unit tests for the VertexAITritonModelHandler class."""
    def test_init_private_without_network(self, mock_auth_default, mock_aiplatform_init):
        """Test that ValueError is raised when private=True but network is not provided."""
        with self.assertRaisesRegex(ValueError, "A VPC network must be provided for a private endpoint."):
            VertexAITritonModelHandler(
                endpoint_id="123",
                project="test-project",
                location="us-central1",
                input_name="input",
                datatype="FP32",
                private=True 
            )
        mock_aiplatform_init.assert_not_called()


    @patch('google.cloud.aiplatform.Endpoint')
    def test_init_public_endpoint_success(self, mock_endpoint_class, mock_auth_default, mock_aiplatform_init):
        """Test successful initialization with a public endpoint."""
        mock_endpoint_instance = MagicMock()
        mock_endpoint_instance.name = "projects/test-project/locations/us-central1/endpoints/123"
        mock_endpoint_instance.list_models.return_value = [MagicMock()]
        mock_endpoint_class.return_value = mock_endpoint_instance

        handler = VertexAITritonModelHandler(
            endpoint_id="ep123",
            project="proj-a",
            location="loc-1",
            input_name="input0",
            datatype="FP32",
            private=False,
            experiment="exp-label",
            network="ignored-for-public"
        )

        mock_aiplatform_init.assert_called_once_with(
            project="proj-a",
            location="loc-1",
            experiment="exp-label",
            network="ignored-for-public" 
        )

        
        mock_endpoint_class.assert_called_once_with(
            endpoint_name="ep123",
            project="proj-a",
            location="loc-1"
          
        )
        
        mock_endpoint_instance.list_models.assert_called_once()
        

   
    @patch('google.cloud.aiplatform.PrivateEndpoint')
    def test_init_private_endpoint_success(self, mock_private_endpoint_class, mock_auth_default, mock_aiplatform_init):
        """Test successful initialization with a private endpoint."""
        mock_private_endpoint_instance = MagicMock()
        mock_private_endpoint_instance.name = "projects/test-project/locations/us-central1/privateEndpoints/123"
        mock_private_endpoint_instance.list_models.return_value = [MagicMock()] 
        mock_private_endpoint_class.return_value = mock_private_endpoint_instance

        handler = VertexAITritonModelHandler(
            endpoint_id="ep456",
            project="proj-b",
            location="loc-2",
            input_name="input1",
            datatype="BYTES",
            private=True,
            network="vpc-net"
        )

        
        mock_aiplatform_init.assert_called_once_with(
            project="proj-b",
            location="loc-2",
            experiment=None, 
            network="vpc-net"
        )

        
        mock_private_endpoint_class.assert_called_once_with(
            endpoint_name="ep456",
            project="proj-b",
            location="loc-2"
        )
        mock_private_endpoint_instance.list_models.assert_called_once()
        self.assertIsNotNone(handler.endpoint)



    @patch('google.cloud.aiplatform.Endpoint')
    def test_init_endpoint_no_models(self, mock_endpoint_class, mock_auth_default, mock_aiplatform_init):
        """Test that ValueError is raised during __init__ when no models are deployed."""
        mock_endpoint_instance = MagicMock()
        mock_endpoint_instance.name = "projects/test-project/locations/us-central1/endpoints/123"
        mock_endpoint_instance.list_models.return_value = [] 
        mock_endpoint_class.return_value = mock_endpoint_instance

        with self.assertRaisesRegex(ValueError, "has no models deployed to it"):
            VertexAITritonModelHandler(
                endpoint_id="123",
                project="test-project",
                location="us-central1",
                input_name="input",
                datatype="FP32"
            )

        mock_aiplatform_init.assert_called_once_with(
            project="test-project",
            location="us-central1",
            experiment=None,
            network=None
        )

        mock_endpoint_class.assert_called_once_with(
             endpoint_name="123", project="test-project", location="us-central1"
        )
        mock_endpoint_instance.list_models.assert_called_once()

    @patch('google.cloud.aiplatform.Endpoint')
    def test_create_client(self, mock_endpoint_class, mock_auth_default, mock_aiplatform_init):
        """Test that create_client returns the endpoint instance stored during init."""
        mock_endpoint_instance = MagicMock()
        mock_endpoint_instance.name = "projects/test-project/locations/us-central1/endpoints/123"
        mock_endpoint_instance.list_models.return_value = [MagicMock()]

        mock_endpoint_class.return_value = mock_endpoint_instance

        handler = VertexAITritonModelHandler( 
            endpoint_id="123",
            project="test-project",
            location="us-central1",
            input_name="input",
            datatype="FP32"
        )
        
    
        self.assertEqual(handler.endpoint, mock_endpoint_instance)

        
        mock_endpoint_class.reset_mock()
        mock_endpoint_instance.reset_mock() 
        
        client = handler.create_client()

        
        self.assertEqual(client, mock_endpoint_instance)

        
        mock_endpoint_class.assert_not_called()

        
        mock_endpoint_instance.list_models.assert_not_called()


    

    
    def _setup_request_mocks(self, mock_endpoint_class, mock_prediction_client_class):
        
        mock_endpoint_instance = MagicMock()
        mock_endpoint_instance.name = "projects/test-project/locations/us-central1/endpoints/123"
        mock_endpoint_instance.list_models.return_value = [MagicMock()]
        
        mock_endpoint_instance.deployed_model_id = "deployed_model_abc"
        mock_endpoint_class.return_value = mock_endpoint_instance

        mock_pred_client_instance = MagicMock()
        mock_predict_response = MagicMock() 
        mock_predict_response.raw_response.data = b'' 
        mock_pred_client_instance.raw_predict.return_value = mock_predict_response
        mock_prediction_client_class.return_value = mock_pred_client_instance

        return mock_endpoint_instance, mock_pred_client_instance, mock_predict_response


    @patch('apache_beam.ml.inference.utils._convert_to_result')
    @patch('google.cloud.aiplatform.gapic.PredictionServiceClient')
    @patch('google.cloud.aiplatform.Endpoint')
    def test_request_scalar_inputs(self, mock_endpoint_class, mock_prediction_client_class, mock_convert_to_result, mock_auth_default, mock_aiplatform_init):
        """Test request method with scalar inputs (FP32 datatype)."""
        mock_endpoint_instance, mock_pred_client_instance, mock_predict_response = \
            self._setup_request_mocks(mock_endpoint_class, mock_prediction_client_class)
        
        output_data = [0.5, 0.6]
      
        response_dict = {"outputs": [{"name": "output_name", "datatype": "FP32", "shape": [2], "data": output_data}]} 
        mock_predict_response.raw_response.data = json.dumps(response_dict).encode('utf-8')

        expected_results = [PredictionResult(ex, pred) for ex, pred in zip([1.0, 2.0], output_data)]
        mock_convert_to_result.return_value = expected_results

        handler = VertexAITritonModelHandler(
            endpoint_id="123",
            project="test-project",
            location="us-central1",
            input_name="input_fp32",
            datatype="FP32"
        )

        batch = [1.0, 2.0] 
        endpoint_instance = handler.create_client()  
        results = list(handler.request(batch, model=endpoint_instance))

        expected_client_options = {"api_endpoint": "us-central1-aiplatform.googleapis.com"}
        mock_prediction_client_class.assert_called_once_with(client_options=expected_client_options)

        expected_endpoint_path = "projects/test-project/locations/us-central1/endpoints/123"
        mock_pred_client_instance.raw_predict.assert_called_once()
        call_args, call_kwargs = mock_pred_client_instance.raw_predict.call_args
        self.assertEqual(call_kwargs['endpoint'], expected_endpoint_path)

        request_body = json.loads(call_kwargs['http_body'].decode('utf-8'))
        expected_request_body = {
            "inputs": [
                {
                    "name": "input_fp32",
                    "shape": [2], 
                    "datatype": "FP32",
                    "data": [1.0, 2.0], 
                }
            ]
        }
        self.assertEqual(request_body, expected_request_body)

        mock_convert_to_result.assert_called_once_with(
            batch, 
            output_data, 
            "deployed_model_abc" 
        )

        self.assertEqual(results, expected_results)


    @patch('apache_beam.ml.inference.utils._convert_to_result')
    @patch('google.cloud.aiplatform.gapic.PredictionServiceClient')
    @patch('google.cloud.aiplatform.Endpoint')
    def test_request_bytes_inputs(self, mock_endpoint_class, mock_prediction_client_class, mock_convert_to_result, mock_auth_default, mock_aiplatform_init):
        """Test request method with string inputs (BYTES datatype)."""
        mock_endpoint_instance, mock_pred_client_instance, mock_predict_response = \
            self._setup_request_mocks(mock_endpoint_class, mock_prediction_client_class)

        
        output_strings = ["response1", "response2"]
        output_data_b64 = [base64.b64encode(s.encode('utf-8')).decode('utf-8') for s in output_strings]
        response_dict = {"outputs": [{"name": "output_bytes", "datatype": "BYTES", "shape": [2], "data": output_data_b64}]}
        mock_predict_response.raw_response.data = json.dumps(response_dict).encode('utf-8')

        expected_results = [PredictionResult(ex, pred) for ex, pred in zip(["text1", "text2"], output_data_b64)]
        mock_convert_to_result.return_value = expected_results

        handler = VertexAITritonModelHandler(
            endpoint_id="123",
            project="test-project",
            location="us-central1",
            input_name="input_bytes",
            datatype="BYTES"
        )

        batch = ["text1", "text2"]
        endpoint_instance = handler.create_client() 
        results = list(handler.request(batch, model=endpoint_instance))

        expected_client_options = {"api_endpoint": "us-central1-aiplatform.googleapis.com"}
        mock_prediction_client_class.assert_called_once_with(client_options=expected_client_options)

        mock_pred_client_instance.raw_predict.assert_called_once()
        call_args, call_kwargs = mock_pred_client_instance.raw_predict.call_args
        self.assertEqual(call_kwargs['endpoint'], "projects/test-project/locations/us-central1/endpoints/123")


        request_body = json.loads(call_kwargs['http_body'].decode('utf-8'))
        expected_request_body = {
            "inputs": [
                {
                    "name": "input_bytes",
                    "shape": [2], 
                    "datatype": "BYTES",
                    "data": ["text1", "text2"],
                }
            ]
        }
        self.assertEqual(request_body, expected_request_body)

        mock_convert_to_result.assert_called_once_with(
            batch, 
            output_data_b64, 
            "deployed_model_abc"
        )

        self.assertEqual(results, expected_results)


    @patch('apache_beam.ml.inference.utils._convert_to_result')
    @patch('google.cloud.aiplatform.gapic.PredictionServiceClient')
    @patch('google.cloud.aiplatform.Endpoint')
    def test_request_ndarray_inputs(self, mock_endpoint_class, mock_prediction_client_class, mock_convert_to_result, mock_auth_default, mock_aiplatform_init):
        """Test request method with ndarray inputs (FP32 datatype)."""
        mock_endpoint_instance, mock_pred_client_instance, mock_predict_response = \
            self._setup_request_mocks(mock_endpoint_class, mock_prediction_client_class)


        output_data = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6]
        response_dict = {"outputs": [{"name": "output_name", "datatype": "FP32", "shape": [len(output_data)], "data": output_data}]}
        mock_predict_response.raw_response.data = json.dumps(response_dict).encode('utf-8')

        input_batch = [np.array([[1.0, 2.0], [3.0, 4.0]]), np.array([[5.0, 6.0], [7.0, 8.0]])]
        expected_results = [PredictionResult(input_batch[0], [output_data[0], output_data[1], output_data[2]]), 
                            PredictionResult(input_batch[1], [output_data[3], output_data[4], output_data[5]])] # Adjust expected structure if needed
        mock_convert_to_result.return_value = expected_results 

        handler = VertexAITritonModelHandler(
            endpoint_id="123",
            project="test-project",
            location="us-central1",
            input_name="input_fp32_ndarray",
            datatype="FP32"
        )

        endpoint_instance = handler.create_client() 
        results = list(handler.request(input_batch, model=endpoint_instance))

        expected_client_options = {"api_endpoint": "us-central1-aiplatform.googleapis.com"}
        mock_prediction_client_class.assert_called_once_with(client_options=expected_client_options)

        mock_pred_client_instance.raw_predict.assert_called_once()
        call_args, call_kwargs = mock_pred_client_instance.raw_predict.call_args
        self.assertEqual(call_kwargs['endpoint'], "projects/test-project/locations/us-central1/endpoints/123")

        request_body = json.loads(call_kwargs['http_body'].decode('utf-8'))
        expected_request_body = {
            "inputs": [
                {
                    "name": "input_fp32_ndarray",
                    "shape": [2, 2, 2], 
                    "datatype": "FP32",
                    "data": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
                }
            ]
        }
        self.assertEqual(request_body, expected_request_body)
        mock_convert_to_result.assert_called_once_with(
            input_batch, 
            output_data,
            "deployed_model_abc"
        )
        self.assertEqual(results, expected_results)

    @patch('google.cloud.aiplatform.Endpoint') 
    def test_request_unsupported_input(self, mock_endpoint_class, mock_auth_default, mock_aiplatform_init):
        """Test that ValueError is raised by request for unsupported input types in the batch."""
        mock_endpoint_instance = MagicMock()
        mock_endpoint_instance.name = "projects/test-project/locations/us-central1/endpoints/123"
        mock_endpoint_instance.list_models.return_value = [MagicMock()]
        mock_endpoint_class.return_value = mock_endpoint_instance

        handler = VertexAITritonModelHandler(
            endpoint_id="123",
            project="test-project",
            location="us-central1",
            input_name="input",
            datatype="FP32" 
        )
        batch = [{"key": "value"}]  
        with self.assertRaisesRegex(ValueError, "Unsupported input type"):
            list(handler.request(batch, model=handler.endpoint))

    def test_batch_elements_kwargs(self, mock_auth_default, mock_aiplatform_init):
        """ Test that batch_elements_kwargs returns correct batching params """
        with patch('google.cloud.aiplatform.Endpoint') as mock_endpoint_class:
             mock_endpoint_instance = MagicMock()
             mock_endpoint_instance.list_models.return_value = [MagicMock()]
             mock_endpoint_class.return_value = mock_endpoint_instance

             handler = VertexAITritonModelHandler(
                 endpoint_id="123",
                 project="test-project",
                 location="us-central1",
                 input_name="input",
                 datatype="FP32",
                 min_batch_size=10,
                 max_batch_size=100,
                 max_batch_duration_secs=5
             )
             kwargs = handler.batch_elements_kwargs()
             expected_kwargs = {
                 "min_batch_size": 10,
                 "max_batch_size": 100,
                 "max_batch_duration_secs": 5,
             }
             self.assertEqual(kwargs, expected_kwargs)


if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)