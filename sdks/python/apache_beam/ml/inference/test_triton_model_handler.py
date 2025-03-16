import json
import time

import pytest
from apache_beam.ml.inference.vertex_ai_inference import VertexAIModelHandlerJSON
from apache_beam.ml.inference.vertex_ai_inference import VertexAITritonModelHandler
from apache_beam.ml.inference.base import PredictionResult

# Define a fake endpoint class to simulate a live AI Platform endpoint.
class FakeEndpoint:
    def __init__(self):
        self.resource_name = "projects/test/locations/us-central1/endpoints/12345"
        self.deployed_model_id = "deployed_model_1"

    def list_models(self):
        return [{"dummy": "model"}]


# Define a fake PredictionServiceClient to simulate the behavior of the AI Platform client.
class FakePredictionServiceClient:
    def __init__(self, client_options=None):
        self.client_options = client_options

    def raw_predict(self, request):
        # Simulate a response. The true handler expects the response data to be a JSON
        # string containing an "outputs" field.
        fake_resp_content = {
            "outputs": [{"data": [["result1"], ["result2"]]}]
        }
        class FakeResponse:
            pass
        fake_response = FakeResponse()
        fake_response.data = json.dumps(fake_resp_content).encode("utf-8")
        return fake_response


@pytest.fixture(autouse=True)
def patch_prediction_service_client(monkeypatch):
    # Replace the PredictionServiceClient with our fake implementation.
    monkeypatch.setattr(
        "apache_beam.ml.inference.vertex_ai_inference.aiplatform.gapic.PredictionServiceClient",
        lambda client_options=None: FakePredictionServiceClient(client_options)
    )


@pytest.fixture(autouse=True)
def patch_retrieve_endpoint(monkeypatch):
    # Patch the _retrieve_endpoint() to always return our fake endpoint.
    monkeypatch.setattr(
        VertexAITritonModelHandler,
        "_retrieve_endpoint",
        lambda self: FakeEndpoint()
    )


@pytest.fixture(autouse=True)
def patch_convert_to_result(monkeypatch):
    # Override the conversion utility to a simple function for testing.
    from apache_beam.ml.inference import utils
    def fake_convert_to_result(batch, predictions, deployed_model_id):
        # In our simple conversion, we return a tuple: (input, prediction, deployed_model_id)
        return [(inp, pred, deployed_model_id) for inp, pred in zip(batch, predictions)]
    monkeypatch.setattr(utils, "_convert_to_result", fake_convert_to_result)


def test_get_request(monkeypatch):
    """
    Test that the get_request method constructs the request properly,
    calls the fake PredictionServiceClient, and returns the expected response data.
    """
    # Create a TritonModelHandler instance.
    handler = VertexAITritonModelHandler(
        project_id="test-project",
        region="us-central1",
        endpoint_name="12345",
        name="input_field",
        location="us-central1",
        datatype="BYTES"
    )

    # Create a fake endpoint to pass into get_request.
    fake_endpoint = FakeEndpoint()
    batch = ["test_input"]

    # Call get_request.
    response_data = handler.get_request(batch, fake_endpoint, throttle_delay_secs=1, inference_agrs={})

    # Validate the response structure.
    assert "outputs" in response_data
    outputs = response_data["outputs"]
    assert isinstance(outputs, list)
    assert "data" in outputs[0]
    # The fake client returns a list with two prediction results.
    assert outputs[0]["data"] == [["result1"], ["result2"]]


def test_run_inference(monkeypatch):
    """
    Test that run_inference converts the raw response into PredictionResult objects.
    """
    handler = VertexAITritonModelHandler(
        project_id="test-project",
        region="us-central1",
        endpoint_name="12345",
        name="input_field",
        location="us-central1",
        datatype="BYTES"
    )

    # load_model() is patched via _retrieve_endpoint so it returns our FakeEndpoint.
    model = handler.load_model()
    batch = ["input1", "input2"]

    # Call run_inference and collect the results.
    results = list(handler.run_inference(batch, model, inference_args={"payload_config": {}}))
    
    # With our fake response (2 predictions) and a batch of 2 inputs,
    # the conversion utility should yield 2 PredictionResult tuples.
    # For our fake_convert_to_result, each result is a tuple: (input, prediction, deployed_model_id).
    assert len(results) == len(batch)
    for inp, pred, deployed_id in results:
        assert inp in batch
        assert deployed_id == model.deployed_model_id 