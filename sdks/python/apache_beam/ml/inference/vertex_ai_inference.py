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
import json
import logging
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import Optional
from typing import Dict

from google.api_core.exceptions import ServerError
from google.api_core.exceptions import TooManyRequests
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import aiplatform

from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import PredictionResult
import numpy as np
MSEC_TO_SEC = 1000
from apache_beam.ml.inference.base import RemoteModelHandler

LOGGER = logging.getLogger("VertexAIModelHandlerJSON")

# pylint: disable=line-too-long


def _retry_on_appropriate_gcp_error(exception):
  """
  Retry filter that returns True if a returned HTTP error code is 5xx or 429.
  This is used to retry remote requests that fail, most notably 429
  (TooManyRequests.)

  Args:
    exception: the returned exception encountered during the request/response
      loop.

  Returns:
    boolean indication whether or not the exception is a Server Error (5xx) or
      a TooManyRequests (429) error.
  """
  return isinstance(exception, (TooManyRequests, ServerError))


class VertexAIModelHandlerJSON(RemoteModelHandler[Any,
                                                  PredictionResult,
                                                  aiplatform.Endpoint]):
  def __init__(
      self,
      endpoint_id: str,
      project: str,
      location: str,
      experiment: Optional[str] = None,
      network: Optional[str] = None,
      private: bool = False,
      *,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      **kwargs):
    """Implementation of the ModelHandler interface for Vertex AI.
    **NOTE:** This API and its implementation are under development and
    do not provide backward compatibility guarantees.
    Unlike other ModelHandler implementations, this does not load the model
    being used onto the worker and instead makes remote queries to a
    Vertex AI endpoint. In that way it functions more like a mid-pipeline
    IO. Public Vertex AI endpoints have a maximum request size of 1.5 MB.
    If you wish to make larger requests and use a private endpoint, provide
    the Compute Engine network you wish to use and set `private=True`

    Args:
      endpoint_id: the numerical ID of the Vertex AI endpoint to query
      project: the GCP project name where the endpoint is deployed
      location: the GCP location where the endpoint is deployed
      experiment: optional. experiment label to apply to the
        queries. See
        https://cloud.google.com/vertex-ai/docs/experiments/intro-vertex-ai-experiments
        for more information.
      network: optional. the full name of the Compute Engine
        network the endpoint is deployed on; used for private
        endpoints. The network or subnetwork Dataflow pipeline
        option must be set and match this network for pipeline
        execution.
        Ex: "projects/12345/global/networks/myVPC"
      private: optional. if the deployed Vertex AI endpoint is
        private, set to true. Requires a network to be provided
        as well.
      min_batch_size: optional. the minimum batch size to use when batching
        inputs.
      max_batch_size: optional. the maximum batch size to use when batching
        inputs.
      max_batch_duration_secs: optional. the maximum amount of time to buffer 
        a batch before emitting; used in streaming contexts.
    """
    self._batching_kwargs = {}
    self._env_vars = kwargs.get('env_vars', {})
    if min_batch_size is not None:
      self._batching_kwargs["min_batch_size"] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs["max_batch_size"] = max_batch_size
    if max_batch_duration_secs is not None:
      self._batching_kwargs["max_batch_duration_secs"] = max_batch_duration_secs

    if private and network is None:
      raise ValueError(
          "A VPC network must be provided to use a private endpoint.")

    # TODO: support the full list of options for aiplatform.init()
    # See https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform#google_cloud_aiplatform_init
    aiplatform.init(
        project=project,
        location=location,
        experiment=experiment,
        network=network)

    # Check for liveness here but don't try to actually store the endpoint
    # in the class yet
    self.endpoint_name = endpoint_id
    self.location = location
    self.is_private = private

    _ = self._retrieve_endpoint(
        self.endpoint_name, self.location, self.is_private)

    super().__init__(
        namespace='VertexAIModelHandlerJSON',
        retry_filter=_retry_on_appropriate_gcp_error,
        **kwargs)

  def _retrieve_endpoint(
      self, endpoint_id: str, location: str,
      is_private: bool) -> aiplatform.Endpoint:
    """Retrieves an AI Platform endpoint and queries it for liveness/deployed
    models.

    Args:
      endpoint_id: the numerical ID of the Vertex AI endpoint to retrieve.
      is_private: a boolean indicating if the Vertex AI endpoint is a private
        endpoint
    Returns:
      An aiplatform.Endpoint object
    Raises:
      ValueError: if endpoint is inactive or has no models deployed to it.
    """
    if is_private:
      endpoint: aiplatform.Endpoint = aiplatform.PrivateEndpoint(
          endpoint_name=endpoint_id, location=location)
      LOGGER.debug("Treating endpoint %s as private", endpoint_id)
    else:
      endpoint = aiplatform.Endpoint(
          endpoint_name=endpoint_id, location=location)
      LOGGER.debug("Treating endpoint %s as public", endpoint_id)

    try:
      mod_list = endpoint.list_models()
    except Exception as e:
      raise ValueError(
          "Failed to contact endpoint %s, got exception: %s", endpoint_id, e)

    if len(mod_list) == 0:
      raise ValueError("Endpoint %s has no models deployed to it.", endpoint_id)

    return endpoint

  def create_client(self) -> aiplatform.Endpoint:
    """Loads the Endpoint object used to build and send prediction request to
    Vertex AI.
    """
    # Check to make sure the endpoint is still active since pipeline
    # construction time
    ep = self._retrieve_endpoint(
        self.endpoint_name, self.location, self.is_private)
    return ep

  def request(
      self,
      batch: Sequence[Any],
      model: aiplatform.Endpoint,
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """ Sends a prediction request to a Vertex AI endpoint containing batch
    of inputs and matches that input with the prediction response from
    the endpoint as an iterable of PredictionResults.

    Args:
      batch: a sequence of any values to be passed to the Vertex AI endpoint.
        Should be encoded as the model expects.
      model: an aiplatform.Endpoint object configured to access the desired
        model.
      inference_args: any additional arguments to send as part of the
        prediction request.

    Returns:
      An iterable of Predictions.
    """
    prediction = model.predict(instances=list(batch), parameters=inference_args)
    return utils._convert_to_result(
        batch, prediction.predictions, prediction.deployed_model_id)

  def validate_inference_args(self, inference_args: Optional[dict[str, Any]]):
    pass

  def batch_elements_kwargs(self) -> Mapping[str, Any]:
    return self._batching_kwargs

class VertexAITritonModelHandler(RemoteModelHandler[Any, PredictionResult, aiplatform.Endpoint]):
    def __init__(
        self,
        endpoint_id: str,
        project: str,
        location: str,
        input_name: str,
        datatype: str,
        experiment: Optional[str] = None,
        network: Optional[str] = None,
        private: bool = False,
        min_batch_size: Optional[int] = None,
        max_batch_size: Optional[int] = None,
        max_batch_duration_secs: Optional[int] = None,
        **kwargs
    ):
        """
        Initialize the handler for Triton Inference Server on Vertex AI.

        Args:
            endpoint_id: Vertex AI endpoint ID.
            project: GCP project name.
            location: GCP location.
            input_name: Name of the input tensor for Triton.
            datatype: Data type of the input (e.g., 'FP32', 'BYTES').
            experiment: Optional experiment label.
            network: Optional VPC network for private endpoints.
            private: Whether the endpoint is private.
            min_batch_size: Minimum batch size for batching.
            max_batch_size: Maximum batch size for batching.
            max_batch_duration_secs: Max buffering time for batches.
            **kwargs: Additional arguments passed to the base class.
        """
        self.input_name = input_name
        self.datatype = datatype
        self._batching_kwargs = {}
        if min_batch_size is not None:
            self._batching_kwargs["min_batch_size"] = min_batch_size
        if max_batch_size is not None:
            self._batching_kwargs["max_batch_size"] = max_batch_size
        if max_batch_duration_secs is not None:
            self._batching_kwargs["max_batch_duration_secs"] = max_batch_duration_secs

        if private and network is None:
            raise ValueError("A VPC network must be provided for a private endpoint.")

        aiplatform.init(
            project=project,
            location=location,
            experiment=experiment,
            network=network
        )
        self.project = project
        self.endpoint_name = endpoint_id
        self.location = location
        self.is_private = private

        self.endpoint = self._retrieve_endpoint(  
            self.endpoint_name, self.project, self.location, self.is_private
        )

        super().__init__(
            namespace='VertexAITritonModelHandler',
            retry_filter=_retry_on_appropriate_gcp_error,
            **kwargs
        )
    def create_client(self) -> aiplatform.Endpoint:
        """
        Create the client for inference.

        Returns:
            aiplatform.Endpoint object.
        """
        return self.endpoint
    
    def _retrieve_endpoint(
        self, endpoint_id: str, project: str, location: str,
        is_private: bool) -> aiplatform.Endpoint:
        """Retrieves an AI Platform endpoint object. Includes liveness check."""
        
        endpoint_class = aiplatform.Endpoint
        log_msg = f"Treating endpoint {endpoint_id} as public"
        if is_private:
            endpoint_class = aiplatform.PrivateEndpoint
            log_msg = f"Treating endpoint {endpoint_id} as private"

        LOGGER.debug(log_msg)
        endpoint = endpoint_class(
            endpoint_name=endpoint_id, project=project, location=location)
        try:
            mod_list = endpoint.list_models()
            LOGGER.debug("Endpoint %s list_models() response: %s", endpoint.name, mod_list)
        except Exception as e:
            raise ValueError(
                f"Failed to contact endpoint {endpoint.name} or endpoint is invalid. "
                f"Error: {e}") from e

        if not mod_list:
            raise ValueError(f"Endpoint {endpoint.name} has no models deployed to it "
                             f"(according to Vertex AI).")
        return endpoint


    def request(
        self,
        batch: Sequence[Any],
        model: aiplatform.Endpoint,
        inference_args: Optional[Dict[str, Any]] = None
    ) -> Iterable[PredictionResult]:
        """
        Send a prediction request to the Triton endpoint.

        Args:
            batch: Sequence of inputs.
            model: Vertex AI endpoint object.
            inference_args: Optional inference arguments (ignored for Triton).

        Returns:
            Iterable of PredictionResult objects.

        Raises:
            ValueError: If input type is unsupported or response lacks outputs.
        """
      
        if self.datatype == "BYTES":
            if not all(isinstance(x, str) for x in batch):
                raise ValueError("For BYTES datatype, batch must be a list of strings")
            batch_shape = [len(batch)]
            data = batch
        else:
            first_instance = batch[0]
            if np.isscalar(first_instance):
                batch_shape = [len(batch)]
                data = [float(x) for x in batch]
            elif isinstance(first_instance, (list, np.ndarray)):
                instance_shape = np.array(first_instance).shape
                batch_shape = (len(batch),) + instance_shape
                data = np.array(batch).flatten().tolist()
            else:
                raise ValueError("Unsupported input type")
        client_options = {"api_endpoint": f"{self.location}-aiplatform.googleapis.com"}
        prediction_client = aiplatform.gapic.PredictionServiceClient(client_options=client_options)
        endpoint = endpoint = (
          f"projects/{self.project}/locations/{self.location}/endpoints/"
          f"{self.endpoint_name}"
        )
        triton_request = {
            "inputs": [
                {
                    "name": self.input_name,
                    "shape": list(batch_shape),
                    "datatype": self.datatype,
                    "data": data,
                }
            ]
        }

        request_body = json.dumps(triton_request).encode('utf-8')
        response = prediction_client.raw_predict(
        endpoint=endpoint,
        http_body=request_body
        )
        
        response_json = json.loads(response.raw_response.data.decode('utf-8'))
        outputs = response_json.get('outputs', [])
        if not outputs:
            raise ValueError("No outputs in response")
        output_data = outputs[0]['data']
        predictions = output_data  

        return utils._convert_to_result(batch, predictions, model.deployed_model_id)
    
    def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
        pass
    
    def batch_elements_kwargs(self) -> Mapping[str, Any]:
        return self._batching_kwargs
    

