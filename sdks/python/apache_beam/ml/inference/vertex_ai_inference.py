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

import base64
import logging
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import Optional
from typing import List

from google.api_core.exceptions import ServerError
from google.api_core.exceptions import TooManyRequests
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import aiplatform

from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.utils import retry
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

InputT = Any
class VertexAITritonModelHandler(ModelHandler[InputT,
                                            PredictionResult,
                                            aiplatform.Endpoint]):
    """
    A custom model handler for Vertex AI endpoints hosting Triton Inference Servers.
    It constructs a payload that Triton expects and calls the raw predict endpoint.
    """
    
    def __init__(self, 
                 project_id: str,
                 location: str,
                 endpoint_name: str,
                 input_name: str,
                 input_datatype: str, 
                 input_tensor_shape: List[int], 
                 output_tensor_name: Optional[str] = None,
                 network: Optional[str] = None,
                 private: bool = False,
                 experiment: Optional[str] = None,
                 *, 
                 min_batch_size: Optional[int] = None,
                 max_batch_size: Optional[int] = None,
                 max_batch_duration_secs: Optional[int] = None,
                 **kwargs
                 ):
        self.project_id = project_id
        self.location = location 
        self.endpoint_name = endpoint_name
        self.input_name = input_name
        self.input_datatype = input_datatype
        self.input_tensor_shape = input_tensor_shape 
        self.output_tensor_name = output_tensor_name
        self.network = network
        self.private = private
        self.experiment = experiment

        self._batching_kwargs = {}
        self._env_vars = kwargs.get('env_vars', {}) 
        if min_batch_size is not None:
            self._batching_kwargs["min_batch_size"] = min_batch_size
        if max_batch_size is not None:
            self._batching_kwargs["max_batch_size"] = max_batch_size
        if max_batch_duration_secs is not None:
            self._batching_kwargs["max_batch_duration_secs"] = max_batch_duration_secs

        if self.private and self.network is None:
            raise ValueError(
                "A VPC network must be provided ('network' arg) to use a private endpoint.")
        try:
             aiplatform.init(
                project=self.project_id,
                location=self.location,
                experiment=self.experiment,
                network=self.network)
             LOGGER.info(
                 "Initialized aiplatform client for project=%s, location=%s",
                 self.project_id, self.location)
        except Exception as e:
            LOGGER.error("Failed to initialize aiplatform client: %s", e, exc_info=True)
            raise RuntimeError(f"Could not initialize Google Cloud AI Platform client: {e}") from e
        
        try:
            LOGGER.info("Performing initial liveness check for endpoint %s...", self.endpoint_name)
            full_endpoint_id_for_check = self.endpoint_name
            if not full_endpoint_id_for_check.startswith('projects/'):
                full_endpoint_id_for_check = f"projects/{self.project_id}/locations/{self.location}/endpoints/{self.endpoint_name}"
            _ = self._retrieve_endpoint(
                endpoint_id=full_endpoint_id_for_check, 
                project=self.project_id, 
                location=self.location,
                is_private=self.private)
            LOGGER.info("Initial liveness check successful.")
        except ValueError as e:
            LOGGER.warning("Initial liveness check for endpoint %s failed: %s. "
                           "Will retry in load_model.", self.endpoint_name, e)
        except Exception as e:
             LOGGER.warning("Unexpected error during initial liveness check for endpoint %s: %s. "
                           "Will retry in load_model.", self.endpoint_name, e, exc_info=True)
        # Configure AdaptiveThrottler and throttling metrics for client-side
        # throttling behavior.
        # See https://docs.google.com/document/d/1ePorJGZnLbNCmLD9mR7iFYOdPsyDA1rDnTpYnbdrzSU/edit?usp=sharing
        # for more details.
        self.throttled_secs = Metrics.counter(
        VertexAITritonModelHandler, "cumulativeThrottlingSeconds")
        self.throttler = AdaptiveThrottler(
        window_ms=1, bucket_ms=1, overload_ratio=2) 
    def load_model(self) -> aiplatform.Endpoint:
        """Loads the Endpoint object for inference, performing liveness check."""
        
        endpoint_id_to_load = self.endpoint_name
        if not endpoint_id_to_load.startswith('projects/'):
             endpoint_id_to_load = f"projects/{self.project_id}/locations/{self.location}/endpoints/{self.endpoint_name}"

        LOGGER.info("Loading/Retrieving Vertex AI endpoint: %s", endpoint_id_to_load)
        ep = self._retrieve_endpoint(
            endpoint_id=endpoint_id_to_load,
            project=self.project_id,
            location=self.location,
            is_private=self.private)
        LOGGER.info("Successfully retrieved endpoint object: %s", ep.name)
        return ep

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

    @retry.with_exponential_backoff(
        num_retries=5, 
        retry_filter=_retry_on_appropriate_gcp_error)
    def send_request(
        self,
        batch: Sequence[InputT],
        model: aiplatform.Endpoint, 
        inference_args: Optional[Dict[str, Any]]): 
        """
        Sends a single rawPredict request to the endpoint.
        Handles throttling, data formatting, and retries.
        """
        throttle_delay_secs = 5 


        while self.throttler.throttle_request(time.time() * MSEC_TO_SEC):
            LOGGER.info(
                "Delaying request for %d seconds due to previous failures for endpoint %s",
                throttle_delay_secs, model.name)
            time.sleep(throttle_delay_secs)
            self.throttled_secs.inc(throttle_delay_secs)

        batch_size = len(batch)
        tensor_shape = [batch_size] + self.input_tensor_shape

        data_list = []
        if self.input_datatype == "BYTES":
            try:
                data_list = [base64.b64encode(item).decode('utf-8') for item in batch]
            except TypeError as e:
                 LOGGER.error("Input data for BYTES tensor could not be base64 encoded. "
                              "Ensure all elements in the batch are bytes-like. Error: %s", e)
                 raise ValueError("Invalid input data type for BYTES tensor.") from e
        elif self.input_datatype in ["FP64", "FP32", "FP16", "INT64", "INT32", "INT16", "INT8", "UINT64", "UINT32", "UINT16", "UINT8", "BOOL"]:
            flat_list = []
            try:
                for item in batch:
                    if isinstance(item, (np.ndarray, np.generic)):
                        flat_list.extend(item.flatten().tolist())
                    elif isinstance(item, list):
                        if not item: continue 
                        if isinstance(item[0], list): 
                           for sub_list in item: flat_list.extend(sub_list)
                        else: 
                           flat_list.extend(item)
                    elif isinstance(item, (int, float, bool, np.number)):
                        flat_list.append(item)
                    else:
                        raise TypeError(f"Unsupported item type in batch for numerical tensor: {type(item)}")
                data_list = flat_list
            except Exception as e:
                LOGGER.error("Failed to flatten batch data for numerical input '%s' (dtype %s): %s",
                             self.input_name, self.input_datatype, e, exc_info=True)
                raise ValueError(f"Input data could not be processed for {self.input_datatype} tensor.") from e
        else:
            raise NotImplementedError(
                f"Data formatting for input datatype '{self.input_datatype}' "
                "is not implemented.")

        payload = {
            "inputs": [
                {
                    "name": self.input_name,
                    "shape": tensor_shape,
                    "datatype": self.input_datatype,
                    "data": data_list,
                }
            ]
        }

        try:
            start_time = time.time()
            prediction_response = model.raw_predict(
                body=payload, 
                headers=None 
            )

            latency_ms = (time.time() - start_time) * MSEC_TO_SEC
            self.throttler.successful_request(int(latency_ms))
            return prediction_response

        except TooManyRequests as e:
            LOGGER.warning("Request failed with TooManyRequests (429) for endpoint %s: %s. Will retry.", model.name, e)
            raise
        except ServerError as e:
            LOGGER.warning("Request failed with ServerError (%s) for endpoint %s: %s. Will retry.", e.code, model.name, e)
            raise
        except GoogleAPICallError as e:
            LOGGER.error("Request failed with GoogleAPICallError for endpoint %s: %s", model.name, e, exc_info=True)
        except Exception as e:
            LOGGER.error("Unexpected error during raw_predict for endpoint %s: %s", model.name, e, exc_info=True)
            raise RuntimeError(f"Unexpected error during Vertex AI raw_predict call: {e}") from e

    def run_inference(
        self,
        batch: Sequence[InputT],
        model: aiplatform.Endpoint, 
        inference_args: Optional[Dict[str, Any]] = None 
    ) -> Iterable[PredictionResult]:
        """
        Sends a prediction request with the Triton-specific payload structure.

        Args:
            batch: A sequence of input examples.
            model: The `aiplatform.Endpoint` object from `load_model`.
            inference_args: Optional. Usually unused for Triton rawPredict.

        Returns:
            An iterable of PredictionResult objects.
        """
        if not batch:
            return []

        response_dict = self.send_request(batch, model, inference_args)

        if not isinstance(response_dict, dict) or 'outputs' not in response_dict:
            raise ValueError(f"Unexpected response format from raw_predict. "
                             f"Expected dict with 'outputs' key, got: {type(response_dict)}")

        outputs = response_dict.get('outputs', [])
        if not isinstance(outputs, list) or not outputs:
            raise ValueError(f"Expected 'outputs' to be a non-empty list, got: {outputs}")
        output_data = None
        found_output_tensor = None
        if self.output_tensor_name:
            for tensor in outputs:
                if tensor.get('name') == self.output_tensor_name:
                    found_output_tensor = tensor
                    break
            if not found_output_tensor:
                raise ValueError(
                    f"Output tensor named '{self.output_tensor_name}' not found in response: {outputs}")
        else:
            found_output_tensor = outputs[0]
            LOGGER.debug("Using data from the first output tensor: %s",
                         found_output_tensor.get('name', 'Unnamed'))

        output_data = found_output_tensor.get('data')
        if output_data is None:
             raise ValueError(
                f"Could not find 'data' in the selected output tensor: {found_output_tensor}")

        output_datatype = found_output_tensor.get('datatype')
        processed_predictions = []

        if output_datatype == 'BYTES':
            try:
                processed_predictions = [base64.b64decode(item) for item in output_data]
            except Exception as e:
                LOGGER.error("Failed to base64 decode BYTES output data: %s", e, exc_info=True)
                raise ValueError("Output data could not be base64 decoded.") from e
        else:
            output_shape = found_output_tensor.get('shape')
            if not output_shape or output_shape[0] != len(batch):
                 LOGGER.warning("Output shape %s in response does not match batch size %d. "
                                "Attempting to proceed, but results may be incorrect.",
                                output_shape, len(batch))
                 if len(output_data) == len(batch):
                     processed_predictions = output_data 
                 else:
                     raise ValueError(f"Cannot reconcile output shape {output_shape} "
                                      f"and data length {len(output_data)} with batch size {len(batch)}")

            else:
                elements_per_item = 1
                if len(output_shape) > 1:
                    elements_per_item = int(np.prod(output_shape[1:]))
                if len(output_data) != elements_per_item * len(batch):
                    raise ValueError(f"Total elements in output data ({len(output_data)}) does not match "
                                     f"expected based on shape ({output_shape}) and batch size ({len(batch)}).")
                processed_predictions = []
                for i in range(len(batch)):
                    start_index = i * elements_per_item
                    end_index = start_index + elements_per_item
                    element_prediction = output_data[start_index:end_index]
                    if elements_per_item == 1:
                         processed_predictions.append(element_prediction[0])
                    else:
                         processed_predictions.append(element_prediction)
        if len(processed_predictions) != len(batch):
            raise ValueError(
                f"Input batch size {len(batch)} does not match "
                f"parsed output size {len(processed_predictions)}. Response: {response_dict}")
        model_id_for_result = response_dict.get('model_name', 'unknown_triton_model')
        if 'model_version' in response_dict:
            model_id_for_result += f":{response_dict['model_version']}"

        return utils._convert_to_result(batch, processed_predictions, model_id_for_result)
    
    def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
        pass
    
    def batch_elements_kwargs(self) -> Mapping[str, Any]:
        return self._batching_kwargs
    

