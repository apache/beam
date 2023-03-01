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

from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence

from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from google.cloud import aiplatform_v1


class VertexAIModelHandlerJSON(ModelHandler[str,
                                            PredictionResult,
                                            aiplatform_v1.types.Endpoint]):
  def __init__(
      self,
      endpoint: str,
      project: Optional[str] = None,
      location: Optional[str] = None,
      experiment: Optional[str] = None):
    self.endpointName = aiplatform_v1.endpoint_path(project, location, endpoint)
    self.client = aiplatform_v1.EndpointServiceClient()

    self.endpoint = self._retrieve_endpoint()

  def _retrieve_endpoint(self) -> aiplatform_v1.types.Endpoint:
    request = aiplatform_v1.GetEndpointRequest(name=self.endpointName)
    response = self.client.get_endpoint(request=request)
    if response is None:
      raise ValueError("Vertex AI endpoint %s not found", self.endpointName)

    return response

  def load_model(self) -> aiplatform_v1.types.Endpoint:
    # Check to make sure the endpoint is still active since pipeline construction time
    _ = self._retrieve_endpoint()
    return self.endpoint

  def run_inference(
      self,
      batch: Sequence[str],
      model: aiplatform_v1.types.Endpoint,
      inference_args: Optional[Dict[str, any]]) -> Iterable[PredictionResult]:

    prediction = model.Predict(instances=batch, parameters=inference_args)

    # TODO: implement an "unpack" function to turn the aiplatform_v1.types.Prediction type 
    # into a PredictionResult. Will involve asking questions about the format, because there's
    # a very real chance the dict is already [JSON str input, prediction output].
    raise NotImplementedError(type(self))
