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

from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from google.cloud import aiplatform

class VertexAIModelHandlerJSON(ModelHandler[str,
                                            PredictionResult,
                                            aiplatform.types.Endpoint]):
  def __init__(
      self,
      endpoint_name: str,
      project: Optional[str] = None,
      location: Optional[str] = None,
      experiment: Optional[str] = None):

    # TODO: support the full list of options for aiplatform.init()
    # See https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform#google_cloud_aiplatform_init
    aiplatform.init(project=project, location=location, experiment=experiment)

    self.endpoint = self._retrieve_endpoint(endpoint_name)

  def _retrieve_endpoint(self, endpoint_name: str) -> aiplatform.types.Endpoint:
    endpoint = aiplatform.Endpoint(endpoint_name=endpoint_name)

    try:
      mod_list = endpoint.list_models()
    except Exception as e:
      raise ValueError("Failed to contact endpoint %s, got exception: %s", endpoint_name, e)
    
    if len(mod_list) == 0:
      raise ValueError("Endpoint %s has no models deployed to it.")
    
    return endpoint

  def load_model(self) -> aiplatform.types.Endpoint:
    # Check to make sure the endpoint is still active since pipeline construction time
    _ = self._retrieve_endpoint()
    return self.endpoint

  def run_inference(
      self,
      batch: Sequence[str],
      model: aiplatform.types.Endpoint,
      inference_args: Optional[Dict[str, any]]) -> Iterable[PredictionResult]:

    # Endpoint.predict returns a Prediction type with the prediction values along 
    # with model metadata
    prediction = model.predict(instances=batch, parameters=inference_args)

    return utils._convert_to_result(batch, prediction.predictions, prediction.deployed_model_id)
