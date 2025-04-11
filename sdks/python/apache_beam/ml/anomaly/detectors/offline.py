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
from typing import Optional

import apache_beam as beam
from apache_beam.ml.anomaly.base import AnomalyDetector
from apache_beam.ml.anomaly.specifiable import specifiable
from apache_beam.ml.inference.base import KeyedModelHandler


@specifiable
class OfflineDetector(AnomalyDetector):
  """A offline anomaly detector that uses a provided model handler for scoring.

  Args:
    keyed_model_handler: The model handler to use for inference.
      Requires a `KeyModelHandler[Any, Row, float, Any]` instance.
    run_inference_args: Optional arguments to pass to RunInference
    **kwargs: Additional keyword arguments to pass to the base
      AnomalyDetector class.
  """
  def __init__(
      self,
      keyed_model_handler: KeyedModelHandler[Any, beam.Row, float, Any],
      run_inference_args: Optional[Dict[str, Any]] = None,
      **kwargs):
    super().__init__(**kwargs)

    # TODO: validate the model handler type
    self._keyed_model_handler = keyed_model_handler
    self._run_inference_args = run_inference_args or {}

    # always override model_identifier with model_id from the detector
    self._run_inference_args["model_identifier"] = self._model_id

  def learn_one(self, x: beam.Row) -> None:
    """Not implemented since OfflineDetector invokes RunInference directly."""
    raise NotImplementedError

  def score_one(self, x: beam.Row) -> Optional[float]:
    """Not implemented since OfflineDetector invokes RunInference directly."""
    raise NotImplementedError
