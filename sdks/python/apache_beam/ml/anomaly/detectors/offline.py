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
from typing import Optional
from typing import SupportsFloat
from typing import SupportsInt
from typing import TypeVar

import apache_beam as beam
from apache_beam.ml.anomaly.base import AnomalyDetector
from apache_beam.ml.anomaly.base import AnomalyPrediction
from apache_beam.ml.anomaly.specifiable import specifiable
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import PredictionT

KeyT = TypeVar('KeyT')


@specifiable
class OfflineDetector(AnomalyDetector):
  """A offline anomaly detector that uses a provided model handler for scoring.

  Args:
    keyed_model_handler: The model handler to use for inference.
      Requires a `KeyModelHandler[Any, Row, PredictionT, Any]` instance.
    run_inference_args: Optional arguments to pass to RunInference
    **kwargs: Additional keyword arguments to pass to the base
      AnomalyDetector class.
  """
  @staticmethod
  def score_prediction_adapter(
      keyed_prediction: tuple[KeyT, PredictionResult]
  ) -> tuple[KeyT, AnomalyPrediction]:
    """Extracts a float score from `PredictionResult.inference` and wraps it.

    Takes a keyed `PredictionResult` from common ModelHandler output, assumes
    its `inference` attribute is a float-convertible score, and returns the key
    paired with an `AnomalyPrediction` containing that float score.

    Args:
      keyed_prediction: tuple of `(key, PredictionResult)`. `PredictionResult`
        must have an `inference` attribute supporting float conversion.

    Returns:
      tuple of `(key, AnomalyPrediction)` with the extracted score.

    Raises:
      AssertionError: If `PredictionResult.inference` doesn't support float().
    """

    key, prediction = keyed_prediction
    score = prediction.inference
    assert isinstance(score, SupportsFloat)
    return key, AnomalyPrediction(score=float(score))

  @staticmethod
  def label_prediction_adapter(
      keyed_prediction: tuple[KeyT, PredictionResult]
  ) -> tuple[KeyT, AnomalyPrediction]:
    """Extracts an integer label from `PredictionResult.inference` and wraps it.

    Takes a keyed `PredictionResult`, assumes its `inference` attribute is an
    integer-convertible label, and returns the key paired with an
    `AnomalyPrediction` containing that integer label.

    Args:
      keyed_prediction: tuple of `(key, PredictionResult)`. `PredictionResult`
        must have an `inference` attribute supporting int conversion.

    Returns:
      tuple of `(key, AnomalyPrediction)` with the extracted label.

    Raises:
      AssertionError: If `PredictionResult.inference` doesn't support int().
    """

    key, prediction = keyed_prediction
    label = prediction.inference
    assert isinstance(label, SupportsInt)
    return key, AnomalyPrediction(label=int(label))

  def __init__(
      self,
      keyed_model_handler: KeyedModelHandler[Any, beam.Row, PredictionT, Any],
      run_inference_args: Optional[dict[str, Any]] = None,
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
