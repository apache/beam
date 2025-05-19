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

import pickle
from collections.abc import Iterable
from collections.abc import Sequence
from typing import Any
from typing import Optional

import numpy as np

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.anomaly.detectors.offline import OfflineDetector
from apache_beam.ml.anomaly.specifiable import specifiable
from apache_beam.ml.anomaly.thresholds import FixedThreshold
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import _PostProcessingModelHandler
from apache_beam.ml.inference.utils import _convert_to_result
from pyod.models.base import BaseDetector as PyODBaseDetector

# Turn the used ModelHandler into specifiable, but without lazy init.
KeyedModelHandler = specifiable(  # type: ignore[misc]
    KeyedModelHandler,
    on_demand_init=False,
    just_in_time_init=False)
_PostProcessingModelHandler = specifiable(  # type: ignore[misc]
    _PostProcessingModelHandler,
    on_demand_init=False,
    just_in_time_init=False)


@specifiable
class PyODModelHandler(ModelHandler[beam.Row,
                                    PredictionResult,
                                    PyODBaseDetector]):
  """Implementation of the ModelHandler interface for PyOD [#]_ Models.

  The ModelHandler processes input data as `beam.Row` objects.

  **NOTE:** This API and its implementation are currently under active
  development and may not be backward compatible.

  Args:
    model_uri: The URI specifying the location of the pickled PyOD model.

  .. [#] https://github.com/yzhao062/pyod
  """
  def __init__(self, model_uri: str):
    self._model_uri = model_uri

  def load_model(self) -> PyODBaseDetector:
    file = FileSystems.open(self._model_uri, 'rb')
    return pickle.load(file)

  def run_inference(
      self,
      batch: Sequence[beam.Row],
      model: PyODBaseDetector,
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    np_batch = []
    for row in batch:
      np_batch.append(np.fromiter(row, dtype=np.float64))

    # stack a batch of samples into a 2-D array for better performance
    vectorized_batch = np.stack(np_batch, axis=0)
    predictions = model.decision_function(vectorized_batch)

    return _convert_to_result(batch, predictions, model_id=self._model_uri)


class PyODFactory():
  @staticmethod
  def create_detector(model_uri: str, **kwargs) -> OfflineDetector:
    """A utility function to create OfflineDetector for a PyOD model.

    **NOTE:** This API and its implementation are currently under active
    development and may not be backward compatible.

    Args:
      model_uri: The URI specifying the location of the pickled PyOD model.
      **kwargs: Additional keyword arguments.
    """
    model_handler = KeyedModelHandler(
        PyODModelHandler(model_uri=model_uri)).with_postprocess_fn(
            OfflineDetector.score_prediction_adapter)
    m = model_handler.load_model()
    assert (isinstance(m, PyODBaseDetector))
    threshold = float(m.threshold_)
    detector = OfflineDetector(
        model_handler, threshold_criterion=FixedThreshold(threshold), **kwargs)  # type: ignore[arg-type]
    return detector
