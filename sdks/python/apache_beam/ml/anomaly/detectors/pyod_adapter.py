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


"""Utilities to adapt PyOD models for Beam's anomaly detection APIs.

Provides a ModelHandler implementation for PyOD detectors and a factory for
creating OfflineDetector wrappers around pickled PyOD models.
"""

import pickle
from collections.abc import Iterable, Sequence
from typing import Optional, cast

import numpy as np
from pyod.models.base import BaseDetector as PyODBaseDetector

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.anomaly.detectors.offline import OfflineDetector
from apache_beam.ml.anomaly.specifiable import specifiable
from apache_beam.ml.anomaly.thresholds import FixedThreshold
from apache_beam.ml.inference.base import (
    KeyedModelHandler,
    ModelHandler,
    PredictionResult,
    _PostProcessingModelHandler,
)
from apache_beam.ml.inference.utils import _convert_to_result


# Turn the used ModelHandler into specifiable, but without lazy init.
KeyedModelHandler = specifiable(  # type: ignore[misc]
    KeyedModelHandler,
    on_demand_init=False,
    just_in_time_init=False,
)

_PostProcessingModelHandler = specifiable(  # type: ignore[misc]
    _PostProcessingModelHandler,
    on_demand_init=False,
    just_in_time_init=False,
)


@specifiable
class PyODModelHandler(ModelHandler[beam.Row, PredictionResult, PyODBaseDetector]):
    """ModelHandler implementation for PyOD models.

    Processes `beam.Row` inputs, flattening vector-like fields (list/tuple/ndarray)
    into a single numeric feature vector per row before invoking the PyOD
    model's ``decision_function``.

    NOTE: Experimental; interface may change.
    Args:
        model_uri: Location of the pickled PyOD model.
    """

    def __init__(self, model_uri: str):
        super().__init__()
        self._model_uri = model_uri

    def load_model(self) -> PyODBaseDetector:
        with FileSystems.open(self._model_uri, 'rb') as file:
            return pickle.load(file)

    def run_inference(  # type: ignore[override]
            self,
            batch: Sequence[beam.Row],
            model: PyODBaseDetector,
            inference_args: Optional[dict[str, object]] = None,
    ) -> Iterable[PredictionResult]:
        """Run inference on a batch of rows.

        Flattens vector-like fields, stacks the batch into a 2-D array and
        returns PredictionResult objects.
        """

        def _flatten_row(row_values):
            for value in row_values:
                if isinstance(value, (list, tuple, np.ndarray)):
                    yield from value
                else:
                    yield value

        np_batch = [np.fromiter(_flatten_row(row), dtype=np.float64) for row in batch]
        vectorized_batch = np.stack(np_batch, axis=0)
        predictions = model.decision_function(vectorized_batch)
        return _convert_to_result(batch, predictions, model_id=self._model_uri)


class PyODFactory:
    """Factory helpers to create OfflineDetector instances from PyOD models."""

    @staticmethod
    def create_detector(model_uri: str, **kwargs) -> OfflineDetector:
        handler = KeyedModelHandler(PyODModelHandler(model_uri=model_uri)).with_postprocess_fn(
            OfflineDetector.score_prediction_adapter)
        model = handler.load_model()
        assert isinstance(model, PyODBaseDetector)
        threshold = float(model.threshold_)
        return OfflineDetector(
            cast(object, handler),
            threshold_criterion=FixedThreshold(threshold),
            **kwargs,
        )
