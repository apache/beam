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

import typing
import uuid
from typing import Any
from typing import Callable
from typing import Iterable
from typing import Tuple
from typing import TypeVar

import apache_beam as beam
from apache_beam.coders import DillCoder
from apache_beam.ml.anomaly import aggregations
from apache_beam.ml.anomaly.base import AggregationFn
from apache_beam.ml.anomaly.base import AnomalyDetector
from apache_beam.ml.anomaly.base import AnomalyPrediction
from apache_beam.ml.anomaly.base import AnomalyResult
from apache_beam.ml.anomaly.base import EnsembleAnomalyDetector
from apache_beam.ml.anomaly.specifiable import Spec
from apache_beam.ml.anomaly.specifiable import Specifiable
from apache_beam.ml.anomaly.thresholds import StatefulThresholdDoFn
from apache_beam.ml.anomaly.thresholds import StatelessThresholdDoFn
from apache_beam.transforms.userstate import ReadModifyWriteRuntimeState
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec

KeyT = TypeVar('KeyT')
TempKeyT = TypeVar('TempKeyT', bound=int)
InputT = Tuple[KeyT, beam.Row]
KeyedInputT = Tuple[KeyT, Tuple[TempKeyT, beam.Row]]
KeyedOutputT = Tuple[KeyT, Tuple[TempKeyT, AnomalyResult]]
OutputT = Tuple[KeyT, AnomalyResult]


class _ScoreAndLearnDoFn(beam.DoFn):
  """Scores and learns from incoming data using an anomaly detection model.

  This DoFn applies an anomaly detection model to score incoming data and
  then updates the model with the same data. It maintains the model state
  using Beam's state management.
  """
  MODEL_STATE_INDEX = ReadModifyWriteStateSpec('saved_model', DillCoder())

  def __init__(self, detector_spec: Spec):
    self._detector_spec = detector_spec
    self._detector_spec.config["_run_init"] = True

  def score_and_learn(self, data):
    """Scores and learns from a single data point.

    Args:
      data: A `beam.Row` representing the input data point.

    Returns:
      float: The anomaly score predicted by the model.
    """
    assert self._underlying
    if self._underlying._features is not None:
      x = beam.Row(**{f: getattr(data, f) for f in self._underlying._features})
    else:
      x = beam.Row(**data._asdict())

    # score the incoming data using the existing model
    y_pred = self._underlying.score_one(x)

    # then update the model with the same data
    self._underlying.learn_one(x)

    return y_pred

  def process(
      self,
      element: KeyedInputT,
      model_state=beam.DoFn.StateParam(MODEL_STATE_INDEX),
      **kwargs) -> Iterable[KeyedOutputT]:

    model_state = typing.cast(ReadModifyWriteRuntimeState, model_state)
    k1, (k2, data) = element
    self._underlying: AnomalyDetector = model_state.read()
    if self._underlying is None:
      self._underlying = typing.cast(
          AnomalyDetector, Specifiable.from_spec(self._detector_spec))

    yield k1, (k2,
               AnomalyResult(
                   example=data,
                   predictions=[AnomalyPrediction(
                       model_id=self._underlying._model_id,
                       score=self.score_and_learn(data))]))

    model_state.write(self._underlying)


class RunScoreAndLearn(beam.PTransform[beam.PCollection[KeyedInputT],
                                       beam.PCollection[KeyedOutputT]]):
  """Applies the _ScoreAndLearnDoFn to a PCollection of data.

  This PTransform scores and learns from data points using an anomaly
  detection model.

  Args:
    detector: The anomaly detection model to use.
  """
  def __init__(self, detector: AnomalyDetector):
    self._detector = detector

  def expand(
      self,
      input: beam.PCollection[KeyedInputT]) -> beam.PCollection[KeyedOutputT]:
    return input | beam.ParDo(_ScoreAndLearnDoFn(self._detector.to_spec()))


class RunThresholdCriterion(beam.PTransform[beam.PCollection[KeyedOutputT],
                                            beam.PCollection[KeyedOutputT]]):
  """Applies a threshold criterion to anomaly detection results.

  This PTransform applies a `ThresholdFn` to the anomaly scores in
  `AnomalyResult` objects, updating the prediction labels. It handles both
  stateful and stateless `ThresholdFn` implementations.

  Args:
    threshold_criterion: The `ThresholdFn` to apply.
  """
  def __init__(self, threshold_criterion):
    self._threshold_fn = threshold_criterion

  def expand(
      self,
      input: beam.PCollection[KeyedOutputT]) -> beam.PCollection[KeyedOutputT]:
    if self._threshold_fn:
      if self._threshold_fn.is_stateful:
        ret = (
            input
            | beam.ParDo(StatefulThresholdDoFn(self._threshold_fn.to_spec())))
      else:
        ret = (
            input
            | beam.ParDo(StatelessThresholdDoFn(self._threshold_fn.to_spec())))
    else:
      ret = input

    return ret


class RunAggregationStrategy(beam.PTransform[beam.PCollection[KeyedOutputT],
                                             beam.PCollection[KeyedOutputT]]):
  """Applies an aggregation strategy to grouped anomaly detection results.

  This PTransform aggregates anomaly predictions from multiple models or
  data points using an `AggregationFn`. It handles both custom and simple
  aggregation strategies.

  Args:
    aggregation_strategy: The `AggregationFn` to use.
    agg_model_id: The model ID for aggregation.
  """
  def __init__(self, aggregation_strategy, agg_model_id):
    self._aggregation_fn = aggregation_strategy
    self._agg_model_id = agg_model_id

  def expand(
      self,
      input: beam.PCollection[KeyedOutputT]) -> beam.PCollection[KeyedOutputT]:
    post_gbk = (
        input | beam.MapTuple(lambda k, v: ((k, v[0]), v[1]))
        | beam.GroupByKey())

    if self._aggregation_fn is None:
      # simply put predictions into an iterable (list)
      ret: Any = (
          post_gbk | beam.MapTuple(
              lambda k,
              v: (
                  k[0],
                  (
                      k[1],
                      AnomalyResult(
                          example=v[0].example,
                          predictions=[
                              prediction for result in v
                              for prediction in result.predictions
                          ])))))
      return ret

    # create a new aggregation_fn from spec and make sure it is initialized
    aggregation_fn_spec = self._aggregation_fn.to_spec()
    aggregation_fn_spec.config["_run_init"] = True
    aggregation_fn: AggregationFn = typing.cast(
        AggregationFn, Specifiable.from_spec(aggregation_fn_spec))

    # if no _agg_model_id is set in the aggregation function, use
    # model id from the ensemble instance
    if (isinstance(aggregation_fn, aggregations._AggModelIdMixin)):
      aggregation_fn._set_agg_model_id_if_unset(self._agg_model_id)

    ret = (
        post_gbk | beam.MapTuple(
            lambda k,
            v,
            agg=aggregation_fn: (
                k[0],
                (
                    k[1],
                    AnomalyResult(
                        example=v[0].example,
                        predictions=[
                            agg.apply([
                                prediction for result in v
                                for prediction in result.predictions
                            ])
                        ])))))
    return ret


class RunOneDetector(beam.PTransform[beam.PCollection[KeyedInputT],
                                     beam.PCollection[KeyedOutputT]]):
  """Runs a single anomaly detector on a PCollection of data.

  This PTransform applies a single `AnomalyDetector` to the input data,
  including scoring, learning, and thresholding.

  Args:
    detector: The `AnomalyDetector` to run.
  """
  def __init__(self, detector):
    self._detector = detector

  def expand(
      self,
      input: beam.PCollection[KeyedInputT]) -> beam.PCollection[KeyedOutputT]:
    model_id = getattr(
        self._detector,
        "_model_id",
        getattr(self._detector, "_key", "unknown_model"))
    model_uuid = f"{model_id}:{uuid.uuid4().hex[:6]}"

    ret: Any = (
        input
        | beam.Reshuffle()
        | f"Score and Learn ({model_uuid})" >> RunScoreAndLearn(self._detector)
        | f"Run Threshold Criterion ({model_uuid})" >> RunThresholdCriterion(
            self._detector._threshold_criterion))

    return ret


class RunEnsembleDetector(beam.PTransform[beam.PCollection[KeyedInputT],
                                          beam.PCollection[KeyedOutputT]]):
  """Runs an ensemble of anomaly detectors on a PCollection of data.

  This PTransform applies an `EnsembleAnomalyDetector` to the input data,
  running each sub-detector and aggregating the results.

  Args:
    ensemble_detector: The `EnsembleAnomalyDetector` to run.
  """
  def __init__(self, ensemble_detector: EnsembleAnomalyDetector):
    self._ensemble_detector = ensemble_detector

  def expand(
      self,
      input: beam.PCollection[KeyedInputT]) -> beam.PCollection[KeyedOutputT]:
    model_uuid = f"{self._ensemble_detector._model_id}:{uuid.uuid4().hex[:6]}"

    assert self._ensemble_detector._sub_detectors is not None
    if not self._ensemble_detector._sub_detectors:
      raise ValueError(f"No detectors found at {model_uuid}")

    results = []
    for idx, detector in enumerate(self._ensemble_detector._sub_detectors):
      if isinstance(detector, EnsembleAnomalyDetector):
        results.append(
            input | f"Run Ensemble Detector at index {idx} ({model_uuid})" >>
            RunEnsembleDetector(detector))
      else:
        results.append(
            input
            | f"Run One Detector at index {idx} ({model_uuid})" >>
            RunOneDetector(detector))

    if self._ensemble_detector._aggregation_strategy is None:
      aggregation_type = "Simple"
    else:
      aggregation_type = "Custom"

    aggregated = (
        results | beam.Flatten()
        | f"Run {aggregation_type} Aggregation Strategy ({model_uuid})" >>
        RunAggregationStrategy(
            self._ensemble_detector._aggregation_strategy,
            self._ensemble_detector._model_id))

    ret: Any = (
        aggregated
        | f"Run Threshold Criterion ({model_uuid})" >> RunThresholdCriterion(
            self._ensemble_detector._threshold_criterion))

    return ret


class AnomalyDetection(beam.PTransform[beam.PCollection[InputT],
                                       beam.PCollection[OutputT]]):
  """Performs anomaly detection on a PCollection of data.

  This PTransform applies an `AnomalyDetector` or `EnsembleAnomalyDetector` to
  the input data and returns a PCollection of `AnomalyResult` objects.

  Examples::

    # Run a single anomaly detector
    p | AnomalyDetection(ZScore(features=["x1"]))

    # Run an ensemble anomaly detector
    sub_detectors = [ZScore(features=["x1"]), IQR(features=["x2"])]
    p | AnomalyDetection(
        EnsembleAnomalyDetector(sub_detectors, aggregation_strategy=AnyVote()))

  Args:
    detector: The `AnomalyDetector` or `EnsembleAnomalyDetector` to use.
  """
  def __init__(
      self,
      detector: AnomalyDetector,
  ) -> None:
    self._root_detector = detector

  def expand(
      self,
      input: beam.PCollection[InputT],
  ) -> beam.PCollection[OutputT]:

    # Add a temporary unique key per data point to facilitate grouping the
    # outputs from multiple anomaly detectors for the same data point.
    #
    # Unique key generation options:
    # (1) Timestamp-based methods: https://docs.python.org/3/library/time.html
    # (2) UUID module: https://docs.python.org/3/library/uuid.html
    #
    # Timestamp precision on Windows can lead to key collisions (see PEP 564:
    # https://peps.python.org/pep-0564/#windows). Only time.perf_counter_ns()
    # provides sufficient precision for our needs.
    #
    # Performance note:
    # $ python -m timeit -n 100000 "import uuid; uuid.uuid1()"
    # 100000 loops, best of 5: 806 nsec per loop
    # $ python -m timeit -n 100000 "import uuid; uuid.uuid4()"
    # 100000 loops, best of 5: 1.53 usec per loop
    # $ python -m timeit -n 100000 "import time; time.perf_counter_ns()"
    # 100000 loops, best of 5: 82.3 nsec per loop
    #
    # We select uuid.uuid1() for its inclusion of node information, making it
    # more suitable for parallel execution environments.
    add_temp_key_fn: Callable[[InputT], KeyedInputT] \
        = lambda e: (e[0], (str(uuid.uuid1()), e[1]))
    keyed_input = (input | "Add temp key" >> beam.Map(add_temp_key_fn))

    if isinstance(self._root_detector, EnsembleAnomalyDetector):
      keyed_output = (keyed_input | RunEnsembleDetector(self._root_detector))
    else:
      keyed_output = (keyed_input | RunOneDetector(self._root_detector))

    # remove the temporary key and simplify the output.
    remove_temp_key_fn: Callable[[KeyedOutputT], OutputT] \
        = lambda e: (e[0], e[1][1])
    ret: Any = keyed_output | "Remove temp key" >> beam.Map(remove_temp_key_fn)

    return ret
