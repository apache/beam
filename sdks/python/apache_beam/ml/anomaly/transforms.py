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

import dataclasses
import uuid
from collections.abc import Callable
from collections.abc import Iterable
from typing import Any
from typing import Optional
from typing import TypeVar
from typing import Union

import apache_beam as beam
from apache_beam.coders import CloudpickleCoder
from apache_beam.ml.anomaly import aggregations
from apache_beam.ml.anomaly.base import AggregationFn
from apache_beam.ml.anomaly.base import AnomalyDetector
from apache_beam.ml.anomaly.base import AnomalyPrediction
from apache_beam.ml.anomaly.base import AnomalyResult
from apache_beam.ml.anomaly.base import EnsembleAnomalyDetector
from apache_beam.ml.anomaly.base import ThresholdFn
from apache_beam.ml.anomaly.detectors.offline import OfflineDetector
from apache_beam.ml.anomaly.specifiable import Spec
from apache_beam.ml.anomaly.specifiable import Specifiable
from apache_beam.ml.inference.base import RunInference
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.typehints.typehints import TupleConstraint

KeyT = TypeVar('KeyT')
TempKeyT = TypeVar('TempKeyT', bound=str)
InputT = beam.Row
OutputT = AnomalyResult
KeyedInputT = tuple[KeyT, beam.Row]
KeyedOutputT = tuple[KeyT, AnomalyResult]
NestedKeyedInputT = tuple[KeyT, tuple[TempKeyT, beam.Row]]
NestedKeyedOutputT = tuple[KeyT, tuple[TempKeyT, AnomalyResult]]


class _ScoreAndLearnDoFn(beam.DoFn):
  """Scores and learns from incoming data using an anomaly detection model.

  This DoFn applies an anomaly detection model to score incoming data and
  then updates the model with the same data. It maintains the model state
  using Beam's state management.
  """
  MODEL_STATE_INDEX = ReadModifyWriteStateSpec(
      'saved_model', CloudpickleCoder())

  def __init__(self, detector_spec: Spec):
    self._detector_spec = detector_spec

    assert isinstance(self._detector_spec.config, dict)
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
      element: NestedKeyedInputT,
      model_state=beam.DoFn.StateParam(MODEL_STATE_INDEX),
      **kwargs) -> Iterable[NestedKeyedOutputT]:

    k1, (k2, data) = element
    self._underlying: AnomalyDetector = model_state.read()
    if self._underlying is None:
      self._underlying = Specifiable.from_spec(self._detector_spec)

    yield k1, (k2,
               AnomalyResult(
                   example=data,
                   predictions=[
                       AnomalyPrediction(
                           model_id=self._underlying._model_id,
                           score=self.score_and_learn(data))
                   ]))

    model_state.write(self._underlying)


class RunScoreAndLearn(beam.PTransform[beam.PCollection[NestedKeyedInputT],
                                       beam.PCollection[NestedKeyedOutputT]]):
  """Applies the _ScoreAndLearnDoFn to a PCollection of data.

  This PTransform scores and learns from data points using an anomaly
  detection model.

  Args:
    detector: The anomaly detection model to use.
  """
  def __init__(self, detector: AnomalyDetector):
    self._detector = detector

  def expand(
      self, input: beam.PCollection[NestedKeyedInputT]
  ) -> beam.PCollection[NestedKeyedOutputT]:
    return input | beam.ParDo(_ScoreAndLearnDoFn(self._detector.to_spec()))


class _BaseThresholdDoFn(beam.DoFn):
  """Applies a ThresholdFn to anomaly detection results.

  This abstract base class defines the structure for DoFns that use a
  `ThresholdFn` to convert anomaly scores into anomaly labels (e.g., normal
  or outlier). It handles the core logic of applying the threshold function
  and updating the prediction labels within `AnomalyResult` objects.

  Args:
    threshold_fn_spec (Spec): Specification defining the `ThresholdFn` to be
      used.
  """
  def __init__(self, threshold_fn_spec: Spec):
    self._threshold_fn_spec = threshold_fn_spec

  def _apply_threshold_to_predictions(
      self, result: AnomalyResult) -> AnomalyResult:
    """Updates the prediction labels in an AnomalyResult using the ThresholdFn.

    Args:
      result (AnomalyResult): The input `AnomalyResult` containing anomaly
        scores.

    Returns:
      AnomalyResult: A new `AnomalyResult` with updated prediction labels
        and threshold values.
    """
    predictions = [
        dataclasses.replace(
            p,
            label=self._threshold_fn.apply(p.score),
            threshold=self._threshold_fn.threshold) for p in result.predictions
    ]
    return dataclasses.replace(result, predictions=predictions)


class _StatelessThresholdDoFn(_BaseThresholdDoFn):
  """Applies a stateless ThresholdFn to anomaly detection results.

  This DoFn is designed for stateless `ThresholdFn` implementations. It
  initializes the `ThresholdFn` once during setup and applies it to each
  incoming element without maintaining any state across elements.

  Args:
    threshold_fn_spec (Spec): Specification defining the `ThresholdFn` to be
      used.

  Raises:
    AssertionError: If the provided `threshold_fn_spec` leads to the
      creation of a stateful `ThresholdFn`.
  """
  def __init__(self, threshold_fn_spec: Spec):
    assert isinstance(threshold_fn_spec.config, dict)
    threshold_fn_spec.config["_run_init"] = True
    self._threshold_fn = Specifiable.from_spec(threshold_fn_spec)
    assert isinstance(self._threshold_fn, ThresholdFn)
    assert not self._threshold_fn.is_stateful, \
      "This DoFn can only take stateless function as threshold_fn"

  def process(self, element: NestedKeyedOutputT,
              **kwargs) -> Iterable[NestedKeyedOutputT]:
    """Processes a batch of anomaly results using a stateless ThresholdFn.

    Args:
      element (tuple[Any, tuple[Any, AnomalyResult]]): A tuple representing
        an element in the Beam pipeline. It is expected to be in the format
        `(key1, (key2, AnomalyResult))`, where key1 is the original input key,
        and key2 is a disambiguating key for distinct data points.
      **kwargs:  Additional keyword arguments passed to the `process` method
        in Beam DoFns.

    Yields:
      Iterable[tuple[Any, tuple[Any, AnomalyResult]]]: An iterable containing
        a single output element with the same structure as the input, but with
        the `AnomalyResult` having updated prediction labels based on the
        stateless `ThresholdFn`.
    """
    k1, (k2, result) = element
    yield k1, (k2, self._apply_threshold_to_predictions(result))


class _StatefulThresholdDoFn(_BaseThresholdDoFn):
  """Applies a stateful ThresholdFn to anomaly detection results.

  This DoFn is designed for stateful `ThresholdFn` implementations. It leverages
  Beam's state management to persist and update the state of the `ThresholdFn`
  across multiple elements. This is necessary for `ThresholdFn`s that need to
  accumulate information or adapt over time, such as quantile-based thresholds.

  Args:
    threshold_fn_spec (Spec): Specification defining the `ThresholdFn` to be
      used.

  Raises:
    AssertionError: If the provided `threshold_fn_spec` leads to the
      creation of a stateless `ThresholdFn`.
  """
  THRESHOLD_STATE_INDEX = ReadModifyWriteStateSpec(
      'saved_tracker', CloudpickleCoder())

  def __init__(self, threshold_fn_spec: Spec):
    assert isinstance(threshold_fn_spec.config, dict)
    threshold_fn_spec.config["_run_init"] = True
    threshold_fn = Specifiable.from_spec(threshold_fn_spec)
    assert isinstance(threshold_fn, ThresholdFn)
    assert threshold_fn.is_stateful, \
      "This DoFn can only take stateful function as threshold_fn"
    self._threshold_fn_spec = threshold_fn_spec

  def process(
      self,
      element: NestedKeyedOutputT,
      threshold_state=beam.DoFn.StateParam(THRESHOLD_STATE_INDEX),
      **kwargs) -> Iterable[NestedKeyedOutputT]:
    """Processes a batch of anomaly results using a stateful ThresholdFn.

    For each input element, this DoFn retrieves the stateful `ThresholdFn` from
    Beam state, initializes it if it's the first time, applies it to update
    the prediction labels in the `AnomalyResult`, and then updates the state in
    Beam for future elements.

    Args:
      element (tuple[Any, tuple[Any, AnomalyResult]]): A tuple representing
        an element in the Beam pipeline. It is expected to be in the format
        `(key1, (key2, AnomalyResult))`, where key1 is the original input key,
        and key2 is a disambiguating key for distinct data points.
      threshold_state: A Beam state parameter that provides access to the
        persisted state of the `ThresholdFn`. It is automatically managed by
        Beam.
      **kwargs: Additional keyword arguments passed to the `process` method
        in Beam DoFns.

    Yields:
      Iterable[tuple[Any, tuple[Any, AnomalyResult]]]: An iterable containing
        a single output element with the same structure as the input, but
        with the `AnomalyResult` having updated prediction labels based on
        the stateful `ThresholdFn`.
    """
    k1, (k2, result) = element

    self._threshold_fn = threshold_state.read()
    if self._threshold_fn is None:
      self._threshold_fn = Specifiable.from_spec(self._threshold_fn_spec)

    yield k1, (k2, self._apply_threshold_to_predictions(result))

    threshold_state.write(self._threshold_fn)


class RunThresholdCriterion(
    beam.PTransform[beam.PCollection[NestedKeyedOutputT],
                    beam.PCollection[NestedKeyedOutputT]]):
  """Applies a threshold criterion to anomaly detection results.

  This PTransform applies a `ThresholdFn` to the anomaly scores in
  `AnomalyResult` objects, updating the prediction labels. It handles both
  stateful and stateless `ThresholdFn` implementations.

  Args:
    threshold_criterion: The `ThresholdFn` to apply.
  """
  def __init__(self, threshold_criterion: ThresholdFn):
    self._threshold_fn = threshold_criterion

  def expand(
      self, input: beam.PCollection[NestedKeyedOutputT]
  ) -> beam.PCollection[NestedKeyedOutputT]:

    if self._threshold_fn.is_stateful:
      return (
          input
          | beam.ParDo(_StatefulThresholdDoFn(self._threshold_fn.to_spec())))
    else:
      return (
          input
          | beam.ParDo(_StatelessThresholdDoFn(self._threshold_fn.to_spec())))


class RunAggregationStrategy(
    beam.PTransform[beam.PCollection[NestedKeyedOutputT],
                    beam.PCollection[NestedKeyedOutputT]]):
  """Applies an aggregation strategy to grouped anomaly detection results.

  This PTransform aggregates anomaly predictions from multiple models or
  data points using an `AggregationFn`. It handles both custom and simple
  aggregation strategies.

  Args:
    aggregation_strategy: The `AggregationFn` to use.
    agg_model_id: The model ID for aggregation.
  """
  def __init__(
      self, aggregation_strategy: Optional[AggregationFn], agg_model_id: str):
    self._aggregation_fn = aggregation_strategy
    self._agg_model_id = agg_model_id

  def expand(
      self, input: beam.PCollection[NestedKeyedOutputT]
  ) -> beam.PCollection[NestedKeyedOutputT]:
    post_gbk = (
        input | beam.MapTuple(lambda k, v: ((k, v[0]), v[1]))
        | beam.GroupByKey())

    if self._aggregation_fn is None:
      # simply put predictions into an iterable (list)
      ret = (
          post_gbk
          | beam.MapTuple(
              lambda k, v: (
                  k[0], (
                      k[1], AnomalyResult(
                          example=v[0].example, predictions=[
                              prediction for result in v
                              for prediction in result.predictions
                          ])))))
      return ret

    # create a new aggregation_fn from spec and make sure it is initialized
    aggregation_fn_spec = self._aggregation_fn.to_spec()
    aggregation_fn_spec.config["_run_init"] = True
    aggregation_fn = Specifiable.from_spec(aggregation_fn_spec)

    # if no _agg_model_id is set in the aggregation function, use
    # model id from the ensemble instance
    if (isinstance(aggregation_fn, aggregations._AggModelIdMixin)):
      aggregation_fn._set_agg_model_id_if_unset(self._agg_model_id)

    # post_gbk is a PCollection of ((original_key, temp_key), AnomalyResult).
    # We use (original_key, temp_key) as the key for GroupByKey() so that
    # scores from multiple detectors per data point are grouped.
    ret = (
        post_gbk
        | beam.MapTuple(
            lambda k, v, agg=aggregation_fn: (
                k[0], (
                    k[1], AnomalyResult(
                        example=v[0].example, predictions=[
                            agg.apply([
                                prediction for result in v
                                for prediction in result.predictions
                            ])
                        ])))))
    return ret


class RunOneDetector(beam.PTransform[beam.PCollection[NestedKeyedInputT],
                                     beam.PCollection[NestedKeyedOutputT]]):
  """Runs a single anomaly detector on a PCollection of data.

  This PTransform applies a single `AnomalyDetector` to the input data,
  including scoring, learning, and thresholding.

  Args:
    detector: The `AnomalyDetector` to run.
  """
  def __init__(self, detector):
    self._detector = detector

  def expand(
      self, input: beam.PCollection[NestedKeyedInputT]
  ) -> beam.PCollection[NestedKeyedOutputT]:
    model_id = getattr(
        self._detector,
        "_model_id",
        getattr(self._detector, "_key", "unknown_model"))
    model_uuid = f"{model_id}:{uuid.uuid4().hex[:6]}"

    ret = (
        input
        | beam.Reshuffle()
        | f"Score and Learn ({model_uuid})" >> RunScoreAndLearn(self._detector))

    if self._detector._threshold_criterion:
      ret = (
          ret | f"Run Threshold Criterion ({model_uuid})" >>
          RunThresholdCriterion(self._detector._threshold_criterion))

    return ret


class RunOfflineDetector(beam.PTransform[beam.PCollection[NestedKeyedInputT],
                                         beam.PCollection[NestedKeyedOutputT]]):
  """Runs a offline anomaly detector on a PCollection of data.

  This PTransform applies a `OfflineDetector` to the input data, handling
  custom input/output conversion and inference.

  Args:
    offline_detector: The `OfflineDetector` to run.
  """
  def __init__(self, offline_detector: OfflineDetector):
    self._offline_detector = offline_detector

  def _restore_and_convert(
      self, elem: tuple[tuple[Any, Any, beam.Row], Any]) -> NestedKeyedOutputT:
    """Converts the model output to AnomalyResult.

    Args:
      elem: A tuple containing the combined key (original key, temp key, row)
        and the output from RunInference.

    Returns:
      A tuple containing the keyed AnomalyResult.
    """
    (orig_key, temp_key, row), prediction = elem
    assert isinstance(prediction, AnomalyPrediction), (
      "Wrong model handler output type." +
      f"Expected: 'AnomalyPrediction', but got '{type(prediction).__name__}'. " +  # pylint: disable=line-too-long
      "Consider adding a post-processing function via `with_postprocess_fn` " +
      f"to convert from '{type(prediction).__name__}' to 'AnomalyPrediction', " +  # pylint: disable=line-too-long
      "or use `score_prediction_adapter` or `label_prediction_adapter` to " +
      "perform the conversion.")

    result = AnomalyResult(
        example=row,
        predictions=[
            dataclasses.replace(
                prediction, model_id=self._offline_detector._model_id)
        ])
    return orig_key, (temp_key, result)

  def _select_features(self, elem: tuple[Any,
                                         beam.Row]) -> tuple[Any, beam.Row]:
    assert self._offline_detector._features is not None
    k, v = elem
    row_dict = v._asdict()
    return (
        k,
        beam.Row(**{k: row_dict[k]
                    for k in self._offline_detector._features}))

  def expand(
      self, input: beam.PCollection[NestedKeyedInputT]
  ) -> beam.PCollection[NestedKeyedOutputT]:
    model_uuid = f"{self._offline_detector._model_id}:{uuid.uuid4().hex[:6]}"

    # Call RunInference Transform with the keyed model handler
    run_inference = RunInference(
        self._offline_detector._keyed_model_handler,
        **self._offline_detector._run_inference_args)

    # ((orig_key, temp_key, beam.Row), beam.Row)
    rekeyed_model_input = input | "Rekey" >> beam.Map(
        lambda x: ((x[0], x[1][0], x[1][1]), x[1][1]))

    if self._offline_detector._features is not None:
      rekeyed_model_input = rekeyed_model_input | "Select Features" >> beam.Map(
          self._select_features)

    # ((orig_key, temp_key, beam.Row), AnomalyPrediction)
    rekeyed_model_output = (
        rekeyed_model_input
        | f"Call RunInference ({model_uuid})" >> run_inference)

    ret = (
        rekeyed_model_output | "Restore keys and convert model output" >>
        beam.Map(self._restore_and_convert))

    if self._offline_detector._threshold_criterion:
      ret = (
          ret | f"Run Threshold Criterion ({model_uuid})" >>
          RunThresholdCriterion(self._offline_detector._threshold_criterion))

    return ret


class RunEnsembleDetector(beam.PTransform[beam.PCollection[NestedKeyedInputT],
                                          beam.PCollection[NestedKeyedOutputT]]
                          ):
  """Runs an ensemble of anomaly detectors on a PCollection of data.

  This PTransform applies an `EnsembleAnomalyDetector` to the input data,
  running each sub-detector and aggregating the results.

  Args:
    ensemble_detector: The `EnsembleAnomalyDetector` to run.
  """
  def __init__(self, ensemble_detector: EnsembleAnomalyDetector):
    self._ensemble_detector = ensemble_detector

  def expand(
      self, input: beam.PCollection[NestedKeyedInputT]
  ) -> beam.PCollection[NestedKeyedOutputT]:
    model_uuid = f"{self._ensemble_detector._model_id}:{uuid.uuid4().hex[:6]}"

    assert self._ensemble_detector._sub_detectors is not None
    if not self._ensemble_detector._sub_detectors:
      raise ValueError(f"No detectors found at {model_uuid}")

    results = []
    for idx, detector in enumerate(self._ensemble_detector._sub_detectors):
      if isinstance(detector, EnsembleAnomalyDetector):
        results.append(
            input
            | f"Run Ensemble Detector at index {idx} ({model_uuid})" >>
            RunEnsembleDetector(detector))
      elif isinstance(detector, OfflineDetector):
        results.append(
            input
            | f"Run Offline Detector at index {idx} ({model_uuid})" >>
            RunOfflineDetector(detector))
      else:
        results.append(
            input
            | f"Run One Detector at index {idx} ({model_uuid})" >>
            RunOneDetector(detector))

    if self._ensemble_detector._aggregation_strategy is None:
      aggregation_type = "Simple"
    else:
      aggregation_type = "Custom"

    ret = (
        results | beam.Flatten()
        | f"Run {aggregation_type} Aggregation Strategy ({model_uuid})" >>
        RunAggregationStrategy(
            self._ensemble_detector._aggregation_strategy,
            self._ensemble_detector._model_id))

    if self._ensemble_detector._threshold_criterion:
      ret = (
          ret | f"Run Threshold Criterion ({model_uuid})" >>
          RunThresholdCriterion(self._ensemble_detector._threshold_criterion))

    return ret


class AnomalyDetection(beam.PTransform[beam.PCollection[Union[InputT,
                                                              KeyedInputT]],
                                       beam.PCollection[Union[OutputT,
                                                              KeyedOutputT]]]):
  """Performs anomaly detection on a PCollection of data.

  This PTransform applies an `AnomalyDetector` or `EnsembleAnomalyDetector` to
  the input data and returns a PCollection of `AnomalyResult` objects.

  Examples::

      # Run a single anomaly detector
      p | AnomalyDetection(ZScore(features=["x1"]))

      # Run an ensemble anomaly detector
      sub_detectors = [ZScore(features=["x1"]), IQR(features=["x2"])]
      p | AnomalyDetection(EnsembleAnomalyDetector(
          sub_detectors, aggregation_strategy=AnyVote()))

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
      input: beam.PCollection[Union[InputT, KeyedInputT]],
  ) -> beam.PCollection[Union[OutputT, KeyedOutputT]]:

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

    if isinstance(input.element_type, TupleConstraint):
      keyed_input = input
    else:
      # Add a default key 0 if the input is unkeyed.
      keyed_input = input | beam.WithKeys(0)

    add_temp_key_fn: Callable[[KeyedInputT], NestedKeyedInputT]
    run_detector: beam.PTransform
    if isinstance(self._root_detector, EnsembleAnomalyDetector):
      add_temp_key_fn = lambda e: (e[0], (str(uuid.uuid1()), e[1]))
      run_detector = RunEnsembleDetector(self._root_detector)
    else:
      # If there is only one non-ensemble detector, temp key can be the same
      # because we don't need it to identify each input during result
      # aggregation.
      add_temp_key_fn = lambda e: (e[0], ("", e[1]))
      if isinstance(self._root_detector, OfflineDetector):
        run_detector = RunOfflineDetector(self._root_detector)
      else:
        run_detector = RunOneDetector(self._root_detector)

    nested_keyed_input = (
        keyed_input | "Add temp key" >> beam.Map(add_temp_key_fn))

    nested_keyed_output = nested_keyed_input | run_detector

    # Remove the temporary key and simplify the output.
    remove_temp_key_fn: Callable[[NestedKeyedOutputT], KeyedOutputT] \
        = lambda e: (e[0], e[1][1])
    keyed_output = nested_keyed_output | "Remove temp key" >> beam.Map(
        remove_temp_key_fn)

    if isinstance(input.element_type, TupleConstraint):
      ret = keyed_output
    else:
      # Remove the default key if the input is unkeyed.
      ret = keyed_output | beam.Values()

    return ret
