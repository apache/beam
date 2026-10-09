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

"""Typed questions and a Beam transform for decision models.

A decision model evaluates named questions against each input state. Choice
selects a label, Boolean estimates the probability of a true statement, and
Score evaluates an ordered rubric. Adapters translate these types to a local
model or a service's API.

Use the same questions and transform when changing providers::

  from apache_beam.ml.inference.decision import ChoiceQuestion
  from apache_beam.ml.inference.decision import EvaluateDecisions

  questions = {
      'team': ChoiceQuestion(
          'Which team should handle this message?',
          {'billing': 'Invoices and refunds', 'support': 'Technical problems'})
  }
  decisions = events | EvaluateDecisions(model, questions)

Each output is a :class:`DecisionResult` containing the original state,
normalized answers, provider metadata, and request latency. Applications apply
thresholds and route these results with ordinary Beam transforms.
"""

import abc
import time
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Union

import apache_beam as beam
from apache_beam.metrics import Metrics

__all__ = [
    'ChoiceQuestion',
    'BooleanQuestion',
    'ScoreQuestion',
    'Question',
    'ChoiceAnswer',
    'BooleanAnswer',
    'ScoreAnswer',
    'Answer',
    'DecisionResponse',
    'DecisionResult',
    'DecisionModel',
    'EvaluateDecisions',
]


@dataclass(frozen=True)
class ChoiceQuestion:
  """Select one label from ``criteria``, a mapping of labels to descriptions."""
  instructions: str
  criteria: Mapping[str, str]


@dataclass(frozen=True)
class BooleanQuestion:
  """Estimate the probability that the statement in ``instructions`` is true."""
  instructions: str


@dataclass(frozen=True)
class ScoreQuestion:
  """Score an ordered rubric whose ``criteria`` levels are indexed from zero.

  Providers may interpolate between levels, returning a fractional score.
  """
  instructions: str
  criteria: Sequence[str]


Question = Union[ChoiceQuestion, BooleanQuestion, ScoreQuestion]


@dataclass(frozen=True)
class ChoiceAnswer:
  """A selected label, with optional label probabilities and confidence.

  Probabilities and confidence use [0, 1]. Confidence is a provider's estimate
  of answer certainty; ``None`` means the provider did not supply it.
  """
  choice: str
  probabilities: Mapping[str, float] = field(default_factory=dict)
  confidence: float | None = None


@dataclass(frozen=True)
class BooleanAnswer:
  """Probability in [0, 1] that the statement is true.

  Applications choose their own threshold for acting on this probability.
  """
  probability: float


@dataclass(frozen=True)
class ScoreAnswer:
  """Rubric score, optional probabilities by level index, and confidence.

  The score uses the question's zero-based scale. Probabilities and confidence
  use [0, 1]; ``None`` confidence means the provider did not supply it.
  """
  score: float
  probabilities: Mapping[int, float] = field(default_factory=dict)
  confidence: float | None = None


Answer = Union[ChoiceAnswer, BooleanAnswer, ScoreAnswer]


@dataclass(frozen=True)
class DecisionResponse:
  """One answer per question name, plus the model and provider identifiers."""
  answers: Mapping[str, Answer]
  model: str
  provider: str
  request_id: str | None = None


@dataclass(frozen=True)
class DecisionResult:
  """An input state, its decision response, and evaluation time in milliseconds."""
  state: Any
  response: DecisionResponse
  latency_ms: float


class DecisionModel(abc.ABC):
  """Adapter contract for hosted and local decision models.

  Implement :meth:`evaluate` to return the shared answer types. Override the
  context manager methods to open and close clients or load model weights.
  Constructors hold serializable configuration; Beam enters the context on
  the worker and reuses it across elements and bundles on that DoFn instance.

  An adapter owns provider timeouts, retries, and conversion to the shared
  types. Input states must be serializable by Beam and accepted by the adapter.
  """
  def __enter__(self) -> 'DecisionModel':
    return self

  def __exit__(self, exc_type, exc_value, traceback) -> None:
    pass

  @abc.abstractmethod
  def evaluate(
      self, state: Any, questions: Mapping[str, Question]) -> DecisionResponse:
    """Evaluate each named question against the same state.

    Return exactly one answer of the corresponding type for each question.
    Choice labels must belong to the question's criteria; score levels use
    their zero-based indices. Raise an exception if evaluation fails.
    """
    raise NotImplementedError


class EvaluateDecisions(beam.PTransform):
  """Evaluate typed questions for every input element with a reusable model.

  Args:
    model: A serializable :class:`DecisionModel` adapter. Its context is entered
      once per DoFn instance and closed during teardown, which is best effort.
    questions: A mapping of question names to Choice, Boolean, or Score
      questions. All questions are evaluated against each input state.

  Returns:
    A PCollection of :class:`DecisionResult` objects, preserving each element's
    timestamp and window. Calls run synchronously on the processing thread.
    The ``EvaluateDecisions`` metrics namespace records ``requests``,
    ``failures``, and the ``request_latency_ms`` distribution for successful
    evaluations. Exceptions propagate to the runner.
  """
  def __init__(self, model: DecisionModel, questions: Mapping[str, Question]):
    super().__init__()
    self._model = model
    self._questions = dict(questions)

  def expand(self, states):
    return states | beam.ParDo(
        _EvaluateDecisionsDoFn(self._model, self._questions))


class _EvaluateDecisionsDoFn(beam.DoFn):
  def __init__(self, model, questions):
    self._model = model
    self._questions = questions
    self._active_model = None
    self._requests = Metrics.counter('EvaluateDecisions', 'requests')
    self._failures = Metrics.counter('EvaluateDecisions', 'failures')
    self._latency = Metrics.distribution(
        'EvaluateDecisions', 'request_latency_ms')

  def setup(self):
    self._active_model = self._model.__enter__()

  def process(self, state):
    self._requests.inc()
    started = time.perf_counter()
    try:
      response = self._active_model.evaluate(state, self._questions)
    except Exception:
      self._failures.inc()
      raise
    elapsed_ms = (time.perf_counter() - started) * 1000
    self._latency.update(int(elapsed_ms))
    yield DecisionResult(state, response, elapsed_ms)

  def teardown(self):
    if self._active_model is not None:
      self._model.__exit__(None, None, None)
      self._active_model = None
