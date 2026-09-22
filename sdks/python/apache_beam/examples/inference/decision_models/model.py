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

"""Typed decision questions and a small Jev adapter for Beam examples."""

from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Mapping
from typing import Protocol
from typing import Sequence
from typing import Union


@dataclass(frozen=True)
class ChoiceQuestion:
  instructions: str
  criteria: Mapping[str, str]


@dataclass(frozen=True)
class NoulQuestion:
  instructions: str


@dataclass(frozen=True)
class ScoreQuestion:
  instructions: str
  criteria: Sequence[str]


Question = Union[ChoiceQuestion, NoulQuestion, ScoreQuestion]


@dataclass(frozen=True)
class ChoiceAnswer:
  choice: str
  probabilities: Mapping[str, float] = field(default_factory=dict)
  confidence: float | None = None


@dataclass(frozen=True)
class NoulAnswer:
  noul: float


@dataclass(frozen=True)
class ScoreAnswer:
  score: float
  probabilities: Mapping[str, float] = field(default_factory=dict)
  confidence: float = 0.0


Answer = Union[ChoiceAnswer, NoulAnswer, ScoreAnswer]


@dataclass(frozen=True)
class DecisionResponse:
  answers: Mapping[str, Answer]
  model: str
  provider: str
  request_id: str | None = None


class DecisionModel(Protocol):
  """A provider owns evaluation; Beam owns routing and persistence."""
  def __enter__(self) -> 'DecisionModel':
    ...

  def __exit__(self, exc_type, exc_value, traceback) -> None:
    ...

  def evaluate(
      self, state: Any, questions: Mapping[str, Question]) -> DecisionResponse:
    ...


class JevDecisionModel:
  """TypeSafe SDK adapter with a persistent HTTP connection pool."""
  def __init__(self, model='jev-latest', timeout=10.0):
    self.model = model
    self.timeout = timeout
    self.client = None

  def __enter__(self):
    try:
      from typesafe_sdk import TypeSafeClient
    except ImportError as error:
      raise ImportError(
          'Install typesafe-sdk to use JevDecisionModel') from error
    self.client = TypeSafeClient(timeout=self.timeout)
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.client.close()
    self.client = None

  def evaluate(self, state, questions):
    if self.client is None:
      raise RuntimeError('Enter JevDecisionModel before evaluating questions')
    from typesafe_sdk import Choice
    from typesafe_sdk import Noul
    from typesafe_sdk import Score

    sdk_questions = {}
    for name, question in questions.items():
      if isinstance(question, ChoiceQuestion):
        sdk_questions[name] = Choice(
            instructions=question.instructions, criteria=question.criteria)
      elif isinstance(question, NoulQuestion):
        sdk_questions[name] = Noul(instructions=question.instructions)
      elif isinstance(question, ScoreQuestion):
        sdk_questions[name] = Score(
            instructions=question.instructions, criteria=question.criteria)
      else:
        raise TypeError('Unsupported decision question: %r' % type(question))

    response = self.client.system_one(
        state=state, questions=sdk_questions, model=self.model)
    answers = {}
    for name, question in questions.items():
      answer = response.answers[name]
      if isinstance(question, ChoiceQuestion):
        answers[name] = ChoiceAnswer(
            choice=answer.choice,
            probabilities=dict(answer.probabilities),
            confidence=answer.confidence)
      elif isinstance(question, NoulQuestion):
        answers[name] = NoulAnswer(noul=answer.noul)
      else:
        answers[name] = ScoreAnswer(
            score=answer.score,
            probabilities={
                str(level): probability
                for level, probability in answer.probabilities.items()
            },
            confidence=answer.confidence)
    return DecisionResponse(
        answers=answers,
        model=response.model,
        provider='typesafe',
        request_id=response.request_id)


_ROUTE_TERMS = {
    'billing': ('invoice', 'charge', 'refund', 'payment', 'subscription'),
    'technical': ('api', '503', 'error', 'integration', 'outage'),
    'sales': ('quote', 'seat', 'pricing', 'price', 'plan'),
}


class LocalDecisionModel:
  """Transparent rules for running these examples without network access."""
  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    return None

  def evaluate(self, state, questions):
    message = state['message'].lower()
    fraud_cue = (
        'skip verification' in message or 'bypass verification' in message or
        'unauthorized' in message or 'phishing' in message)
    answers = {}
    for name, question in questions.items():
      if isinstance(question, ChoiceQuestion):
        matches = {
            label: sum(term in message for term in _ROUTE_TERMS.get(label, ()))
            for label in question.criteria
        }
        if not matches:
          raise ValueError('Choice needs at least one option')
        best = max(matches.values())
        winners = [label for label, count in matches.items() if count == best]
        probabilities = {
            label: 1 / len(winners) if label in winners else 0.0
            for label in matches
        }
        answers[name] = ChoiceAnswer(
            choice=winners[0],
            probabilities=probabilities,
            confidence=0.0 if best == 0 else 1 / len(winners))
      elif isinstance(question, NoulQuestion):
        answers[name] = NoulAnswer(noul=float(fraud_cue))
      elif isinstance(question, ScoreQuestion):
        if not question.criteria:
          raise ValueError('Score needs at least one level')
        score = min(3, len(question.criteria) - 1) if fraud_cue else 0
        answers[name] = ScoreAnswer(
            score=float(score),
            probabilities={
                str(level): float(level == score)
                for level in range(len(question.criteria))
            },
            confidence=1.0)
      else:
        raise TypeError('Unsupported decision question: %r' % type(question))
    return DecisionResponse(
        answers=answers, model='local-rules', provider='local')
