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

"""Optional TypeSafe adapter for Beam decision models."""

from collections.abc import Mapping

from apache_beam.ml.inference.decision import BooleanAnswer
from apache_beam.ml.inference.decision import BooleanQuestion
from apache_beam.ml.inference.decision import ChoiceAnswer
from apache_beam.ml.inference.decision import ChoiceQuestion
from apache_beam.ml.inference.decision import DecisionModel
from apache_beam.ml.inference.decision import DecisionResponse
from apache_beam.ml.inference.decision import ScoreAnswer
from apache_beam.ml.inference.decision import ScoreQuestion

__all__ = ['JevDecisionModel']


def _number(name, field, value, maximum=1):
  if (isinstance(value, bool) or not isinstance(value, (int, float)) or
      not 0 <= value <= maximum):
    raise ValueError(
        f'Invalid {field} for question {name!r}: '
        f'expected a finite number in [0, {maximum}]')
  return value


def _probabilities(name, probabilities, keys, key_type):
  if not isinstance(probabilities, Mapping):
    raise ValueError(
        f'Invalid probabilities for question {name!r}: '
        'expected a mapping')
  for key, probability in probabilities.items():
    if (not isinstance(key, key_type) or isinstance(key, bool) or
        key not in keys):
      raise ValueError(
          f'Invalid probability key for question {name!r}: {key!r}')
    _number(name, 'probability', probability)
  return dict(probabilities)


def _confidence(name, confidence):
  return None if confidence is None else _number(name, 'confidence', confidence)


def _answer_type(name, answer, expected_type):
  if not isinstance(answer, expected_type):
    raise ValueError(
        f'Invalid answer type for question {name!r}: '
        f'expected {expected_type.__name__}')


class JevDecisionModel(DecisionModel):
  """Evaluate Beam decision questions with Jev through the TypeSafe SDK.

  Install the optional SDK from ``typesafe_tests_requirements.txt`` and set
  ``TYPESAFE_API_KEY`` on the workers. The client is created when Beam
  enters the adapter context; its HTTP pool is reused until teardown.
  Choice and Score map directly to the SDK primitives. Boolean maps to Noul,
  whose ``noul`` field becomes :attr:`BooleanAnswer.probability`.

  The SDK retries 429 and server errors with exponential backoff, jitter, and
  ``Retry-After`` support. Calls and retries block the processing thread; they
  do not coordinate rate limits across workers. Invalid answer keys, types,
  labels, numeric ranges, or probability keys raise ``ValueError``.

  Args:
    model: TypeSafe model identifier, defaulting to ``jev-latest``.
    timeout: Timeout in seconds for HTTP operations. The SDK handles retries.
  """
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
    import typesafe_sdk

    sdk_questions = {}
    for name, question in questions.items():
      if isinstance(question, ChoiceQuestion):
        sdk_questions[name] = typesafe_sdk.Choice(
            instructions=question.instructions, criteria=question.criteria)
      elif isinstance(question, BooleanQuestion):
        sdk_questions[name] = typesafe_sdk.Noul(
            instructions=question.instructions)
      elif isinstance(question, ScoreQuestion):
        sdk_questions[name] = typesafe_sdk.Score(
            instructions=question.instructions, criteria=question.criteria)
      else:
        raise TypeError('Unsupported decision question: %r' % type(question))

    try:
      response = self.client.system_one(
          state=state, questions=sdk_questions, model=self.model)
    except typesafe_sdk.TypeSafeAPIResponseValidationError as error:
      raise ValueError('Invalid Jev response') from error
    if (not isinstance(response.answers, Mapping) or
        response.answers.keys() != questions.keys()):
      raise ValueError(
          'Jev answer question names must match the requested '
          'question names')
    answers = {}
    for name, question in questions.items():
      answer = response.answers[name]
      if isinstance(question, ChoiceQuestion):
        _answer_type(name, answer, typesafe_sdk.ChoiceAnswer)
        if (not isinstance(answer.choice, str) or
            answer.choice not in question.criteria):
          raise ValueError(
              f'Invalid choice for question {name!r}: '
              f'{answer.choice!r}')
        answers[name] = ChoiceAnswer(
            choice=answer.choice,
            probabilities=_probabilities(
                name, answer.probabilities, question.criteria, str),
            confidence=_confidence(name, answer.confidence))
      elif isinstance(question, BooleanQuestion):
        _answer_type(name, answer, typesafe_sdk.NoulAnswer)
        answers[name] = BooleanAnswer(
            probability=_number(name, 'probability', answer.noul))
      else:
        _answer_type(name, answer, typesafe_sdk.ScoreAnswer)
        answers[name] = ScoreAnswer(
            score=_number(
                name, 'score', answer.score, len(question.criteria) - 1),
            probabilities=_probabilities(
                name, answer.probabilities, range(len(question.criteria)), int),
            confidence=_confidence(name, answer.confidence))
    return DecisionResponse(
        answers=answers,
        model=response.model,
        provider='typesafe',
        request_id=response.request_id)
