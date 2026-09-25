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

from apache_beam.ml.inference.decision import BooleanAnswer
from apache_beam.ml.inference.decision import BooleanQuestion
from apache_beam.ml.inference.decision import ChoiceAnswer
from apache_beam.ml.inference.decision import ChoiceQuestion
from apache_beam.ml.inference.decision import DecisionModel
from apache_beam.ml.inference.decision import DecisionResponse
from apache_beam.ml.inference.decision import ScoreAnswer
from apache_beam.ml.inference.decision import ScoreQuestion

__all__ = ['JevDecisionModel']


class JevDecisionModel(DecisionModel):
  """Evaluate Beam decision questions with Jev through the TypeSafe SDK.

  Install ``typesafe-sdk>=0.7.1,<0.8`` on the workers and set
  ``TYPESAFE_API_KEY`` in their environment. The client is created when Beam
  enters the adapter context; its HTTP pool is reused until teardown.
  Choice and Score map directly to the SDK primitives. Boolean maps to Noul,
  whose ``noul`` field becomes :attr:`BooleanAnswer.probability`.

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
    from typesafe_sdk import Choice
    from typesafe_sdk import Noul
    from typesafe_sdk import Score

    sdk_questions = {}
    for name, question in questions.items():
      if isinstance(question, ChoiceQuestion):
        sdk_questions[name] = Choice(
            instructions=question.instructions, criteria=question.criteria)
      elif isinstance(question, BooleanQuestion):
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
      elif isinstance(question, BooleanQuestion):
        answers[name] = BooleanAnswer(probability=answer.noul)
      else:
        answers[name] = ScoreAnswer(
            score=answer.score,
            probabilities={
                int(level): probability
                for level, probability in answer.probabilities.items()
            },
            confidence=answer.confidence)
    return DecisionResponse(
        answers=answers,
        model=response.model,
        provider='typesafe',
        request_id=response.request_id)
