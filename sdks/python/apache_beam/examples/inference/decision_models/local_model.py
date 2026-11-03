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

"""Deterministic rules for the decision model examples."""

from apache_beam.ml.inference.decision import BooleanAnswer
from apache_beam.ml.inference.decision import BooleanQuestion
from apache_beam.ml.inference.decision import ChoiceAnswer
from apache_beam.ml.inference.decision import ChoiceQuestion
from apache_beam.ml.inference.decision import DecisionModel
from apache_beam.ml.inference.decision import DecisionResponse
from apache_beam.ml.inference.decision import ScoreAnswer
from apache_beam.ml.inference.decision import ScoreQuestion

_ROUTE_TERMS = {
    'billing': ('invoice', 'charge', 'refund', 'payment', 'subscription'),
    'technical': ('api', '503', 'error', 'integration', 'outage'),
    'sales': ('quote', 'seat', 'pricing', 'price', 'plan'),
}


class LocalDecisionModel(DecisionModel):
  """Transparent rules for running these examples without network access."""
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
      elif isinstance(question, BooleanQuestion):
        answers[name] = BooleanAnswer(probability=float(fraud_cue))
      elif isinstance(question, ScoreQuestion):
        if not question.criteria:
          raise ValueError('Score needs at least one level')
        score = min(3, len(question.criteria) - 1) if fraud_cue else 0
        answers[name] = ScoreAnswer(
            score=float(score),
            probabilities={
                level: float(level == score)
                for level in range(len(question.criteria))
            },
            confidence=1.0)
      else:
        raise TypeError('Unsupported decision question: %r' % type(question))
    return DecisionResponse(
        answers=answers, model='local-rules', provider='local')
