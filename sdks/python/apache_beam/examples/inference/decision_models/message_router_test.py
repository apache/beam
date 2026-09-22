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

"""Tests for the policy boundary after a Choice decision."""

import unittest

from apache_beam.examples.inference.decision_models.message_router import ClassifyMessage
from apache_beam.examples.inference.decision_models.model import ChoiceAnswer
from apache_beam.examples.inference.decision_models.model import DecisionResponse


class FixedModel:
  def __init__(self, answer):
    self.answer = answer

  def evaluate(self, state, questions):
    return DecisionResponse(
        answers={'destination': self.answer}, model='fixed', provider='test')


class ClassifyMessageTest(unittest.TestCase):
  def test_uncertain_choice_goes_to_review(self):
    caller = ClassifyMessage(
        FixedModel(
            ChoiceAnswer(
                choice='billing',
                probabilities={
                    'billing': 0.6, 'sales': 0.4
                },
                confidence=0.4)),
        min_confidence=0.65)

    row = caller({'event_id': '1', 'message': 'I have an account question'})

    self.assertEqual('billing', row['decision'])
    self.assertEqual('review', row['destination'])

  def test_unknown_choice_cannot_select_a_table(self):
    caller = ClassifyMessage(
        FixedModel(ChoiceAnswer(choice='other', confidence=1.0)),
        min_confidence=0.5)

    with self.assertRaisesRegex(ValueError, 'unknown destination'):
      caller({'event_id': '1', 'message': 'Hello'})

  def test_missing_confidence_goes_to_review(self):
    caller = ClassifyMessage(
        FixedModel(ChoiceAnswer(choice='sales')), min_confidence=0.5)

    row = caller({'event_id': '1', 'message': 'What does it cost?'})

    self.assertEqual('review', row['destination'])


if __name__ == '__main__':
  unittest.main()
