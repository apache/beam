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

from apache_beam.examples.inference.decision_models.message_router import route_message
from apache_beam.examples.inference.decision_models.model import ChoiceAnswer
from apache_beam.examples.inference.decision_models.model import DecisionResponse
from apache_beam.examples.inference.decision_models.transforms import DecisionResult


def result_for(answer):
  return DecisionResult(
      state={
          'event_id': '1', 'message': 'I have an account question'
      },
      response=DecisionResponse(
          answers={'destination': answer}, model='fixed', provider='test'),
      latency_ms=1.0)


class RouteMessageTest(unittest.TestCase):
  def test_uncertain_choice_goes_to_review(self):
    result = result_for(
        ChoiceAnswer(
            choice='billing',
            probabilities={
                'billing': 0.6, 'sales': 0.4
            },
            confidence=0.4))

    row = route_message(result, min_confidence=0.65)

    self.assertEqual('billing', row['decision'])
    self.assertEqual('review', row['destination'])

  def test_confident_choice_selects_destination(self):
    result = result_for(ChoiceAnswer(choice='sales', confidence=0.65))

    row = route_message(result, min_confidence=0.65)

    self.assertEqual('sales', row['destination'])

  def test_unknown_choice_cannot_select_a_table(self):
    result = result_for(ChoiceAnswer(choice='other', confidence=1.0))

    with self.assertRaisesRegex(ValueError, 'unknown destination'):
      route_message(result, min_confidence=0.5)

  def test_missing_confidence_goes_to_review(self):
    result = result_for(ChoiceAnswer(choice='sales'))

    row = route_message(result, min_confidence=0.5)

    self.assertEqual('review', row['destination'])


if __name__ == '__main__':
  unittest.main()
