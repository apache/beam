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

"""Tests for TypeSafe's translation to the shared decision types."""

import unittest
from types import SimpleNamespace
from unittest import mock

try:
  import typesafe_sdk
except ImportError:
  raise unittest.SkipTest('typesafe-sdk is not installed')

from apache_beam.ml.inference.decision import BooleanAnswer
from apache_beam.ml.inference.decision import BooleanQuestion
from apache_beam.ml.inference.decision import ChoiceAnswer
from apache_beam.ml.inference.decision import ChoiceQuestion
from apache_beam.ml.inference.decision import ScoreAnswer
from apache_beam.ml.inference.decision import ScoreQuestion
from apache_beam.ml.inference.typesafe_inference import JevDecisionModel


class JevDecisionModelTest(unittest.TestCase):
  def test_normalizes_all_primitives_and_reuses_client(self):
    client = mock.Mock()
    client.system_one.return_value = SimpleNamespace(
        answers={
            'team': SimpleNamespace(
                choice='billing',
                probabilities={'billing': 1.0},
                confidence=0.9),
            'review': SimpleNamespace(noul=0.8),
            'urgency': SimpleNamespace(
                score=0.7, probabilities={
                    '0': 0.3, '1': 0.7
                }, confidence=0.6),
        },
        model='jev-test',
        request_id='request-1')
    questions = {
        'team': ChoiceQuestion('Pick a team', {'billing': 'Invoices'}),
        'review': BooleanQuestion('Does this need review?'),
        'urgency': ScoreQuestion('How urgent is it?', ['routine', 'urgent']),
    }
    state = {'message': 'Review my invoice'}
    with mock.patch.object(typesafe_sdk, 'TypeSafeClient',
                           return_value=client) as factory:
      adapter = JevDecisionModel(model='jev-test', timeout=2.0)
      factory.assert_not_called()
      with adapter:
        response = adapter.evaluate(state, questions)
        adapter.evaluate(state, questions)
      factory.assert_called_once_with(timeout=2.0)
    client.close.assert_called_once_with()
    self.assertEqual(2, client.system_one.call_count)
    request = client.system_one.call_args.kwargs
    self.assertEqual(state, request['state'])
    self.assertEqual('jev-test', request['model'])
    sent = request['questions']
    self.assertIsInstance(sent['team'], typesafe_sdk.Choice)
    self.assertEqual(questions['team'].criteria, sent['team'].criteria)
    self.assertIsInstance(sent['review'], typesafe_sdk.Noul)
    self.assertEqual(
        questions['review'].instructions, sent['review'].instructions)
    self.assertIsInstance(sent['urgency'], typesafe_sdk.Score)
    self.assertEqual(questions['urgency'].criteria, sent['urgency'].criteria)
    self.assertEqual(
        ChoiceAnswer('billing', {'billing': 1.0}, 0.9),
        response.answers['team'])
    self.assertEqual(BooleanAnswer(0.8), response.answers['review'])
    self.assertEqual(
        ScoreAnswer(0.7, {
            0: 0.3, 1: 0.7
        }, 0.6), response.answers['urgency'])
    self.assertEqual('jev-test', response.model)
    self.assertEqual('typesafe', response.provider)
    self.assertEqual('request-1', response.request_id)

  def test_provider_error_propagates_and_client_closes(self):
    client = mock.Mock()
    client.system_one.side_effect = TimeoutError('request timed out')
    with mock.patch.object(typesafe_sdk, 'TypeSafeClient', return_value=client):
      with self.assertRaisesRegex(TimeoutError, 'request timed out'):
        with JevDecisionModel() as adapter:
          adapter.evaluate(
              'Hello', {'review': BooleanQuestion('Needs review?')})
    client.close.assert_called_once_with()


if __name__ == '__main__':
  unittest.main()
