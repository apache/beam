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

from parameterized import parameterized

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


def _choice_answer(**fields):
  return typesafe_sdk.ChoiceAnswer.model_construct(
      **{
          'choice': 'billing',
          'probabilities': {
              'billing': 1.0
          },
          'confidence': 0.9,
          **fields,
      })


def _score_answer(**fields):
  return typesafe_sdk.ScoreAnswer.model_construct(
      **{
          'score': 0.7,
          'probabilities': {
              0: 0.3, 1: 0.7
          },
          'confidence': 0.6,
          **fields,
      })


class JevDecisionModelTest(unittest.TestCase):
  def test_normalizes_all_primitives_and_reuses_client(self):
    client = mock.Mock()
    client.system_one.return_value = SimpleNamespace(
        answers={
            'team': _choice_answer(),
            'review': typesafe_sdk.NoulAnswer.model_construct(noul=0.8),
            'urgency': _score_answer(),
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

  @parameterized.expand([
      ('choice_label', 'team', _choice_answer(choice='sales')),
      ('choice_type', 'team', typesafe_sdk.NoulAnswer.model_construct(noul=.5)),
      (
          'choice_probability_key',
          'team',
          _choice_answer(probabilities={'sales': 1.0})),
      ('choice_probabilities_type', 'team', _choice_answer(probabilities=[])),
      (
          'choice_probability_range',
          'team',
          _choice_answer(probabilities={'billing': 1.2})),
      (
          'choice_probability_nan',
          'team',
          _choice_answer(probabilities={'billing': float('nan')})),
      ('choice_confidence', 'team', _choice_answer(confidence=1.3)),
      ('choice_confidence_type', 'team', _choice_answer(confidence=True)),
      (
          'boolean_range',
          'review',
          typesafe_sdk.NoulAnswer.model_construct(noul=1.4)),
      (
          'boolean_nan',
          'review',
          typesafe_sdk.NoulAnswer.model_construct(noul=float('nan'))),
      (
          'boolean_type',
          'review',
          typesafe_sdk.NoulAnswer.model_construct(noul='0.5')),
      ('boolean_answer_type', 'review', _choice_answer()),
      ('score_range', 'urgency', _score_answer(score=3.5)),
      ('score_negative', 'urgency', _score_answer(score=-.1)),
      ('score_infinite', 'urgency', _score_answer(score=float('inf'))),
      ('score_boolean', 'urgency', _score_answer(score=True)),
      ('score_answer_type', 'urgency', _choice_answer()),
      (
          'score_probability_key',
          'urgency',
          _score_answer(probabilities={999: 1.0})),
      (
          'score_probability_key_type',
          'urgency',
          _score_answer(probabilities={'1': 1.0})),
      (
          'score_probability_fractional_key',
          'urgency',
          _score_answer(probabilities={.5: 1.0})),
      (
          'score_probability_negative',
          'urgency',
          _score_answer(probabilities={0: -.1})),
      ('score_confidence', 'urgency', _score_answer(confidence=-.1)),
  ])
  def test_rejects_malformed_answer(self, unused_name, name, answer):
    questions = {
        'team': ChoiceQuestion('Pick a team', {'billing': 'Invoices'}),
        'review': BooleanQuestion('Does this need review?'),
        'urgency': ScoreQuestion('How urgent is it?', ['routine', 'urgent']),
    }
    client = mock.Mock()
    client.system_one.return_value = SimpleNamespace(
        answers={name: answer}, model='jev-test', request_id='request-1')
    with mock.patch.object(typesafe_sdk, 'TypeSafeClient', return_value=client):
      with JevDecisionModel() as adapter:
        with self.assertRaisesRegex(ValueError, name):
          adapter.evaluate('Hello', {name: questions[name]})

  @parameterized.expand([
      ('missing', {}),
      (
          'extra',
          {
              'review': typesafe_sdk.NoulAnswer.model_construct(noul=.5),
              'unexpected': typesafe_sdk.NoulAnswer.model_construct(noul=.5),
          }),
      ('wrong_container', []),
  ])
  def test_rejects_answer_keys(self, unused_name, answers):
    client = mock.Mock()
    client.system_one.return_value = SimpleNamespace(
        answers=answers, model='jev-test', request_id='request-1')
    with mock.patch.object(typesafe_sdk, 'TypeSafeClient', return_value=client):
      with JevDecisionModel() as adapter:
        with self.assertRaisesRegex(ValueError, 'question names'):
          adapter.evaluate(
              'Hello', {'review': BooleanQuestion('Needs review?')})

  def test_accepts_boundaries_and_optional_probabilities(self):
    questions = {
        'team': ChoiceQuestion('Pick a team', {'billing': 'Invoices'}),
        'review': BooleanQuestion('Does this need review?'),
        'urgency': ScoreQuestion('How urgent is it?', ['routine', 'urgent']),
    }
    client = mock.Mock()
    for probability in (0, 1):
      with self.subTest(probability=probability):
        client.system_one.return_value = SimpleNamespace(
            answers={
                'team': _choice_answer(probabilities={}, confidence=None),
                'review': typesafe_sdk.NoulAnswer.model_construct(
                    noul=probability),
                'urgency': _score_answer(
                    score=probability, probabilities={}, confidence=None),
            },
            model='jev-test',
            request_id='request-1')
        with mock.patch.object(typesafe_sdk,
                               'TypeSafeClient',
                               return_value=client):
          with JevDecisionModel() as adapter:
            response = adapter.evaluate('Hello', questions)
        self.assertEqual(probability, response.answers['review'].probability)
        self.assertEqual(probability, response.answers['urgency'].score)
        self.assertIsNone(response.answers['team'].confidence)

  def test_provider_error_propagates_and_client_closes(self):
    client = mock.Mock()
    client.system_one.side_effect = TimeoutError('request timed out')
    with mock.patch.object(typesafe_sdk, 'TypeSafeClient', return_value=client):
      with self.assertRaisesRegex(TimeoutError, 'request timed out'):
        with JevDecisionModel() as adapter:
          adapter.evaluate(
              'Hello', {'review': BooleanQuestion('Needs review?')})
    client.close.assert_called_once_with()

  def test_sdk_response_validation_error_is_a_value_error(self):
    error = typesafe_sdk.TypeSafeAPIResponseValidationError(
        status=200, body={}, headers={}, field_path='answers.review.noul')
    client = mock.Mock()
    client.system_one.side_effect = error
    with mock.patch.object(typesafe_sdk, 'TypeSafeClient', return_value=client):
      with JevDecisionModel() as adapter:
        with self.assertRaisesRegex(ValueError,
                                    'Invalid Jev response') as raised:
          adapter.evaluate(
              'Hello', {'review': BooleanQuestion('Needs review?')})
    self.assertIs(error, raised.exception.__cause__)
    client.close.assert_called_once_with()


if __name__ == '__main__':
  unittest.main()
