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

import unittest
from unittest import mock

import apache_beam as beam
from apache_beam.ml.inference.decision import BooleanAnswer
from apache_beam.ml.inference.decision import BooleanQuestion
from apache_beam.ml.inference.decision import ChoiceAnswer
from apache_beam.ml.inference.decision import ChoiceQuestion
from apache_beam.ml.inference.decision import DecisionModel
from apache_beam.ml.inference.decision import DecisionResponse
from apache_beam.ml.inference.decision import EvaluateDecisions
from apache_beam.ml.inference.decision import ScoreAnswer
from apache_beam.ml.inference.decision import ScoreQuestion
from apache_beam.ml.inference.decision import _EvaluateDecisionsDoFn
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.window import FixedWindows

_QUESTIONS = {
    'team': ChoiceQuestion(
        'Which team should handle this message?', {
            'billing': 'Invoices and refunds',
            'support': 'Technical problems',
        }),
    'eligible': BooleanQuestion('Is this message eligible for automation?'),
    'severity': ScoreQuestion(
        'How severe is this message?', ('low', 'medium', 'high')),
}


def _response(model, provider, state, eligible, severity):
  return DecisionResponse(
      answers={
          'team': ChoiceAnswer(
              'support' if eligible > .5 else 'billing',
              probabilities={
                  'support': eligible,
                  'billing': 1 - eligible,
              }),
          'eligible': BooleanAnswer(eligible),
          'severity': ScoreAnswer(float(severity)),
      },
      model=model,
      provider=provider,
      request_id=state['id'])


class _KeywordModel(DecisionModel):
  def evaluate(self, state, questions):
    if set(questions) != set(_QUESTIONS):
      raise AssertionError('the model received a different question set')
    eligible = .9 if 'bug' in state['text'] else .2
    return _response(
        'keyword-v1', 'local-keyword', state, eligible, state['priority'])


class _PriorityModel(DecisionModel):
  def evaluate(self, state, questions):
    if set(questions) != set(_QUESTIONS):
      raise AssertionError('the model received a different question set')
    eligible = .8 if state['priority'] >= 2 else .1
    return _response(
        'priority-v1', 'local-priority', state, eligible, state['priority'])


class _SummarizeDecision(beam.DoFn):
  def process(
      self,
      result,
      timestamp=beam.DoFn.TimestampParam,
      window=beam.DoFn.WindowParam):
    answers = result.response.answers
    yield (
        result.state,
        result.response,
        answers['team'].choice,
        answers['eligible'].probability >= .5,
        answers['severity'].score,
        timestamp.micros, (window.start.micros, window.end.micros))


class _LifecycleModel(DecisionModel):
  def __init__(self):
    self.enter_count = 0
    self.exit_calls = []
    self.active = mock.Mock()

  def __enter__(self):
    self.enter_count += 1
    return self.active

  def __exit__(self, exc_type, exc_value, traceback):
    self.exit_calls.append((exc_type, exc_value, traceback))

  def evaluate(self, state, questions):
    raise AssertionError('the context returned model must evaluate')


class DecisionTest(unittest.TestCase):
  def test_models_share_questions_downstream_and_preserve_window_metadata(self):
    first = {'id': 'first', 'text': 'bug in invoice', 'priority': 2}
    second = {'id': 'second', 'text': 'refund request', 'priority': 0}
    stream = (
        TestStream().add_elements([
            beam.window.TimestampedValue(first, 1),
            beam.window.TimestampedValue(second, 11),
        ]).advance_watermark_to_infinity())

    expected = {
        'keyword': [
            (
                first,
                _response('keyword-v1', 'local-keyword', first, .9, 2),
                'support',
                True,
                2.0,
                1_000_000, (0, 10_000_000)),
            (
                second,
                _response('keyword-v1', 'local-keyword', second, .2, 0),
                'billing',
                False,
                0.0,
                11_000_000, (10_000_000, 20_000_000)),
        ],
        'priority': [
            (
                first,
                _response('priority-v1', 'local-priority', first, .8, 2),
                'support',
                True,
                2.0,
                1_000_000, (0, 10_000_000)),
            (
                second,
                _response('priority-v1', 'local-priority', second, .1, 0),
                'billing',
                False,
                0.0,
                11_000_000, (10_000_000, 20_000_000)),
        ],
    }

    with TestPipeline() as pipeline:
      states = pipeline | stream | beam.WindowInto(FixedWindows(10))
      for label, model in (
          ('keyword', _KeywordModel()),
          ('priority', _PriorityModel()),
      ):
        decisions = states | f'{label} decisions' >> EvaluateDecisions(
            model, _QUESTIONS)
        summarized = decisions | f'{label} downstream' >> beam.ParDo(
            _SummarizeDecision())
        assert_that(
            summarized, equal_to(expected[label]), label=f'check {label}')

  def test_context_model_is_reused_across_bundles_and_closed(self):
    model = _LifecycleModel()
    model.active.evaluate.side_effect = [
        _response('test', 'mock', {'id': 'one'}, .9, 1),
        _response('test', 'mock', {'id': 'two'}, .1, 0),
    ]
    dofn = _EvaluateDecisionsDoFn(model, _QUESTIONS)

    dofn.setup()
    dofn.start_bundle()
    first = list(dofn.process({'id': 'one'}))
    dofn.finish_bundle()
    dofn.start_bundle()
    second = list(dofn.process({'id': 'two'}))
    dofn.finish_bundle()
    dofn.teardown()

    self.assertEqual(1, model.enter_count)
    self.assertEqual(2, model.active.evaluate.call_count)
    self.assertEqual(1, len(model.exit_calls))
    self.assertEqual((None, None, None), model.exit_calls[0])
    self.assertEqual('one', first[0].state['id'])
    self.assertEqual('two', second[0].state['id'])

  def test_model_exception_propagates_from_process(self):
    model = _LifecycleModel()
    model.active.evaluate.side_effect = RuntimeError('provider failed')
    dofn = _EvaluateDecisionsDoFn(model, _QUESTIONS)

    dofn.setup()
    with self.assertRaisesRegex(RuntimeError, 'provider failed'):
      list(dofn.process({'id': 'failed'}))
    dofn.teardown()
    self.assertEqual(1, len(model.exit_calls))


if __name__ == '__main__':
  unittest.main()
