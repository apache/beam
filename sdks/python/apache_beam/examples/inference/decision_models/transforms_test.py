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

"""Tests for decision model lifecycle and evaluation failures."""

import unittest
from unittest.mock import Mock

from apache_beam.examples.inference.decision_models.model import LocalDecisionModel
from apache_beam.examples.inference.decision_models.model import NoulQuestion
from apache_beam.examples.inference.decision_models.transforms import EvaluateDecisions


class EvaluateDecisionsTest(unittest.TestCase):
  def test_reuses_model_across_bundles_and_preserves_state(self):
    model = Mock(wraps=LocalDecisionModel())
    model.__enter__ = Mock()
    model.__exit__ = Mock()
    question = NoulQuestion('Does the message ask to bypass verification?')
    transform = EvaluateDecisions(model, {'fraud': question})
    transform.setup()
    try:
      for message, expected in [('Pay my invoice', 0.0),
                                ('Please bypass verification', 1.0)]:
        state = {'message': message, 'event_id': message}
        transform.start_bundle()
        result, = transform.process(state)
        transform.finish_bundle()
        self.assertEqual(state, result.state)
        self.assertEqual(expected, result.response.answers['fraud'].noul)
        self.assertGreaterEqual(result.latency_ms, 0)
      model.__enter__.assert_called_once_with()
      self.assertEqual(2, model.evaluate.call_count)
    finally:
      transform.teardown()
    model.__exit__.assert_called_once_with(None, None, None)

  def test_evaluation_failure_propagates(self):
    model = Mock()
    model.evaluate.side_effect = TimeoutError('decision timed out')
    transform = EvaluateDecisions(model, {})

    with self.assertRaisesRegex(TimeoutError, 'decision timed out'):
      list(transform.process({'message': 'Hello'}))


if __name__ == '__main__':
  unittest.main()
