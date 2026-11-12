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

"""Live Jev integration test, enabled with TYPESAFE_API_KEY."""

import os
import unittest

import pytest

try:
  import typesafe_sdk
except ImportError:
  typesafe_sdk = None

from apache_beam.ml.inference.decision import BooleanAnswer
from apache_beam.ml.inference.decision import BooleanQuestion
from apache_beam.ml.inference.decision import ChoiceAnswer
from apache_beam.ml.inference.decision import ChoiceQuestion
from apache_beam.ml.inference.decision import EvaluateDecisions
from apache_beam.ml.inference.decision import ScoreAnswer
from apache_beam.ml.inference.decision import ScoreQuestion
from apache_beam.ml.inference.typesafe_inference import JevDecisionModel
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that


@pytest.mark.it_postcommit
@unittest.skipIf(typesafe_sdk is None, 'typesafe-sdk is not installed')
@unittest.skipUnless(
    os.environ.get('TYPESAFE_API_KEY'),
    'TYPESAFE_API_KEY environment variable is not set')
class JevInferenceIT(unittest.TestCase):
  def test_typed_decisions_in_streaming_pipeline(self):
    state = {
        'message': 'My invoice was charged twice. Please refund the duplicate.'
    }
    questions = {
        'team': ChoiceQuestion(
            'Which team should handle this message?',
            {
                'billing': 'Invoices and refunds',
                'technical': 'Bugs and outages',
            }),
        'refund': BooleanQuestion('Does this message ask for a refund?'),
        'urgency': ScoreQuestion(
            'How urgent is this message?', ('Routine', 'Urgent', 'Emergency')),
    }

    def check_results(results):
      assert len(results) == 1, results
      result = results[0]
      assert result.state == state
      assert result.response.provider == 'typesafe'
      assert result.response.model
      assert set(result.response.answers) == set(questions)
      choice = result.response.answers['team']
      boolean = result.response.answers['refund']
      score = result.response.answers['urgency']
      assert isinstance(choice, ChoiceAnswer)
      assert choice.choice in questions['team'].criteria
      assert isinstance(boolean, BooleanAnswer)
      assert 0 <= boolean.probability <= 1
      assert isinstance(score, ScoreAnswer)
      assert 0 <= score.score <= 2
      assert set(score.probabilities) <= {0, 1, 2}

    stream = TestStream().add_elements([state]).advance_watermark_to_infinity()
    with TestPipeline() as pipeline:
      results = (
          pipeline
          | stream
          | EvaluateDecisions(JevDecisionModel(), questions))
      assert_that(results, check_results)


if __name__ == '__main__':
  unittest.main()
