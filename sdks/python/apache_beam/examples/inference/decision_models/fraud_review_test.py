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

"""Tests for the fraud example's opt-in malformed-response dead letter."""

import glob
import json
import os
import tempfile
import unittest

import apache_beam as beam
from apache_beam.examples.inference.decision_models.fraud_review import (
    dead_letter_record)
from apache_beam.ml.inference.decision import BooleanAnswer
from apache_beam.ml.inference.decision import BooleanQuestion
from apache_beam.ml.inference.decision import DecisionModel
from apache_beam.ml.inference.decision import DecisionResponse
from apache_beam.ml.inference.decision import EvaluateDecisions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

_QUESTION = {'fraud_cue': BooleanQuestion('Does this need fraud review?')}
_VALID_INPUT = {'message': 'Looks like an ordinary payment question.'}
_MALFORMED_INPUT = {'message': 'Provider returned an invalid probability.'}


class _MalformedResponseModel(DecisionModel):
  def evaluate(self, state, questions):
    if state == _MALFORMED_INPUT:
      raise ValueError('Invalid response probability: 1.2')
    return DecisionResponse(
        answers={'fraud_cue': BooleanAnswer(0.2)},
        model='fixed',
        provider='test')


class FraudReviewDeadLetterTest(unittest.TestCase):
  def test_malformed_response_is_routed_with_original_input(self):
    with tempfile.TemporaryDirectory() as output_dir:
      with TestPipeline() as pipeline:
        good, bad = (
            pipeline
            | beam.Create([_VALID_INPUT, _MALFORMED_INPUT])
            | EvaluateDecisions(_MalformedResponseModel(), _QUESTION).
            with_exception_handling(exc_class=ValueError))

        assert_that(
            good | beam.Map(lambda result: result.state),
            equal_to([_VALID_INPUT]))
        (
            bad
            | beam.Map(dead_letter_record)
            | beam.Map(lambda row: json.dumps(row, sort_keys=True))
            | beam.io.WriteToText(
                os.path.join(output_dir, 'malformed'),
                file_name_suffix='.jsonl',
                num_shards=1))

      output_files = glob.glob(os.path.join(output_dir, 'malformed-*.jsonl'))
      self.assertEqual(1, len(output_files))
      with open(output_files[0], encoding='utf-8') as output_file:
        records = [json.loads(line) for line in output_file]
      self.assertEqual(1, len(records))
      self.assertEqual(_MALFORMED_INPUT, records[0]['input'])
      self.assertTrue(records[0]['error'].startswith('ValueError('))
      self.assertIn('1.2', records[0]['error'])


if __name__ == '__main__':
  unittest.main()
