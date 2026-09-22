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

"""Small Beam example: flag messages for fraud review with Noul or Score."""

import argparse
import json
import os
import time

import apache_beam as beam
from apache_beam.io.requestresponse import Caller
from apache_beam.io.requestresponse import RequestResponseIO
from apache_beam.options.pipeline_options import PipelineOptions

from apache_beam.examples.inference.decision_models.model import JevDecisionModel
from apache_beam.examples.inference.decision_models.model import LocalDecisionModel
from apache_beam.examples.inference.decision_models.model import NoulQuestion
from apache_beam.examples.inference.decision_models.model import ScoreQuestion

SAMPLE_MESSAGES = [
    'Please update my billing address before the next renewal.',
    'I was charged twice; please look into the duplicate payment.',
    'Transfer the refund to this new account immediately and skip verification.',
]

FRAUD_NOUL = NoulQuestion(
    instructions='Does this message contain signals that warrant fraud review?')
FRAUD_SCORE = ScoreQuestion(
    instructions='How strongly does this message warrant fraud review?',
    criteria=(
        'No visible fraud cue',
        'A weak cue that merits routine checking',
        'A concrete suspicious instruction',
        'An explicit attempt to bypass verification'))


class ReviewFraudCues(Caller):
  def __init__(self, model, primitive):
    self.model = model
    self.primitive = primitive

  def __enter__(self):
    self.model.__enter__()
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    return self.model.__exit__(exc_type, exc_val, exc_tb)

  def __call__(self, message):
    questions = {}
    if self.primitive in ('noul', 'both'):
      questions['fraud_cue'] = FRAUD_NOUL
    if self.primitive in ('score', 'both'):
      questions['risk_level'] = FRAUD_SCORE
    started = time.perf_counter()
    response = self.model.evaluate(
        state={'message': message}, questions=questions)
    row = {
        'message': message,
        'model': response.model,
        'provider': response.provider,
        'latency_ms': round((time.perf_counter() - started) * 1000, 2),
    }
    if 'fraud_cue' in response.answers:
      probability = response.answers['fraud_cue'].noul
      row['fraud_review_probability'] = probability
      row['needs_review'] = probability >= 0.7
    if 'risk_level' in response.answers:
      score = response.answers['risk_level']
      row['risk_score'] = score.score
      row['risk_confidence'] = score.confidence
      row['risk_probabilities'] = score.probabilities
    return row


def run(argv=None):
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('--model', choices=('jev', 'local'), default='jev')
  parser.add_argument(
      '--primitive', choices=('noul', 'score', 'both'), default='noul')
  args, pipeline_args = parser.parse_known_args(argv)
  if args.model == 'jev' and not os.environ.get('TYPESAFE_API_KEY'):
    parser.error('TYPESAFE_API_KEY is required for --model=jev')
  model = JevDecisionModel() if args.model == 'jev' else LocalDecisionModel()
  with beam.Pipeline(options=PipelineOptions(pipeline_args)) as pipeline:
    (
        pipeline
        | 'Sample payment messages' >> beam.Create(SAMPLE_MESSAGES)
        | 'Evaluate fraud cues' >> RequestResponseIO(
            ReviewFraudCues(model, args.primitive), timeout=15, repeater=None)
        | 'Show review decisions' >>
        beam.Map(lambda row: print(json.dumps(row, sort_keys=True))))


if __name__ == '__main__':
  run()
