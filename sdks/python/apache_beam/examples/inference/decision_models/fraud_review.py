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

import apache_beam as beam
from apache_beam.examples.inference.decision_models.local_model import LocalDecisionModel
from apache_beam.ml.inference.decision import BooleanQuestion
from apache_beam.ml.inference.decision import EvaluateDecisions
from apache_beam.ml.inference.decision import ScoreQuestion
from apache_beam.ml.inference.typesafe_inference import JevDecisionModel
from apache_beam.options.pipeline_options import PipelineOptions

SAMPLE_MESSAGES = [
    'Please update my billing address before the next renewal.',
    'I was charged twice; please look into the duplicate payment.',
    'Transfer the refund to this new account immediately and skip verification.',
]

FRAUD_NOUL = BooleanQuestion(
    instructions='Does this message contain signals that warrant fraud review?')
FRAUD_SCORE = ScoreQuestion(
    instructions='How strongly does this message warrant fraud review?',
    criteria=(
        'No visible fraud cue',
        'A weak cue that merits routine checking',
        'A concrete suspicious instruction',
        'An explicit attempt to bypass verification'))


def review_fraud_cues(result):
  response = result.response
  row = {
      'message': result.state['message'],
      'model': response.model,
      'provider': response.provider,
      'latency_ms': round(result.latency_ms, 2),
  }
  if 'fraud_cue' in response.answers:
    probability = response.answers['fraud_cue'].probability
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
  questions = {}
  if args.primitive in ('noul', 'both'):
    questions['fraud_cue'] = FRAUD_NOUL
  if args.primitive in ('score', 'both'):
    questions['risk_level'] = FRAUD_SCORE
  with beam.Pipeline(options=PipelineOptions(pipeline_args)) as pipeline:
    (
        pipeline
        | 'Sample payment messages' >> beam.Create([{
            'message': message
        } for message in SAMPLE_MESSAGES])
        | 'Evaluate fraud cues' >> EvaluateDecisions(model, questions)
        | 'Apply review threshold' >> beam.Map(review_fraud_cues)
        | 'Show review decisions' >>
        beam.Map(lambda row: print(json.dumps(row, sort_keys=True))))


if __name__ == '__main__':
  run()
