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

"""Route a short stream of messages to local files or BigQuery with Choice."""

import argparse
import json
import os

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import render
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import TimestampedValue

from apache_beam.examples.inference.decision_models.model import ChoiceQuestion
from apache_beam.examples.inference.decision_models.model import JevDecisionModel
from apache_beam.examples.inference.decision_models.model import LocalDecisionModel
from apache_beam.examples.inference.decision_models.transforms import EvaluateDecisions

DESTINATIONS = {
    'billing': 'Invoices, charges, payments, and refunds',
    'technical': 'Bugs, outages, and integration problems',
    'sales': 'Pricing, plans, and buying questions',
}

QUESTION = ChoiceQuestion(
    instructions='Which team should handle this customer message?',
    criteria=DESTINATIONS)

SAMPLE_MESSAGES = [
    {
        'event_id': 'msg-001',
        'message': 'My invoice has two charges for the same subscription. '
        'Please refund one.'
    },
    {
        'event_id': 'msg-002',
        'message': 'The API returns 503 for every request after I rotated '
        'my key.'
    },
    {
        'event_id': 'msg-003',
        'message': 'Can I get a quote for 50 seats next month?'
    },
]

ROW_SCHEMA = (
    'event_id:STRING,message:STRING,decision:STRING,destination:STRING,'
    'confidence:FLOAT,probabilities_json:STRING,model:STRING,'
    'provider:STRING,request_id:STRING,latency_ms:FLOAT')


def route_message(result, min_confidence):
  event = result.state
  response = result.response
  answer = response.answers['destination']
  if answer.choice not in DESTINATIONS:
    raise ValueError('Decision model returned an unknown destination')
  destination = (
      answer.choice if answer.confidence is not None and
      answer.confidence >= min_confidence else 'review')
  return {
      'event_id': event['event_id'],
      'message': event['message'],
      'decision': answer.choice,
      'destination': destination,
      'confidence': answer.confidence,
      'probabilities_json': json.dumps(answer.probabilities, sort_keys=True),
      'model': response.model,
      'provider': response.provider,
      'request_id': response.request_id,
      'latency_ms': round(result.latency_ms, 2),
  }


class JsonSink(fileio.FileSink):
  def open(self, fh):
    self.fh = fh

  def write(self, row):
    self.fh.write(json.dumps(row, sort_keys=True).encode('utf-8') + b'\n')

  def flush(self):
    self.fh.flush()


def decode_event(payload):
  event = json.loads(payload.decode('utf-8'))
  if not isinstance(event, dict) or not isinstance(event.get('message'), str):
    raise ValueError('Pub/Sub event must be JSON with a message string')
  if not isinstance(event.get('event_id'), str):
    raise ValueError('Pub/Sub event must have an event_id string')
  return event


def table_for(row, dataset):
  return '%s.messages_%s' % (dataset, row['destination'])


def build_pipeline(pipeline, args):
  if args.pubsub_subscription:
    events = (
        pipeline
        | 'Read Pub/Sub' >>
        beam.io.ReadFromPubSub(subscription=args.pubsub_subscription)
        | 'Decode events' >> beam.Map(decode_event))
  else:
    stream = TestStream().add_elements([
        TimestampedValue(message, index)
        for index, message in enumerate(SAMPLE_MESSAGES)
    ]).advance_watermark_to_infinity()
    events = pipeline | 'Sample stream' >> stream

  model = JevDecisionModel() if args.model == 'jev' else LocalDecisionModel()
  rows = (
      events
      | 'Ask Choice' >> beam.ParDo(
          EvaluateDecisions(model, {'destination': QUESTION}))
      | 'Select destination' >> beam.Map(route_message, args.min_confidence)
      | 'Window routed messages' >> beam.WindowInto(FixedWindows(60)))

  if args.bq_dataset:
    rows | 'Write routed BigQuery tables' >> beam.io.WriteToBigQuery(
        table=lambda row: table_for(row, args.bq_dataset),
        schema=ROW_SCHEMA,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
  else:
    rows | 'Write routed local files' >> fileio.WriteToFiles(
        path=args.output_dir,
        destination=lambda row: row['destination'],
        file_naming=fileio.destination_prefix_naming('.jsonl'),
        sink=JsonSink)
  return rows


def run(argv=None):
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('--model', choices=('jev', 'local'), default='jev')
  parser.add_argument('--min-confidence', type=float, default=0.5)
  parser.add_argument('--pubsub-subscription')
  parser.add_argument('--bq-dataset', help='BigQuery project:dataset')
  parser.add_argument('--output-dir', help='Local output directory')
  parser.add_argument('--graph', help='Render the Beam graph to .dot or .svg')
  parser.add_argument(
      '--graph-only',
      action='store_true',
      help='Render without running the job')
  args, pipeline_args = parser.parse_known_args(argv)
  if not 0 <= args.min_confidence <= 1:
    parser.error('--min-confidence must be between 0 and 1')
  if not args.bq_dataset and not args.output_dir:
    parser.error('Set --output-dir or --bq-dataset')
  if args.bq_dataset and args.output_dir:
    parser.error('Use only one sink')
  if (args.model == 'jev' and not args.graph_only and
      not os.environ.get('TYPESAFE_API_KEY')):
    parser.error('TYPESAFE_API_KEY is required for --model=jev')
  if args.graph_only and not args.graph:
    parser.error('--graph-only requires --graph')

  options = PipelineOptions(pipeline_args)
  options.view_as(StandardOptions).streaming = True
  pipeline = beam.Pipeline(options=options)
  build_pipeline(pipeline, args)
  if args.graph:
    render_options = render.RenderOptions([
        '--render_output=' + args.graph,
        '--render_leaf_composite_nodes=^Sample stream$',
        '--render_leaf_composite_nodes=^Write routed .*$',
    ])
    render.RenderRunner().run_pipeline(pipeline, render_options)
  if args.graph_only:
    return None
  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  run()
