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

"""Collect statistics on transactions in a public bitcoin dataset that was
exported to avro

Usage:
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
python -m apache_beam.examples.bitcoin \
  --compress --fastavro --output fastavro-compressed
"""

# pytype: skip-file

import argparse
import logging

from fastavro.schema import parse_schema

import apache_beam as beam
from apache_beam.io.avroio import ReadFromAvro
from apache_beam.io.avroio import WriteToAvro
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class BitcoinTxnCountDoFn(beam.DoFn):
  """Count inputs and outputs per transaction"""
  def __init__(self):
    # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
    # super().__init__()
    beam.DoFn.__init__(self)
    self.txn_counter = Metrics.counter(self.__class__, 'txns')
    self.inputs_dist = Metrics.distribution(self.__class__, 'inputs_per_txn')
    self.outputs_dist = Metrics.distribution(self.__class__, 'outputs_per_txn')
    self.output_amts_dist = Metrics.distribution(self.__class__, 'output_amts')
    self.txn_amts_dist = Metrics.distribution(self.__class__, 'txn_amts')

  def process(self, elem):
    """Update counters and distributions, and filter and sum some fields"""

    inputs = elem['inputs']
    outputs = elem['outputs']

    self.txn_counter.inc()

    num_inputs = len(inputs)
    num_outputs = len(outputs)

    self.inputs_dist.update(num_inputs)
    self.outputs_dist.update(num_outputs)

    total = 0
    for output in outputs:
      amt = output['output_satoshis']
      self.output_amts_dist.update(amt)
      total += amt

    self.txn_amts_dist.update(total)

    return [{
        "transaction_id": elem["transaction_id"],
        "timestamp": elem["timestamp"],
        "block_id": elem["block_id"],
        "previous_block": elem["previous_block"],
        "num_inputs": num_inputs,
        "num_outputs": num_outputs,
        "sum_output": total,
    }]


SCHEMA = parse_schema({
    "namespace": "example.avro",
    "type": "record",
    "name": "Transaction",
    "fields": [{
        "name": "transaction_id", "type": "string"
    }, {
        "name": "timestamp", "type": "long"
    }, {
        "name": "block_id", "type": "string"
    }, {
        "name": "previous_block", "type": "string"
    }, {
        "name": "num_inputs", "type": "int"
    }, {
        "name": "num_outputs", "type": "int"
    }, {
        "name": "sum_output", "type": "long"
    }]
})


def run(argv=None):
  """Test Avro IO (backed by fastavro or Apache Avro) on a simple pipeline
  that transforms bitcoin transactions"""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://beam-avro-test/bitcoin/txns/*',
      help='Input file(s) to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  parser.add_argument(
      '--compress',
      dest='compress',
      required=False,
      action='store_true',
      help='When set, compress the output data')
  parser.add_argument(
      '--fastavro',
      dest='use_fastavro',
      required=False,
      action='store_true',
      help='When set, use fastavro for Avro I/O')

  opts, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  # Read the avro file[pattern] into a PCollection.
  records = \
      p | 'read' >> ReadFromAvro(opts.input)

  measured = records | 'scan' >> beam.ParDo(BitcoinTxnCountDoFn())

  # pylint: disable=expression-not-assigned
  measured | 'write' >> \
      WriteToAvro(
          opts.output,
          schema=SCHEMA,
          codec=('deflate' if opts.compress else 'null'),
      )

  result = p.run()
  result.wait_until_finish()

  # Do not query metrics when creating a template which doesn't run
  if (not hasattr(result, 'has_job')  # direct runner
      or result.has_job):  # not just a template creation
    metrics = result.metrics().query()

    for counter in metrics['counters']:
      logging.info("Counter: %s", counter)

    for dist in metrics['distributions']:
      logging.info("Distribution: %s", dist)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
