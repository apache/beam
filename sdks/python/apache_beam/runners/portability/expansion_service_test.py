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
# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import signal
import sys

import grpc

import apache_beam as beam
import apache_beam.transforms.combiners as combine
from apache_beam.pipeline import PipelineOptions
from apache_beam.portability.api import beam_expansion_api_pb2_grpc
from apache_beam.runners.portability import expansion_service
from apache_beam.transforms import ptransform
from apache_beam.utils.thread_pool_executor import UnboundedThreadPoolExecutor

# This script provides an expansion service and example ptransforms for running
# external transform test cases. See external_test.py for details.

_LOGGER = logging.getLogger(__name__)


@ptransform.PTransform.register_urn('beam:transforms:xlang:count', None)
class CountPerElementTransform(ptransform.PTransform):
  def expand(self, pcoll):
    return (
        pcoll | combine.Count.PerElement()
    )

  def to_runner_api_parameter(self, unused_context):
    return 'beam:transforms:xlang:count', None

  @staticmethod
  def from_runner_api_parameter(unused_parameter, unused_context):
    return CountPerElementTransform()


@ptransform.PTransform.register_urn(
    'beam:transforms:xlang:filter_less_than_eq', bytes)
class FilterLessThanTransform(ptransform.PTransform):
  def __init__(self, payload):
    self._payload = payload

  def expand(self, pcoll):
    return (
        pcoll | beam.Filter(
            lambda elem, target: elem <= target, int(ord(self._payload[0])))
    )

  def to_runner_api_parameter(self, unused_context):
    return (
        'beam:transforms:xlang:filter_less_than', self._payload.encode('utf8'))

  @staticmethod
  def from_runner_api_parameter(payload, unused_context):
    return FilterLessThanTransform(payload.decode('utf8'))


@ptransform.PTransform.register_urn('simple', None)
class SimpleTransform(ptransform.PTransform):
  def expand(self, pcoll):
    return pcoll | 'TestLabel' >> beam.Map(lambda x: 'Simple(%s)' % x)

  def to_runner_api_parameter(self, unused_context):
    return 'simple', None

  @staticmethod
  def from_runner_api_parameter(unused_parameter, unused_context):
    return SimpleTransform()


@ptransform.PTransform.register_urn('multi', None)
class MutltiTransform(ptransform.PTransform):
  def expand(self, pcolls):
    return {
        'main':
            (pcolls['main1'], pcolls['main2'])
            | beam.Flatten()
            | beam.Map(lambda x, s: x + s,
                       beam.pvalue.AsSingleton(pcolls['side'])),
        'side': pcolls['side'] | beam.Map(lambda x: x + x),
    }

  def to_runner_api_parameter(self, unused_context):
    return 'multi', None

  @staticmethod
  def from_runner_api_parameter(unused_parameter, unused_context):
    return MutltiTransform()


@ptransform.PTransform.register_urn('payload', bytes)
class PayloadTransform(ptransform.PTransform):
  def __init__(self, payload):
    self._payload = payload

  def expand(self, pcoll):
    return pcoll | beam.Map(lambda x, s: x + s, self._payload)

  def to_runner_api_parameter(self, unused_context):
    return b'payload', self._payload.encode('ascii')

  @staticmethod
  def from_runner_api_parameter(payload, unused_context):
    return PayloadTransform(payload.decode('ascii'))


@ptransform.PTransform.register_urn('fib', bytes)
class FibTransform(ptransform.PTransform):
  def __init__(self, level):
    self._level = level

  def expand(self, p):
    if self._level <= 2:
      return p | beam.Create([1])
    else:
      a = p | 'A' >> beam.ExternalTransform(
          'fib', str(self._level - 1).encode('ascii'),
          expansion_service.ExpansionServiceServicer())
      b = p | 'B' >> beam.ExternalTransform(
          'fib', str(self._level - 2).encode('ascii'),
          expansion_service.ExpansionServiceServicer())
      return (
          (a, b)
          | beam.Flatten()
          | beam.CombineGlobally(sum).without_defaults())

  def to_runner_api_parameter(self, unused_context):
    return 'fib', str(self._level).encode('ascii')

  @staticmethod
  def from_runner_api_parameter(level, unused_context):
    return FibTransform(int(level.decode('ascii')))


server = None


def cleanup(unused_signum, unused_frame):
  _LOGGER.info('Shutting down expansion service.')
  server.stop(None)


def main(unused_argv):
  parser = argparse.ArgumentParser()
  parser.add_argument('-p', '--port',
                      type=int,
                      help='port on which to serve the job api')
  options = parser.parse_args()
  global server
  server = grpc.server(UnboundedThreadPoolExecutor())
  beam_expansion_api_pb2_grpc.add_ExpansionServiceServicer_to_server(
      expansion_service.ExpansionServiceServicer(PipelineOptions()), server
  )
  server.add_insecure_port('localhost:{}'.format(options.port))
  server.start()
  _LOGGER.info('Listening for expansion requests at %d', options.port)

  signal.signal(signal.SIGTERM, cleanup)
  signal.signal(signal.SIGINT, cleanup)
  # blocking main thread forever.
  signal.pause()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main(sys.argv)
