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

import argparse
import logging
import signal
import sys
import typing

import grpc

import apache_beam as beam
import apache_beam.transforms.combiners as combine
from apache_beam.coders import RowCoder
from apache_beam.pipeline import PipelineOptions
from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.portability.api import beam_expansion_api_pb2_grpc
from apache_beam.portability.api.external_transforms_pb2 import ExternalConfigurationPayload
from apache_beam.runners.portability import artifact_service
from apache_beam.runners.portability import expansion_service
from apache_beam.runners.portability.external_transform_registry import ExternalTransformRegistry
from apache_beam.transforms import ptransform
from apache_beam.transforms.environments import PyPIArtifactRegistry
from apache_beam.transforms.external import ImplicitSchemaPayloadBuilder
from apache_beam.utils import thread_pool_executor

# This script provides an expansion service and example ptransforms for running
# external transform test cases. See external_test.py for details.

_LOGGER = logging.getLogger(__name__)

TEST_PREFIX_URN = "beam:transforms:xlang:test:prefix"
TEST_MULTI_URN = "beam:transforms:xlang:test:multi"
TEST_GBK_URN = "beam:transforms:xlang:test:gbk"
TEST_CGBK_URN = "beam:transforms:xlang:test:cgbk"
TEST_COMGL_URN = "beam:transforms:xlang:test:comgl"
TEST_COMPK_URN = "beam:transforms:xlang:test:compk"
TEST_FLATTEN_URN = "beam:transforms:xlang:test:flatten"
TEST_PARTITION_URN = "beam:transforms:xlang:test:partition"
TEST_PYTHON_BS4_URN = "beam:transforms:xlang:test:python_bs4"
TEST_BUILDER_URN = "beam:transforms:xlang:test:builder"

# A transform that does not produce an output.
TEST_NO_OUTPUT_URN = "beam:transforms:xlang:test:nooutput"


@ExternalTransformRegistry.register_urn('beam:transforms:xlang:count')
class CountPerElementTransform(ptransform.PTransform):
  def expand(self, pcoll):
    return pcoll | combine.Count.PerElement()


@ExternalTransformRegistry.register_urn(
    'beam:transforms:xlang:filter_less_than_eq')
class FilterLessThanTransform(ptransform.PTransform):
  def __init__(self, payload):
    self._payload = payload

  def expand(self, pcoll):
    return (
        pcoll | beam.Filter(
            lambda elem, target: elem <= target, int(ord(self._payload[0]))))


@ExternalTransformRegistry.register_urn(TEST_PREFIX_URN)
@beam.typehints.with_output_types(str)
class PrefixTransform(ptransform.PTransform):
  def __init__(self, data):
    self._data = data

  def expand(self, pcoll):
    return pcoll | 'TestLabel' >> beam.Map(
        lambda x: '{}{}'.format(self._data, x))


@ExternalTransformRegistry.register_urn(TEST_MULTI_URN)
class MutltiTransform(ptransform.PTransform):
  def expand(self, pcolls):
    return {
        'main': (pcolls['main1'], pcolls['main2'])
        | beam.Flatten()
        | beam.Map(lambda x, s: x + s, beam.pvalue.AsSingleton(
            pcolls['side'])).with_output_types(str),
        'side': pcolls['side']
        | beam.Map(lambda x: x + x).with_output_types(str),
    }


@ExternalTransformRegistry.register_urn(TEST_GBK_URN)
class GBKTransform(ptransform.PTransform):
  def expand(self, pcoll):
    return pcoll | 'TestLabel' >> beam.GroupByKey()


@ExternalTransformRegistry.register_urn(TEST_CGBK_URN)
class CoGBKTransform(ptransform.PTransform):
  class ConcatFn(beam.DoFn):
    def process(self, element):
      (k, v) = element
      return [(k, v['col1'] + v['col2'])]

  def expand(self, pcoll):
    return pcoll \
           | beam.CoGroupByKey() \
           | beam.ParDo(self.ConcatFn()).with_output_types(
               typing.Tuple[int, typing.Iterable[str]])


@ExternalTransformRegistry.register_urn(TEST_COMGL_URN)
class CombineGloballyTransform(ptransform.PTransform):
  def expand(self, pcoll):
    return pcoll \
           | beam.CombineGlobally(sum).with_output_types(int)


@ExternalTransformRegistry.register_urn(TEST_COMPK_URN)
class CombinePerKeyTransform(ptransform.PTransform):
  def expand(self, pcoll):
    output = pcoll \
           | beam.CombinePerKey(sum)
    # TODO: Use `with_output_types` instead of explicitly
    #  assigning to `.element_type` after fixing BEAM-12872
    output.element_type = beam.typehints.Tuple[str, int]
    return output


@ExternalTransformRegistry.register_urn(TEST_FLATTEN_URN)
class FlattenTransform(ptransform.PTransform):
  def expand(self, pcoll):
    return pcoll.values() | beam.Flatten().with_output_types(int)


@ExternalTransformRegistry.register_urn(TEST_PARTITION_URN)
class PartitionTransform(ptransform.PTransform):
  def expand(self, pcoll):
    col1, col2 = pcoll | beam.Partition(
        lambda elem, n: 0 if elem % 2 == 0 else 1, 2)
    typed_col1 = col1 | beam.Map(lambda x: x).with_output_types(int)
    typed_col2 = col2 | beam.Map(lambda x: x).with_output_types(int)
    return {'0': typed_col1, '1': typed_col2}


class ExtractHtmlTitleDoFn(beam.DoFn):
  def process(self, element):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(element, 'html.parser')
    return [soup.title.string]


@ExternalTransformRegistry.register_urn(TEST_PYTHON_BS4_URN)
class ExtractHtmlTitleTransform(ptransform.PTransform):
  def expand(self, pcoll):
    return pcoll | beam.ParDo(ExtractHtmlTitleDoFn()).with_output_types(str)


class PrefixTransformBuilder(object):
  @classmethod
  def build(cls, data):
    return PrefixTransform(data)


ExternalTransformRegistry.register_urn(
    TEST_BUILDER_URN, PrefixTransformBuilder.build)


@ExternalTransformRegistry.register_urn('payload')
class PayloadTransform(ptransform.PTransform):
  def __init__(self, payload):
    self._payload = payload

  def expand(self, pcoll):
    return pcoll | beam.Map(lambda x, s: x + s, self._payload)


@ExternalTransformRegistry.register_urn('fib')
class FibTransform(ptransform.PTransform):
  def __init__(self, level):
    self._level = level

  def expand(self, p):
    if self._level <= 2:
      return p | beam.Create([1])
    else:
      a = p | 'A' >> beam.ExternalTransform(
          'fib',
          ImplicitSchemaPayloadBuilder({'level': self._level - 1}),
          expansion_service.ExpansionServiceServicer())
      b = p | 'B' >> beam.ExternalTransform(
          'fib',
          ImplicitSchemaPayloadBuilder({'level': self._level - 2}),
          expansion_service.ExpansionServiceServicer())
      return ((a, b)
              | beam.Flatten()
              | beam.CombineGlobally(sum).without_defaults())


@ExternalTransformRegistry.register_urn(TEST_NO_OUTPUT_URN)
class NoOutputTransform(ptransform.PTransform):
  def expand(self, pcoll):
    def log_val(val):
      logging.debug('Got value: %r', val)

    # Logging without returning anything
    _ = (pcoll | 'TestLabel' >> beam.ParDo(log_val))


def parse_string_payload(input_byte):
  payload = ExternalConfigurationPayload()
  payload.ParseFromString(input_byte)

  return RowCoder(payload.schema).decode(payload.payload)._asdict()


server = None


def cleanup(unused_signum, unused_frame):
  _LOGGER.info('Shutting down expansion service.')
  server.stop(None)


def main(unused_argv):
  PyPIArtifactRegistry.register_artifact('beautifulsoup4', '>=4.9,<5.0')
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '-p', '--port', type=int, help='port on which to serve the job api')
  options = parser.parse_args()
  global server
  server = grpc.server(thread_pool_executor.shared_unbounded_instance())
  beam_expansion_api_pb2_grpc.add_ExpansionServiceServicer_to_server(
      expansion_service.ExpansionServiceServicer(
          PipelineOptions(
              ["--experiments", "beam_fn_api", "--sdk_location", "container"])),
      server)
  beam_artifact_api_pb2_grpc.add_ArtifactRetrievalServiceServicer_to_server(
      artifact_service.ArtifactRetrievalService(
          artifact_service.BeamFilesystemHandler(None).file_reader),
      server)
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
