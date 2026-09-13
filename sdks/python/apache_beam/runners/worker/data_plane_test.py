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

"""Tests for apache_beam.runners.worker.data_plane."""

# pytype: skip-file

import itertools
import logging
import time
import unittest

import grpc

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.runners.worker import data_plane
from apache_beam.runners.worker.worker_id_interceptor import DataStreamIdInterceptor
from apache_beam.runners.worker.worker_id_interceptor import WorkerIdInterceptor
from apache_beam.utils import thread_pool_executor


class DataChannelTest(unittest.TestCase):
  def test_grpc_data_channel(self):
    self._grpc_data_channel_test()

  def test_time_based_flush_grpc_data_channel(self):
    self._grpc_data_channel_test(True)

  def test_named_grpc_data_channel(self):
    self._grpc_data_channel_test(data_stream_id='stream_1')

  def _grpc_data_channel_test(self, time_based_flush=False, data_stream_id=''):
    if time_based_flush:
      data_servicer = data_plane.BeamFnDataServicer(
          data_buffer_time_limit_ms=100)
    else:
      data_servicer = data_plane.BeamFnDataServicer()
    worker_id = 'worker_0'
    data_channel_service = \
      data_servicer.get_conn_by_worker_id(worker_id, data_stream_id=data_stream_id)

    server = grpc.server(thread_pool_executor.shared_unbounded_instance())
    beam_fn_api_pb2_grpc.add_BeamFnDataServicer_to_server(data_servicer, server)
    test_port = server.add_insecure_port('[::]:0')
    server.start()

    grpc_channel = grpc.insecure_channel('localhost:%s' % test_port)
    interceptors = [WorkerIdInterceptor(worker_id)]
    if data_stream_id:
      interceptors.append(DataStreamIdInterceptor(data_stream_id))
    grpc_channel = grpc.intercept_channel(grpc_channel, *interceptors)
    data_channel_stub = beam_fn_api_pb2_grpc.BeamFnDataStub(grpc_channel)
    if time_based_flush:
      data_channel_client = data_plane.GrpcClientDataChannel(
          data_channel_stub, data_buffer_time_limit_ms=100)
    else:
      data_channel_client = data_plane.GrpcClientDataChannel(data_channel_stub)

    try:
      self._data_channel_test(
          data_channel_service, data_channel_client, time_based_flush)
    finally:
      data_channel_client.close()
      data_channel_service.close()
      data_channel_client.wait()
      data_channel_service.wait()

  def test_grpc_client_data_channel_factory_named_streams(self):
    data_servicer = data_plane.BeamFnDataServicer()
    server = grpc.server(thread_pool_executor.shared_unbounded_instance())
    beam_fn_api_pb2_grpc.add_BeamFnDataServicer_to_server(data_servicer, server)
    test_port = server.add_insecure_port('[::]:0')
    server.start()

    try:
      factory = data_plane.GrpcClientDataChannelFactory(worker_id='worker_0')
      url = 'localhost:%s' % test_port
      ch_default = factory.create_data_channel_from_url(url)
      ch_default2 = factory.create_data_channel_from_url(url, data_stream_id='')
      ch_stream1 = factory.create_data_channel_from_url(
          url, data_stream_id='stream_1')
      ch_stream1_dup = factory.create_data_channel_from_url(
          url, data_stream_id='stream_1')
      ch_stream2 = factory.create_data_channel_from_url(
          url, data_stream_id='stream_2')

      self.assertIs(ch_default, ch_default2)
      self.assertIs(ch_stream1, ch_stream1_dup)
      self.assertIsNot(ch_default, ch_stream1)
      self.assertIsNot(ch_stream1, ch_stream2)
      factory.close()
    finally:
      server.stop(0)

  def test_in_memory_data_channel(self):
    channel = data_plane.InMemoryDataChannel()
    self._data_channel_test(channel, channel.inverse())

  def _data_channel_test(self, server, client, time_based_flush=False):
    self._data_channel_test_one_direction(server, client, time_based_flush)
    self._data_channel_test_one_direction(client, server, time_based_flush)

  def _data_channel_test_one_direction(
      self, from_channel, to_channel, time_based_flush):
    transform_1 = '1'
    transform_2 = '2'

    # Single write.
    stream01 = from_channel.output_stream('0', transform_1)
    stream01.write(b'abc')
    if not time_based_flush:
      stream01.close()
    self.assertEqual(
        list(
            itertools.islice(to_channel.input_elements('0', [transform_1]), 1)),
        [
            beam_fn_api_pb2.Elements.Data(
                instruction_id='0', transform_id=transform_1, data=b'abc')
        ])

    # Multiple interleaved writes to multiple instructions.
    stream11 = from_channel.output_stream('1', transform_1)
    stream11.write(b'abc')
    stream21 = from_channel.output_stream('2', transform_1)
    stream21.write(b'def')
    if not time_based_flush:
      stream11.close()
    self.assertEqual(
        list(
            itertools.islice(to_channel.input_elements('1', [transform_1]), 1)),
        [
            beam_fn_api_pb2.Elements.Data(
                instruction_id='1', transform_id=transform_1, data=b'abc')
        ])
    if time_based_flush:
      # Wait to ensure stream21 is flushed before stream22.
      # Because the flush callback is invoked periodically starting from when a
      # stream is constructed, there is no guarantee that one stream's callback
      # is called before the other.
      time.sleep(0.1)
    else:
      stream21.close()
    stream22 = from_channel.output_stream('2', transform_2)
    stream22.write(b'ghi')
    if not time_based_flush:
      stream22.close()
    self.assertEqual(
        list(
            itertools.islice(
                to_channel.input_elements('2', [transform_1, transform_2]), 2)),
        [
            beam_fn_api_pb2.Elements.Data(
                instruction_id='2', transform_id=transform_1, data=b'def'),
            beam_fn_api_pb2.Elements.Data(
                instruction_id='2', transform_id=transform_2, data=b'ghi')
        ])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
