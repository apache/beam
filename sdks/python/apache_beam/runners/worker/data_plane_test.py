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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import itertools
import logging
import sys
import threading
import unittest

import grpc
from future.utils import raise_

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.runners.worker import data_plane
from apache_beam.runners.worker.worker_id_interceptor import WorkerIdInterceptor
from apache_beam.utils.thread_pool_executor import UnboundedThreadPoolExecutor


def timeout(timeout_secs):
  def decorate(fn):
    exc_info = []

    def wrapper(*args, **kwargs):
      def call_fn():
        try:
          fn(*args, **kwargs)
        except:  # pylint: disable=bare-except
          exc_info[:] = sys.exc_info()
      thread = threading.Thread(target=call_fn)
      thread.daemon = True
      thread.start()
      thread.join(timeout_secs)
      if exc_info:
        t, v, tb = exc_info  # pylint: disable=unbalanced-tuple-unpacking
        raise_(t, v, tb)
      assert not thread.is_alive(), 'timed out after %s seconds' % timeout_secs
    return wrapper
  return decorate


class DataChannelTest(unittest.TestCase):

  @timeout(5)
  def test_grpc_data_channel(self):
    self._grpc_data_channel_test()

  @timeout(5)
  def test_time_based_flush_grpc_data_channel(self):
    self._grpc_data_channel_test(True)

  def _grpc_data_channel_test(self, time_based_flush=False):
    if time_based_flush:
      data_servicer = data_plane.BeamFnDataServicer(
          data_buffer_time_limit_ms=100)
    else:
      data_servicer = data_plane.BeamFnDataServicer()
    worker_id = 'worker_0'
    data_channel_service = \
      data_servicer.get_conn_by_worker_id(worker_id)

    server = grpc.server(UnboundedThreadPoolExecutor())
    beam_fn_api_pb2_grpc.add_BeamFnDataServicer_to_server(
        data_servicer, server)
    test_port = server.add_insecure_port('[::]:0')
    server.start()

    grpc_channel = grpc.insecure_channel('localhost:%s' % test_port)
    # Add workerId to the grpc channel
    grpc_channel = grpc.intercept_channel(
        grpc_channel, WorkerIdInterceptor(worker_id))
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

  def test_in_memory_data_channel(self):
    channel = data_plane.InMemoryDataChannel()
    self._data_channel_test(channel, channel.inverse())

  def _data_channel_test(self, server, client, time_based_flush=False):
    self._data_channel_test_one_direction(server, client, time_based_flush)
    self._data_channel_test_one_direction(client, server, time_based_flush)

  def _data_channel_test_one_direction(
      self, from_channel, to_channel, time_based_flush):
    def send(instruction_id, transform_id, data):
      stream = from_channel.output_stream(instruction_id, transform_id)
      stream.write(data)
      if not time_based_flush:
        stream.close()
    transform_1 = '1'
    transform_2 = '2'

    # Single write.
    send('0', transform_1, b'abc')
    self.assertEqual(
        list(itertools.islice(
            to_channel.input_elements('0', [transform_1]), 1)),
        [beam_fn_api_pb2.Elements.Data(
            instruction_id='0',
            transform_id=transform_1,
            data=b'abc')])

    # Multiple interleaved writes to multiple instructions.
    send('1', transform_1, b'abc')
    send('2', transform_1, b'def')
    self.assertEqual(
        list(itertools.islice(
            to_channel.input_elements('1', [transform_1]), 1)),
        [beam_fn_api_pb2.Elements.Data(
            instruction_id='1',
            transform_id=transform_1,
            data=b'abc')])
    send('2', transform_2, b'ghi')
    self.assertEqual(
        list(itertools.islice(
            to_channel.input_elements('2', [transform_1, transform_2]), 2)),
        [beam_fn_api_pb2.Elements.Data(
            instruction_id='2',
            transform_id=transform_1,
            data=b'def'),
         beam_fn_api_pb2.Elements.Data(
             instruction_id='2',
             transform_id=transform_2,
             data=b'ghi')])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
