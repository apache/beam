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

import logging
import sys
import threading
import unittest
from concurrent import futures

import grpc
from future.utils import raise_

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.runners.worker import data_plane


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
    data_channel_service = data_plane.GrpcServerDataChannel()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    beam_fn_api_pb2_grpc.add_BeamFnDataServicer_to_server(
        data_channel_service, server)
    test_port = server.add_insecure_port('[::]:0')
    server.start()

    data_channel_stub = beam_fn_api_pb2_grpc.BeamFnDataStub(
        grpc.insecure_channel('localhost:%s' % test_port))
    data_channel_client = data_plane.GrpcClientDataChannel(data_channel_stub)

    try:
      self._data_channel_test(data_channel_service, data_channel_client)
    finally:
      data_channel_client.close()
      data_channel_service.close()
      data_channel_client.wait()
      data_channel_service.wait()

  def test_in_memory_data_channel(self):
    channel = data_plane.InMemoryDataChannel()
    self._data_channel_test(channel, channel.inverse())

  def _data_channel_test(self, server, client):
    self._data_channel_test_one_direction(server, client)
    self._data_channel_test_one_direction(client, server)

  def _data_channel_test_one_direction(self, from_channel, to_channel):
    def send(instruction_id, target, data):
      stream = from_channel.output_stream(instruction_id, target)
      stream.write(data)
      stream.close()
    target_1 = beam_fn_api_pb2.Target(
        primitive_transform_reference='1',
        name='out')
    target_2 = beam_fn_api_pb2.Target(
        primitive_transform_reference='2',
        name='out')

    # Single write.
    send('0', target_1, b'abc')
    self.assertEqual(
        list(to_channel.input_elements('0', [target_1])),
        [beam_fn_api_pb2.Elements.Data(
            instruction_reference='0',
            target=target_1,
            data=b'abc')])

    # Multiple interleaved writes to multiple instructions.
    target_2 = beam_fn_api_pb2.Target(
        primitive_transform_reference='2',
        name='out')

    send('1', target_1, b'abc')
    send('2', target_1, b'def')
    self.assertEqual(
        list(to_channel.input_elements('1', [target_1])),
        [beam_fn_api_pb2.Elements.Data(
            instruction_reference='1',
            target=target_1,
            data=b'abc')])
    send('2', target_2, b'ghi')
    self.assertEqual(
        list(to_channel.input_elements('2', [target_1, target_2])),
        [beam_fn_api_pb2.Elements.Data(
            instruction_reference='2',
            target=target_1,
            data=b'def'),
         beam_fn_api_pb2.Elements.Data(
             instruction_reference='2',
             target=target_2,
             data=b'ghi')])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
