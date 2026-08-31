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

"""Test for WorkerIdInterceptor"""
# pytype: skip-file

import collections
import logging
import unittest

import grpc

from apache_beam.runners.worker.worker_id_interceptor import DataStreamIdInterceptor
from apache_beam.runners.worker.worker_id_interceptor import WorkerIdInterceptor


class _ClientCallDetails(collections.namedtuple(
    '_ClientCallDetails', ('method', 'timeout', 'metadata', 'credentials')),
                         grpc.ClientCallDetails):
  pass


class WorkerIdInterceptorTest(unittest.TestCase):
  def test_worker_id_insertion(self):
    worker_id_key = 'worker_id'
    headers_holder = {}

    def continuation(client_details, request_iterator):
      headers_holder.update(
          {worker_id_key: dict(client_details.metadata).get(worker_id_key)})

    WorkerIdInterceptor._worker_id = 'my_worker_id'

    WorkerIdInterceptor().intercept_stream_stream(
        continuation, _ClientCallDetails(None, None, None, None), [])
    self.assertEqual(
        headers_holder[worker_id_key], 'my_worker_id', 'worker_id_key not set')

  def test_failure_when_worker_id_exists(self):
    worker_id_key = 'worker_id'
    headers_holder = {}

    def continuation(client_details, request_iterator):
      headers_holder.update(
          {worker_id_key: dict(client_details.metadata).get(worker_id_key)})

    WorkerIdInterceptor._worker_id = 'my_worker_id'

    with self.assertRaises(RuntimeError):
      WorkerIdInterceptor().intercept_stream_stream(
          continuation,
          _ClientCallDetails(None, None, [('worker_id', '1')], None), [])


class DataStreamIdInterceptorTest(unittest.TestCase):
  def test_data_stream_id_insertion(self):
    data_stream_id_key = 'data_stream_id'
    headers_holder = {}

    def continuation(client_details, request_iterator):
      headers_holder.update(
          {data_stream_id_key: dict(client_details.metadata).get(data_stream_id_key)})

    DataStreamIdInterceptor('stream_123').intercept_stream_stream(
        continuation, _ClientCallDetails(None, None, None, None), [])
    self.assertEqual(
        headers_holder[data_stream_id_key],
        'stream_123',
        'data_stream_id not set')

  def test_no_data_stream_id_when_empty(self):
    headers_holder = {}

    def continuation(client_details, request_iterator):
      headers_holder['metadata'] = client_details.metadata

    DataStreamIdInterceptor('').intercept_stream_stream(
        continuation, _ClientCallDetails(None, None, None, None), [])
    self.assertIsNone(headers_holder['metadata'])

  def test_failure_when_data_stream_id_exists(self):
    data_stream_id_key = 'data_stream_id'
    headers_holder = {}

    def continuation(client_details, request_iterator):
      headers_holder.update(
          {data_stream_id_key: dict(client_details.metadata).get(data_stream_id_key)})

    with self.assertRaises(RuntimeError):
      DataStreamIdInterceptor('stream_123').intercept_stream_stream(
          continuation,
          _ClientCallDetails(None, None, [('data_stream_id', 'existing')], None),
          [])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()

