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

"""Tests for apache_beam.runners.worker.sdk_worker."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import unittest

from concurrent import futures
import grpc

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.runners.worker import sdk_worker


class BeamFnControlServicer(beam_fn_api_pb2.BeamFnControlServicer):

  def __init__(self, requests, raise_errors=True):
    self.requests = requests
    self.instruction_ids = set(r.instruction_id for r in requests)
    self.responses = {}
    self.raise_errors = raise_errors

  def Control(self, response_iterator, context):
    for request in self.requests:
      logging.info("Sending request %s", request)
      yield request
    for response in response_iterator:
      logging.info("Got response %s", response)
      if response.instruction_id != -1:
        assert response.instruction_id in self.instruction_ids
        assert response.instruction_id not in self.responses
        self.responses[response.instruction_id] = response
        if self.raise_errors and response.error:
          raise RuntimeError(response.error)
        elif len(self.responses) == len(self.requests):
          logging.info("All %s instructions finished.", len(self.requests))
          return
    raise RuntimeError("Missing responses: %s" %
                       (self.instruction_ids - set(self.responses.keys())))


class SdkWorkerTest(unittest.TestCase):

  def test_fn_registration(self):
    fns = [beam_fn_api_pb2.FunctionSpec(id=str(ix)) for ix in range(4)]

    process_bundle_descriptors = [beam_fn_api_pb2.ProcessBundleDescriptor(
        id=str(100+ix),
        primitive_transform=[
            beam_fn_api_pb2.PrimitiveTransform(function_spec=fn)])
                                  for ix, fn in enumerate(fns)]

    test_controller = BeamFnControlServicer([beam_fn_api_pb2.InstructionRequest(
        register=beam_fn_api_pb2.RegisterRequest(
            process_bundle_descriptor=process_bundle_descriptors))])

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    beam_fn_api_pb2.add_BeamFnControlServicer_to_server(test_controller, server)
    test_port = server.add_insecure_port("[::]:0")
    server.start()

    channel = grpc.insecure_channel("localhost:%s" % test_port)
    harness = sdk_worker.SdkHarness(channel)
    harness.run()
    self.assertEqual(
        harness.worker.fns,
        {item.id: item for item in fns + process_bundle_descriptors})


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
