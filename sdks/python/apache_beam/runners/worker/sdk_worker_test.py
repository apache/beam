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
from builtins import range
from concurrent import futures

import grpc

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.worker import sdk_worker


class BeamFnControlServicer(beam_fn_api_pb2_grpc.BeamFnControlServicer):

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

  def _get_process_bundles(self, prefix, size):
    return [
        beam_fn_api_pb2.ProcessBundleDescriptor(
            id=str(str(prefix) + "-" + str(ix)),
            transforms={
                str(ix): beam_runner_api_pb2.PTransform(unique_name=str(ix))
            }) for ix in range(size)
    ]

  def _check_fn_registration_multi_request(self, *args):
    """Check the function registration calls to the sdk_harness.

    Args:
     tuple of request_count, number of process_bundles per request and workers
     counts to process the request.
    """
    for (request_count, process_bundles_per_request, worker_count) in args:
      requests = []
      process_bundle_descriptors = []

      for i in range(request_count):
        pbd = self._get_process_bundles(i, process_bundles_per_request)
        process_bundle_descriptors.extend(pbd)
        requests.append(
            beam_fn_api_pb2.InstructionRequest(
                instruction_id=str(i),
                register=beam_fn_api_pb2.RegisterRequest(
                    process_bundle_descriptor=process_bundle_descriptors)))

      test_controller = BeamFnControlServicer(requests)

      server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
      beam_fn_api_pb2_grpc.add_BeamFnControlServicer_to_server(
          test_controller, server)
      test_port = server.add_insecure_port("[::]:0")
      server.start()

      harness = sdk_worker.SdkHarness(
          "localhost:%s" % test_port, worker_count=worker_count)
      harness.run()

      for worker in harness.workers.queue:
        self.assertEqual(worker.bundle_processor_cache.fns,
                         {item.id: item
                          for item in process_bundle_descriptors})

  def test_fn_registration(self):
    self._check_fn_registration_multi_request((1, 4, 1), (4, 4, 1), (4, 4, 2))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
