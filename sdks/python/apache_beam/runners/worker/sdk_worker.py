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

"""SDK harness for executing Python Fns via the Fn API."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import Queue as queue
import threading
import traceback

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker import data_plane


class SdkHarness(object):

  def __init__(self, control_channel):
    self._control_channel = control_channel
    self._data_channel_factory = data_plane.GrpcClientDataChannelFactory()

  def run(self):
    contol_stub = beam_fn_api_pb2.BeamFnControlStub(self._control_channel)
    # TODO(robertwb): Wire up to new state api.
    state_stub = None
    self.worker = SdkWorker(state_stub, self._data_channel_factory)

    responses = queue.Queue()
    no_more_work = object()

    def get_responses():
      while True:
        response = responses.get()
        if response is no_more_work:
          return
        yield response

    def process_requests():
      for work_request in contol_stub.Control(get_responses()):
        logging.info('Got work %s', work_request.instruction_id)
        try:
          response = self.worker.do_instruction(work_request)
        except Exception:  # pylint: disable=broad-except
          logging.error(
              'Error processing instruction %s',
              work_request.instruction_id,
              exc_info=True)
          response = beam_fn_api_pb2.InstructionResponse(
              instruction_id=work_request.instruction_id,
              error=traceback.format_exc())
        responses.put(response)
    t = threading.Thread(target=process_requests)
    t.start()
    t.join()
    # get_responses may be blocked on responses.get(), but we need to return
    # control to its caller.
    responses.put(no_more_work)
    self._data_channel_factory.close()
    logging.info('Done consuming work.')


class SdkWorker(object):

  def __init__(self, state_handler, data_channel_factory):
    self.fns = {}
    self.state_handler = state_handler
    self.data_channel_factory = data_channel_factory

  def do_instruction(self, request):
    request_type = request.WhichOneof('request')
    if request_type:
      # E.g. if register is set, this will construct
      # InstructionResponse(register=self.register(request.register))
      return beam_fn_api_pb2.InstructionResponse(**{
          'instruction_id': request.instruction_id,
          request_type: getattr(self, request_type)
                        (getattr(request, request_type), request.instruction_id)
      })
    else:
      raise NotImplementedError

  def register(self, request, unused_instruction_id=None):
    for process_bundle_descriptor in request.process_bundle_descriptor:
      self.fns[process_bundle_descriptor.id] = process_bundle_descriptor
    return beam_fn_api_pb2.RegisterResponse()

  def process_bundle(self, request, instruction_id):
    bundle_processor.BundleProcessor(
        self.fns[request.process_bundle_descriptor_reference],
        self.state_handler,
        self.data_channel_factory).process_bundle(instruction_id)

    return beam_fn_api_pb2.ProcessBundleResponse()
