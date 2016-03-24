# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Python Dataflow streaming worker."""

from __future__ import absolute_import

import logging
import random
import sys
import time
import traceback


from grpc.beta import implementations

from google.cloud.dataflow.internal import windmill_pb2
from google.cloud.dataflow.internal import windmill_service_pb2
from google.cloud.dataflow.utils import retry
from google.cloud.dataflow.worker import executor
from google.cloud.dataflow.worker import maptask
from google.cloud.dataflow.worker import windmillio
from google.cloud.dataflow.worker import windmillstate
import apitools.base.py as apitools_base
import google.cloud.dataflow.internal.clients.dataflow as dataflow


# pylint: disable=invalid-name
class WindmillClient(object):
  """Client for communication with Windmill."""

  def __init__(self, host, port, request_timeout=10):
    self.host = host
    self.port = port
    self.request_timeout = request_timeout

    channel = implementations.insecure_channel(host, port)
    self.stub = (
        windmill_service_pb2.beta_create_CloudWindmillServiceV1Alpha1_stub(
            channel))

  @retry.with_exponential_backoff()
  def GetWork(self, request):
    return self.stub.GetWork(request, self.request_timeout)

  @retry.with_exponential_backoff()
  def GetData(self, request):
    return self.stub.GetData(request, self.request_timeout)

  @retry.with_exponential_backoff()
  def CommitWork(self, request):
    return self.stub.CommitWork(request, self.request_timeout)

  @retry.with_exponential_backoff()
  def GetConfig(self, request):
    return self.stub.GetConfig(request, self.request_timeout)

  @retry.with_exponential_backoff()
  def ReportStats(self, request):
    return self.stub.ReportStats(request, self.request_timeout)
# pylint: enable=invalid-name


class StreamingWorker(object):
  """A streaming worker that communicates with Windmill."""

  # Maximum size of the result of a GetWork request.
  MAX_GET_WORK_FETCH_BYTES = 64 << 20  # 64m

  # Maximum number of items to return in a GetWork request.
  MAX_GET_WORK_ITEMS = 100

  # Delay to use before retrying work items locally, in seconds.
  RETRY_LOCALLY_DELAY = 10.0

  def __init__(self, properties):
    self.project_id = properties['project_id']
    self.job_id = properties['job_id']
    self.worker_id = properties['worker_id']

    self.client_id = random.getrandbits(63)
    windmill_host = properties['windmill.host']
    windmill_port = int(properties['windmill.grpc_port'])
    logging.info('Using gRPC to connect to Windmill at %s:%d.', windmill_host,
                 windmill_port)
    self.windmill = WindmillClient(windmill_host, windmill_port)

    self.instruction_map = {}
    self.system_name_to_computation_id_map = {}

  def run(self):
    self.running = True
    # TODO(ccy): support multi-threaded or multi-process execution.
    self.dispatch_loop()

  def get_work(self):
    request = windmill_pb2.GetWorkRequest(
        client_id=self.client_id,
        max_items=StreamingWorker.MAX_GET_WORK_ITEMS,
        max_bytes=StreamingWorker.MAX_GET_WORK_FETCH_BYTES)
    return self.windmill.GetWork(request)

  def add_computation(self, map_task):
    computation_id = self.system_name_to_computation_id_map.get(
        map_task.systemName, map_task.systemName)
    if computation_id not in self.instruction_map:
      self.instruction_map[computation_id] = map_task

  def parse_map_task(self, serialized_map_task):
    return apitools_base.JsonToMessage(dataflow.MapTask, serialized_map_task)

  def get_config(self, computation_id):
    """Load the config for a given computation from Windmill."""
    request = windmill_pb2.GetConfigRequest(computations=[computation_id])
    response = self.windmill.GetConfig(request)

    for map_entry in response.system_name_to_computation_id_map:
      self.system_name_to_computation_id_map[
          map_entry.system_name] = map_entry.computation_id
    for serialized_map_task in response.cloud_works:
      # Print the serialized version here as it's more readable.
      logging.info('Adding config for computation %s: %r', computation_id,
                   serialized_map_task)
      self.add_computation(self.parse_map_task(serialized_map_task))

    return response

  def dispatch_loop(self):
    while self.running:
      backoff_seconds = 0.001
      while self.running:
        work_response = self.get_work()
        if work_response.work:
          break
        time.sleep(backoff_seconds)
        backoff_seconds = min(1.0, backoff_seconds * 2)

      for computation_work in work_response.work:
        self.process_computation(computation_work)

  def process_computation(self, computation_work):
    computation_id = computation_work.computation_id
    input_data_watermark = windmillio.windmill_to_harness_timestamp(
        computation_work.input_data_watermark)
    if computation_id not in self.instruction_map:
      self.get_config(computation_id)
    map_task_proto = self.instruction_map[computation_id]
    for work_item in computation_work.work:
      retry_locally = True
      while retry_locally:
        try:
          self.process_work_item(computation_id, map_task_proto,
                                 input_data_watermark, work_item)
          break
        except:  # pylint: disable=bare-except
          logging.error(
              'Exception while processing work item for computation %r: '
              '%s, %s', computation_id, work_item, traceback.format_exc())

          # Send exception details to Windmill, retry locally if possible.
          retry_locally = self.report_failure(computation_id, work_item,
                                              sys.exc_info())

          # TODO(ccy): handle token expiration in retry logic.
          # TODO(ccy): handle out-of-memory error in retry logic.
          if retry_locally:
            logging.error('Execution of work in computation %s for key %r '
                          'failed; will retry locally.', computation_id,
                          work_item.key)
            time.sleep(StreamingWorker.RETRY_LOCALLY_DELAY)
          else:
            logging.error('Execution of work in computation %s for key %r '
                          'failed; Windmill indicated to not retry '
                          'locally.', computation_id, work_item.key)

  def report_failure(self, computation_id, work_item, exc_info):
    """Send exception details to Windmill; returns whether to retry locally."""
    exc_type, exc_value, exc_traceback = exc_info
    messages = list(line.strip() for line in
                    (traceback.format_exception_only(exc_type,
                                                     exc_value) +
                     traceback.format_tb(exc_traceback)))
    wm_exception = windmill_pb2.Exception(stack_frames=messages)
    report_stats_request = windmill_pb2.ReportStatsRequest(
        computation_id=computation_id,
        key=work_item.key,
        sharding_key=work_item.sharding_key,
        work_token=work_item.work_token,
        exceptions=[wm_exception])
    response = self.windmill.ReportStats(report_stats_request)
    return not response.failed

  def process_work_item(self, computation_id, map_task_proto,
                        input_data_watermark, work_item):
    """Process a work item."""
    workitem_commit_request = windmill_pb2.WorkItemCommitRequest(
        key=work_item.key,
        work_token=work_item.work_token)

    env = maptask.WorkerEnvironment()
    context = maptask.StreamingExecutionContext()

    reader = windmillstate.WindmillStateReader(
        computation_id,
        work_item.key,
        work_item.work_token,
        self.windmill)
    state_internals = windmillstate.WindmillStateInternals(reader)
    state = windmillstate.WindmillUnmergedState(state_internals)
    output_data_watermark = windmillio.windmill_to_harness_timestamp(
        work_item.output_data_watermark)

    context.start(computation_id, work_item, input_data_watermark,
                  output_data_watermark, workitem_commit_request,
                  self.windmill, state)

    map_task_executor = executor.MapTaskExecutor()
    map_task = maptask.decode_map_task(map_task_proto, env, context)

    map_task_executor.execute(map_task)
    state_internals.persist_to(workitem_commit_request)

    # Send result to Windmill.
    # TODO(ccy): in the future, this will not be done serially with respect to
    # work execution.
    commit_request = windmill_pb2.CommitWorkRequest()
    computation_commit_request = windmill_pb2.ComputationCommitWorkRequest(
        computation_id=computation_id,
        requests=[workitem_commit_request])
    commit_request.requests.extend([computation_commit_request])
    self.windmill.CommitWork(commit_request)
