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

"""Worker utilities for parsing out a LeaseWorkItemResponse message.

The worker requests work items in a loop. Every response is a description of a
complex operation to be executed. For now only MapTask(s) are supported. These
tasks represent a sequence of ParallelInstruction(s): read from a source,
write to a sink, parallel do, etc.
"""

import threading

from google.cloud.dataflow.worker import maptask


class BatchWorkItem(object):
  """A work item wrapper over the work item proto returned by the service.

  Attributes:
    proto: The proto returned by the service for this work item. Some of the
      fields in the proto are surfaced as attributes of the wrapper class for
      convenience.
    map_task: The parsed MapTask object describing the work to perform.
    next_report_index: The reporting index (an int64) to be used when reporting
      status. This is returned in the response proto. If there are several
      status updates for the work item then each update response will contain
      the next reporting index to be used. This protocol is very important for
      the service to be able to handle update errors (missed, duplicated, etc.).
    lease_expire_time: UTC time (a string) when the lease will expire
      (e.g., '2015-06-17T17:22:49.999Z' or '2015-06-17T17:22:49Z' if zero
      milliseconds).
    report_status_interval: Duration (as a string) until a status update for the
      work item should be send back to the service (e.g., '5.000s' or '5s' if
      zero milliseconds).
  """

  def __init__(self, proto, map_task):
    self.proto = proto
    self.map_task = map_task
    # Lock to be acquired when reporting status (either reporting progress or
    # reporting completion). The attributes following the lock attribute (e.g.,
    # 'done', 'next_report_index', etc.) must be accessed using the lock because
    # the main worker thread executing a work item and the progress reporting
    # thread handling progress reports will modify them in parallel.
    self.lock = threading.Lock()
    self.done = False
    self.next_report_index = self.proto.initialReportIndex
    self.lease_expire_time = self.proto.leaseExpireTime
    self.report_status_interval = self.proto.reportStatusInterval

  def __str__(self):
    return '<%s %s steps=%s %s>' % (
        self.__class__.__name__, self.map_task.stage_name,
        '+'.join(self.map_task.step_names), self.proto.id)


def get_work_items(response, env=maptask.WorkerEnvironment(),
                   context=maptask.ExecutionContext()):
  """Parses a lease work item response into a list of Worker* objects.

  The response is received by the worker as a result of a LeaseWorkItem
  request to the Dataflow service.

  Args:
    response: A LeaseWorkItemResponse protobuf object returned by the service.
    env: An environment object with worker configuration.
    context: A maptask.ExecutionContext object providing context for operations
             to be executed.

  Returns:
    A tuple of work item id and the list of Worker* objects (see definitions
    above) representing the list of operations to be executed as part of the
    work item.
  """
  # Check if the request for work did not return anything.
  if not response.workItems:
    return None
  # For now service always sends one work item only.
  assert len(response.workItems) == 1
  work_item = response.workItems[0]
  map_task = maptask.decode_map_task(work_item.mapTask, env, context)
  return BatchWorkItem(work_item, map_task)
