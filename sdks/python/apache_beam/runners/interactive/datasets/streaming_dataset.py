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
from __future__ import absolute_import

import json
import threading
import time
from datetime import datetime

import pytz

try:
  from weakref import finalize
except ImportError:
  from backports.weakref import finalize


class StreamingDataset(object):

  def __init__(self, name, pub_client, topic_path, **kwargs):
    if name not in ["timestamp"]:
      raise ValueError

    self.dataset_worker = TimestampPublisher(pub_client, topic_path, **kwargs)
    self._finalizer = finalize(self, lambda worker: worker.stop(),
                               self.dataset_worker)

  def start(self):
    self.dataset_worker.start()


class TimestampPublisher(threading.Thread):

  def __init__(self, pub_client, topic_path, time_between_events):
    """

    Args:
        time_between_events (float): Seconds
    """
    super(TimestampPublisher, self).__init__()
    self.pub_client = pub_client
    self.topic_path = topic_path
    self.time_between_events = time_between_events
    self._stop_event = threading.Event()

  def run(self):
    while not self.stopped():
      start_time = time.monotonic()
      timestamp = current_time_milliseconds(pytz.timezone("US/Pacific"))
      element = {"ts": timestamp}
      future = self.pub_client.publish(
          self.topic_path,
          json.dumps(element).encode("utf-8"),
          timestamp=str(timestamp),
      )
      _ = future.result()
      while (time.monotonic() - start_time) < self.time_between_events:
        time.sleep(0.015625)

  def stop(self):
    self._stop_event.set()

  def stopped(self):
    return self._stop_event.is_set()

  def __enter__(self):
    self.start()
    return self

  def __exit__(self, *args):
    self.stop()


def current_time_milliseconds(timezone=pytz.UTC):
  current_time = datetime.utcnow().replace(
      tzinfo=pytz.UTC).astimezone(timezone).replace(tzinfo=None)
  unix_time = (current_time - datetime.utcfromtimestamp(0)).total_seconds()
  # ReadFromPubSub expects timestamps to be in milliseconds
  unix_time_milliseconds = int(unix_time * 1000)
  return unix_time_milliseconds
