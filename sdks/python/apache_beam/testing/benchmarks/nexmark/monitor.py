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

from time import time

import apache_beam as beam
from apache_beam.metrics import Metrics


class Monitor(object):
  """
  A monitor of elements with support for later retrieving their metrics

  monitor objects contains a doFn to record metrics

  Args:
    namespace: the namespace all metrics within this Monitor uses
    name_prefix: a prefix for this Monitor's metrics' names, intended to
      be unique in per-monitor basis in pipeline
  """
  def __init__(self, namespace, name_prefix):
    # type: (str, str) -> None
    self.namespace = namespace
    self.name_prefix = name_prefix
    self.doFn = MonitorDoFn(namespace, name_prefix)


class MonitorDoFn(beam.DoFn):
  def __init__(self, namespace, prefix):
    self.element_count = Metrics.counter(
        namespace, prefix + MonitorSuffix.ELEMENT_COUNTER)
    self.event_time = Metrics.distribution(
        namespace, prefix + MonitorSuffix.EVENT_TIME)
    self.event_timestamp = Metrics.distribution(
        namespace, prefix + MonitorSuffix.EVENT_TIMESTAMP)

  def start_bundle(self):
    self.event_time.update(int(time() * 1000))

  def process(self, element, timestamp=beam.DoFn.TimestampParam):
    self.element_count.inc()
    self.event_timestamp.update(timestamp)
    yield element

  def finish_bundle(self):
    self.event_time.update(int(time() * 1000))


class MonitorSuffix:
  ELEMENT_COUNTER = '.elements'
  EVENT_TIMESTAMP = '.event_timestamp'
  EVENT_TIME = '.event_time'
