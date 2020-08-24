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
from apache_beam.testing.benchmarks.nexmark.nexmark_util import MonitorSuffix


class Monitor(object):
  """
  A monitor of elements with support for later retrieving their metrics
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

  def process(self, element):
    self.element_count.inc()
    self.event_time.update(int(time() * 1000))
    yield element
