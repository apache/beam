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

"""Utils for the io library.
* CountingSource: Subclass of iobase.BoundedSource. Used
on transforms.ptransform_test.test_read_metrics.
"""

# pytype: skip-file

from apache_beam.io import iobase
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.metrics import Metrics


class CountingSource(iobase.BoundedSource):
  def __init__(self, count):
    self.records_read = Metrics.counter(self.__class__, 'recordsRead')
    self._count = count

  def estimate_size(self):
    return self._count

  def get_range_tracker(self, start_position, stop_position):
    if start_position is None:
      start_position = 0
    if stop_position is None:
      stop_position = self._count

    return OffsetRangeTracker(start_position, stop_position)

  def read(self, range_tracker):
    for i in range(range_tracker.start_position(),
                   range_tracker.stop_position()):
      if not range_tracker.try_claim(i):
        return
      self.records_read.inc()
      yield i

  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    if start_position is None:
      start_position = 0
    if stop_position is None:
      stop_position = self._count

    bundle_start = start_position
    while bundle_start < stop_position:
      bundle_stop = min(stop_position, bundle_start + desired_bundle_size)
      yield iobase.SourceBundle(
          weight=(bundle_stop - bundle_start),
          source=self,
          start_position=bundle_start,
          stop_position=bundle_stop)
      bundle_start = bundle_stop
