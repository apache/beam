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

"""A collection of WatermarkEstimator implementations that SplittableDoFns
can use."""

# pytype: skip-file

from __future__ import absolute_import

from apache_beam.io.iobase import WatermarkEstimator
from apache_beam.utils.timestamp import Timestamp


class MonotonicWatermarkEstimator(WatermarkEstimator):
  """A WatermarkEstimator which assumes that timestamps of all ouput records
  are increasing monotonically.
  """
  def __init__(self, timestamp):
    """For a new <element, restriction> pair, the initial value is None. When
    resuming processing, the initial timestamp will be the last reported
    watermark.
    """
    self._watermark = timestamp

  def observe_timestamp(self, timestamp):
    if self._watermark is None:
      self._watermark = timestamp
    else:
      if timestamp < self._watermark:
        raise ValueError(
            'A MonotonicWatermarkEstimator expects output '
            'timestamp to be increasing monotonically.')
      self._watermark = timestamp

  def current_watermark(self):
    return self._watermark

  def get_estimator_state(self):
    return self._watermark


class WalltimeWatermarkEstimator(WatermarkEstimator):
  """A WatermarkEstimator which uses processing time as the estimated watermark.
  """
  def __init__(self, timestamp=None):
    if timestamp:
      self._timestamp = timestamp
    else:
      self._timestamp = Timestamp.now()

  def observe_timestamp(self, timestamp):
    pass

  def current_watermark(self):
    self._timestamp = max(self._timestamp, Timestamp.now())
    return self._timestamp

  def get_estimator_state(self):
    return self._timestamp


class ManualWatermarkEstimator(WatermarkEstimator):
  """A WatermarkEstimator which is controlled manually from within a DoFn.

  The DoFn must invoke set_watermark to advance the watermark.
  """
  def __init__(self, watermark):
    self._watermark = watermark

  def observe_timestamp(self, timestamp):
    pass

  def current_watermark(self):
    return self._watermark

  def get_estimator_state(self):
    return self._watermark

  def set_watermark(self, timestamp):
    if not isinstance(timestamp, Timestamp):
      raise ValueError('set_watermark expects a Timestamp as input')
    if self._watermark and self._watermark > timestamp:
      raise ValueError(
          'Watermark must be monotonically increasing.'
          'Provided watermark %s is less than '
          'current watermark %s',
          timestamp,
          self._watermark)
    self._watermark = timestamp
