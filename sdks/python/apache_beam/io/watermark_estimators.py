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
from apache_beam.transforms.core import WatermarkEstimatorProvider
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
      # TODO(BEAM-9312): Consider making it configurable to deal with late
      # timestamp.
      if timestamp < self._watermark:
        raise ValueError(
            'A MonotonicWatermarkEstimator expects output '
            'timestamp to be increasing monotonically.')
      self._watermark = timestamp

  def current_watermark(self):
    return self._watermark

  def get_estimator_state(self):
    return self._watermark

  @staticmethod
  def default_provider():
    """Provide a default WatermarkEstimatorProvider for
    MonotonicWatermarkEstimator.
    """
    class DefaultMonotonicWatermarkEstimator(WatermarkEstimatorProvider):
      def initial_estimator_state(self, element, restriction):
        return None

      def create_watermark_estimator(self, estimator_state):
        return MonotonicWatermarkEstimator(estimator_state)

    return DefaultMonotonicWatermarkEstimator()


class WalltimeWatermarkEstimator(WatermarkEstimator):
  """A WatermarkEstimator which uses processing time as the estimated watermark.
  """
  def __init__(self, timestamp=None):
    self._timestamp = timestamp or Timestamp.now()

  def observe_timestamp(self, timestamp):
    pass

  def current_watermark(self):
    self._timestamp = max(self._timestamp, Timestamp.now())
    return self._timestamp

  def get_estimator_state(self):
    return self._timestamp

  @staticmethod
  def default_provider():
    """Provide a default WatermarkEstimatorProvider for
    WalltimeWatermarkEstimator.
    """
    class DefaultWalltimeWatermarkEstimator(WatermarkEstimatorProvider):
      def initial_estimator_state(self, element, restriction):
        return None

      def create_watermark_estimator(self, estimator_state):
        return WalltimeWatermarkEstimator(estimator_state)

    return DefaultWalltimeWatermarkEstimator()


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
    # pylint: disable=line-too-long

    """Sets a timestamp before or at the timestamps of all future elements
    produced by the associated DoFn.

    This can be approximate. If records are output that violate this guarantee,
    they will be considered late, which will affect how they will be processed.
    See https://beam.apache.org/documentation/programming-guide/#watermarks-and-late-data
    for more information on late data and how to handle it.

    However, this value should be as late as possible. Downstream windows may
    not be able to close until this watermark passes their end.
    """
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

  @staticmethod
  def default_provider():
    """Provide a default WatermarkEstimatorProvider for
    WalltimeWatermarkEstimator.
    """
    class DefaultManualWatermarkEstimatorProvider(WatermarkEstimatorProvider):
      def initial_estimator_state(self, element, restriction):
        return None

      def create_watermark_estimator(self, estimator_state):
        return ManualWatermarkEstimator(estimator_state)

    return DefaultManualWatermarkEstimatorProvider()
