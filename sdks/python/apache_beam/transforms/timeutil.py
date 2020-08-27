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

"""Timestamp utilities."""

# pytype: skip-file

from __future__ import absolute_import

from abc import ABCMeta
from abc import abstractmethod
from builtins import object

from future.utils import with_metaclass

from apache_beam.portability.api import beam_runner_api_pb2

__all__ = [
    'TimeDomain',
]


class TimeDomain(object):
  """Time domain for streaming timers."""

  WATERMARK = 'WATERMARK'
  REAL_TIME = 'REAL_TIME'
  DEPENDENT_REAL_TIME = 'DEPENDENT_REAL_TIME'

  _RUNNER_API_MAPPING = {
      WATERMARK: beam_runner_api_pb2.TimeDomain.EVENT_TIME,
      REAL_TIME: beam_runner_api_pb2.TimeDomain.PROCESSING_TIME,
      DEPENDENT_REAL_TIME: beam_runner_api_pb2.TimeDomain.
      SYNCHRONIZED_PROCESSING_TIME,
  }

  @staticmethod
  def from_string(domain):
    if domain in (TimeDomain.WATERMARK,
                  TimeDomain.REAL_TIME,
                  TimeDomain.DEPENDENT_REAL_TIME):
      return domain
    raise ValueError('Unknown time domain: %s' % domain)

  @staticmethod
  def to_runner_api(domain):
    return TimeDomain._RUNNER_API_MAPPING[domain]

  @staticmethod
  def is_event_time(domain):
    return TimeDomain.from_string(domain) == TimeDomain.WATERMARK


class TimestampCombinerImpl(with_metaclass(ABCMeta,
                                           object)):  # type: ignore[misc]
  """Implementation of TimestampCombiner."""
  @abstractmethod
  def assign_output_time(self, window, input_timestamp):
    raise NotImplementedError

  @abstractmethod
  def combine(self, output_timestamp, other_output_timestamp):
    raise NotImplementedError

  def combine_all(self, merging_timestamps):
    """Apply combine to list of timestamps."""
    combined_output_time = None
    for output_time in merging_timestamps:
      if combined_output_time is None:
        combined_output_time = output_time
      elif output_time is not None:
        combined_output_time = self.combine(combined_output_time, output_time)
    return combined_output_time

  def merge(self, unused_result_window, merging_timestamps):
    """Default to returning the result of combine_all."""
    return self.combine_all(merging_timestamps)


class DependsOnlyOnWindow(with_metaclass(ABCMeta, TimestampCombinerImpl)
                          ):  # type: ignore[misc]
  """TimestampCombinerImpl that only depends on the window."""
  def merge(self, result_window, unused_merging_timestamps):
    # Since we know that the result only depends on the window, we can ignore
    # the given timestamps.
    return self.assign_output_time(result_window, None)


class OutputAtEarliestInputTimestampImpl(TimestampCombinerImpl):
  """TimestampCombinerImpl outputting at earliest input timestamp."""
  def assign_output_time(self, window, input_timestamp):
    return input_timestamp

  def combine(self, output_timestamp, other_output_timestamp):
    """Default to returning the earlier of two timestamps."""
    return min(output_timestamp, other_output_timestamp)


class OutputAtEarliestTransformedInputTimestampImpl(TimestampCombinerImpl):
  """TimestampCombinerImpl outputting at earliest input timestamp."""
  def __init__(self, window_fn):
    self.window_fn = window_fn

  def assign_output_time(self, window, input_timestamp):
    return self.window_fn.get_transformed_output_time(window, input_timestamp)

  def combine(self, output_timestamp, other_output_timestamp):
    return min(output_timestamp, other_output_timestamp)


class OutputAtLatestInputTimestampImpl(TimestampCombinerImpl):
  """TimestampCombinerImpl outputting at latest input timestamp."""
  def assign_output_time(self, window, input_timestamp):
    return input_timestamp

  def combine(self, output_timestamp, other_output_timestamp):
    return max(output_timestamp, other_output_timestamp)


class OutputAtEndOfWindowImpl(DependsOnlyOnWindow):
  """TimestampCombinerImpl outputting at end of window."""
  def assign_output_time(self, window, unused_input_timestamp):
    return window.max_timestamp()

  def combine(self, output_timestamp, other_output_timestamp):
    return max(output_timestamp, other_output_timestamp)
