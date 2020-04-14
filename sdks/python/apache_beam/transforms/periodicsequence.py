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

import time

import apache_beam as beam
import apache_beam.runners.sdf_utils as sdf_utils
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.transforms import core
from apache_beam.transforms import window
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils import timestamp
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp


class ImpulseSeqGenRestrictionProvider(core.RestrictionProvider):
  def initial_restriction(self, element):
    start, end, interval = element
    return OffsetRange(start - interval, end)

  def create_tracker(self, restriction):
    return ImpulseSeqGenRestrictionTracker(restriction)

  def restriction_size(self, unused_element, restriction):
    return restriction.size()


class ImpulseSeqGenRestrictionTracker(OffsetRestrictionTracker):
  def try_split(self, fraction_of_remainder):
    if not self._checkpointed:
      if fraction_of_remainder != 0:
        return None

      if self._current_position is None:
        cur = self._range.start
      else:
        cur = self._current_position
      split_point = cur

      if split_point < self._range.stop:
        self._checkpointed = True
        self._range, residual_range = self._range.split_at(split_point)
        return self._range, residual_range

  def cur_pos(self):
    return self._current_position

  def try_claim(self, pos):
    if ((self._last_claim_attempt is None) or
        (pos > self._last_claim_attempt and pos == self._range.stop)):
      self._last_claim_attempt = pos
      return True
    else:
      return super(ImpulseSeqGenRestrictionTracker, self).try_claim(pos)


class ImpulseSeqGenDoFn(beam.DoFn):
  '''
  ImpulseSeqGenDoFn fn receives tuple elements with three parts:

  * first_timestamp = first timestamp to output element for.
  * last_timestamp = last timestamp/time to output element for.
  * fire_interval = how often to fire an element.

  For each input element received, ImpulseSeqGenDoFn fn will start
  generating output elements in following pattern:

  * if element timestamp is less than current runtime then output element.
  * if element timestamp is greater than current runtime, wait until next
    element timestamp.

  ImpulseSeqGenDoFn can't guarantee that each element is output at exact time.
  ImpulseSeqGenDoFn guarantees that elements would not be output prior to
  given runtime timestamp.
  '''
  def process(
      self,
      element,
      restriction_tracker=beam.DoFn.RestrictionParam(
          ImpulseSeqGenRestrictionProvider())):
    '''
    :param element: (start_timestamp, end_timestamp, interval)
    :param restriction_tracker:
    :return: yields elements at processing real-time intervals with value of
      target output timestamp for the element.
    '''
    _, _, interval = element

    assert isinstance(restriction_tracker, sdf_utils.RestrictionTrackerView)

    current_time = time.time()
    restriction = restriction_tracker.current_restriction()
    current_output_timestamp = restriction.start

    restriction_tracker.try_claim(current_output_timestamp)
    if current_output_timestamp <= current_time:
      if restriction_tracker.try_claim(current_output_timestamp + interval):
        current_output_timestamp += interval
        yield current_output_timestamp

    if current_output_timestamp + interval >= restriction.stop:
      restriction_tracker.try_claim(restriction.stop)
    else:
      restriction_tracker.defer_remainder(
          timestamp.Timestamp(current_output_timestamp))


class PeriodicSequence(PTransform):
  '''
  PeriodicSequence transform receives tuple elements with three parts:

  * first_timestamp = first timestamp to output element for.
  * last_timestamp = last timestamp/time to output element for.
  * fire_interval = how often to fire an element.

  For each input element received, PeriodicSequence transform will start
  generating output elements in following pattern:

  * if element timestamp is less than current runtime then output element.
  * if element timestamp is greater than current runtime, wait until next
    element timestamp.

  PeriodicSequence can't guarantee that each element is output at exact time.
  PeriodicSequence guarantees that elements would not be output prior to given
  runtime timestamp.
  '''
  def __init_(self):
    pass

  def expand(self, pcoll):
    return (
        pcoll
        | 'GenSequence' >> beam.ParDo(ImpulseSeqGenDoFn())
        | 'MapToTimestamped' >> beam.Map(lambda tt: TimestampedValue(tt, tt)))


class PeriodicImpulse(PTransform):
  '''
  PeriodicImpulse transform generates an infinite sequence of elements with
  given runtime interval.

  PeriodicImpulse transform behaves same as {@link PeriodicSequence} transform,
  but can be used as first transform in pipeline.
  '''
  def __init__(
      self,
      start_timestamp=Timestamp.now(),
      stop_timestamp=MAX_TIMESTAMP,
      fire_interval=360.0,
      apply_windowing=False):
    '''
    :param start_timestamp: Timestamp for first element.
    :param stop_timestamp: Timestamp after which no elements will be output.
    :param fire_interval: Interval at which to output elements.
    :param apply_windowing: Whether each element should be assigned to
      individual window. If false, all elements will reside in global window.
    '''
    self.start_ts = start_timestamp
    self.stop_ts = stop_timestamp
    self.interval = fire_interval
    self.apply_windowing = apply_windowing

  def expand(self, pbegin):
    result = (
        pbegin
        | 'ImpulseElement' >> beam.Create(
            [(self.start_ts, self.stop_ts, self.interval)])
        | 'GenSequence' >> beam.ParDo(ImpulseSeqGenDoFn())
        | 'MapToTimestamped' >> beam.Map(lambda tt: TimestampedValue(tt, tt)))
    if self.apply_windowing:
      result = result | 'ApplyWindowing' >> beam.WindowInto(
          window.FixedWindows(self.interval))
    return result
