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

"""Timestamp utilities.

For internal use only; no backwards-compatibility guarantees.
"""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division

import datetime
import time
from builtins import object
from typing import Any
from typing import Union
from typing import overload

import dateutil.parser
import pytz
from google.protobuf import duration_pb2
from google.protobuf import timestamp_pb2
from past.builtins import long

from apache_beam.portability import common_urns

# types compatible with Timestamp.of()
TimestampTypes = Union[int, float, 'Timestamp']
# types compatible with Duration.of()
DurationTypes = Union[int, float, 'Duration']
TimestampDurationTypes = Union[int, float, 'Duration', 'Timestamp']


class Timestamp(object):
  """Represents a Unix second timestamp with microsecond granularity.

  Can be treated in common timestamp arithmetic operations as a numeric type.

  Internally stores a time interval as an int of microseconds. This strategy
  is necessary since floating point values lose precision when storing values,
  especially after arithmetic operations (for example, 10000000 % 0.1 evaluates
  to 0.0999999994448885).
  """
  def __init__(self, seconds: Union[int, float] = 0, micros: Union[int, float] = 0) -> None:
    if not isinstance(seconds, (int, long, float)):
      raise TypeError(
          'Cannot interpret %s %s as seconds.' % (seconds, type(seconds)))
    if not isinstance(micros, (int, long, float)):
      raise TypeError(
          'Cannot interpret %s %s as micros.' % (micros, type(micros)))
    self.micros = int(seconds * 1000000) + int(micros)

  @staticmethod
  def of(seconds: TimestampTypes) -> Timestamp:

    """Return the Timestamp for the given number of seconds.

    If the input is already a Timestamp, the input itself will be returned.

    Args:
      seconds: Number of seconds as int, float, long, or Timestamp.

    Returns:
      Corresponding Timestamp object.
    """

    if not isinstance(seconds, (int, long, float, Timestamp)):
      raise TypeError(
          'Cannot interpret %s %s as Timestamp.' % (seconds, type(seconds)))
    if isinstance(seconds, Timestamp):
      return seconds
    return Timestamp(seconds)

  @staticmethod
  def now():
    return Timestamp(seconds=time.time())

  @staticmethod
  def _epoch_datetime_utc():
    return datetime.datetime.fromtimestamp(0, pytz.utc)

  @classmethod
  def from_utc_datetime(cls, dt):
    """Create a ``Timestamp`` instance from a ``datetime.datetime`` object.

    Args:
      dt: A ``datetime.datetime`` object in UTC (offset-aware).
    """
    if dt.tzinfo != pytz.utc:
      raise ValueError('dt not in UTC: %s' % dt)
    duration = dt - cls._epoch_datetime_utc()
    return Timestamp(duration.total_seconds())

  @classmethod
  def from_rfc3339(cls, rfc3339):
    """Create a ``Timestamp`` instance from an RFC 3339 compliant string.

    .. note::
      All timezones are implicitly converted to UTC.

    Args:
      rfc3339: String in RFC 3339 form.
    """
    try:
      dt = dateutil.parser.isoparse(rfc3339).astimezone(pytz.UTC)
    except ValueError as e:
      raise ValueError(
          "Could not parse RFC 3339 string '{}' due to error: '{}'.".format(
              rfc3339, e))
    return cls.from_utc_datetime(dt)

  def predecessor(self):
    """Returns the largest timestamp smaller than self."""
    return Timestamp(micros=self.micros - 1)

  def __repr__(self):
    micros = self.micros
    sign = ''
    if micros < 0:
      sign = '-'
      micros = -micros
    int_part = micros // 1000000
    frac_part = micros % 1000000
    if frac_part:
      return 'Timestamp(%s%d.%06d)' % (sign, int_part, frac_part)
    return 'Timestamp(%s%d)' % (sign, int_part)

  def to_utc_datetime(self):
    # We can't easily construct a datetime object from microseconds, so we
    # create one at the epoch and add an appropriate timedelta interval.
    return self._epoch_datetime_utc().replace(tzinfo=None) + datetime.timedelta(
        microseconds=self.micros)

  def to_rfc3339(self):
    # Append 'Z' for UTC timezone.
    return self.to_utc_datetime().isoformat() + 'Z'

  def to_proto(self):
    """Returns the `google.protobuf.timestamp_pb2` representation."""
    secs = self.micros // 1000000
    nanos = (self.micros % 1000000) * 1000
    return timestamp_pb2.Timestamp(seconds=secs, nanos=nanos)

  @staticmethod
  def from_proto(timestamp_proto):
    """Creates a Timestamp from a `google.protobuf.timestamp_pb2`.

    Note that the google has a sub-second resolution of nanoseconds whereas this
    class has a resolution of microsends. This class will truncate the
    nanosecond resolution down to the microsecond.
    """

    if timestamp_proto.nanos % 1000 != 0:
      # TODO(BEAM-8738): Better define timestamps.
      raise ValueError(
          "Cannot convert from nanoseconds to microseconds " +
          "because this loses precision. Please make sure that " +
          "this is the correct behavior you want and manually " +
          "truncate the precision to the nearest microseconds. " +
          "See [BEAM-8738] for more information.")

    return Timestamp(
        seconds=timestamp_proto.seconds, micros=timestamp_proto.nanos // 1000)

  def __float__(self) -> float:
    # Note that the returned value may have lost precision.
    return self.micros / 1000000

  def __int__(self) -> int:
    # Note that the returned value may have lost precision.
    return self.micros // 1000000

  def __eq__(self, other: object) -> bool:
    # Allow comparisons between Duration and Timestamp values.
    if isinstance(other, (Duration, Timestamp)):
      return self.micros == other.micros
    elif isinstance(other, (int, long, float)):
      return self.micros == Timestamp.of(other).micros
    else:
      # Support equality with other types
      return NotImplemented

  def __ne__(self, other: Any) -> bool:
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __lt__(self, other: TimestampDurationTypes) -> bool:
    # Allow comparisons between Duration and Timestamp values.
    if not isinstance(other, Duration):
      other = Timestamp.of(other)
    return self.micros < other.micros

  def __gt__(self, other: TimestampDurationTypes) -> bool:
    return not (self < other or self == other)

  def __le__(self, other: TimestampDurationTypes) -> bool:
    return self < other or self == other

  def __ge__(self, other: TimestampDurationTypes) -> bool:
    return not self < other

  def __hash__(self):
    return hash(self.micros)

  def __add__(self, other: DurationTypes) -> Timestamp:
    other = Duration.of(other)
    return Timestamp(micros=self.micros + other.micros)

  def __radd__(self, other: DurationTypes) -> Timestamp:
    return self + other

  @overload
  def __sub__(self, other: DurationTypes) -> Timestamp:
    pass

  @overload
  def __sub__(self, other: Timestamp) -> Duration:
    pass

  def __sub__(self, other):
    if isinstance(other, Timestamp):
      return Duration(micros=self.micros - other.micros)
    other = Duration.of(other)
    return Timestamp(micros=self.micros - other.micros)

  def __mod__(self, other: DurationTypes) -> Duration:
    other = Duration.of(other)
    return Duration(micros=self.micros % other.micros)


MIN_TIMESTAMP = Timestamp(
    micros=int(common_urns.constants.MIN_TIMESTAMP_MILLIS.constant) * 1000)
MAX_TIMESTAMP = Timestamp(
    micros=int(common_urns.constants.MAX_TIMESTAMP_MILLIS.constant) * 1000)


class Duration(object):
  """Represents a second duration with microsecond granularity.

  Can be treated in common arithmetic operations as a numeric type.

  Internally stores a time interval as an int of microseconds. This strategy
  is necessary since floating point values lose precision when storing values,
  especially after arithmetic operations (for example, 10000000 % 0.1 evaluates
  to 0.0999999994448885).
  """
  def __init__(self, seconds: Union[int, float] = 0, micros: Union[int, float] = 0) -> None:
    self.micros = int(seconds * 1000000) + int(micros)

  @staticmethod
  def of(seconds: DurationTypes) -> Duration:

    """Return the Duration for the given number of seconds since Unix epoch.

    If the input is already a Duration, the input itself will be returned.

    Args:
      seconds: Number of seconds as int, float or Duration.

    Returns:
      Corresponding Duration object.
    """

    if isinstance(seconds, Timestamp):
      raise TypeError('Cannot interpret %s as Duration.' % seconds)
    if isinstance(seconds, Duration):
      return seconds
    return Duration(seconds)

  def to_proto(self):
    """Returns the `google.protobuf.duration_pb2` representation."""
    secs = self.micros // 1000000
    nanos = (self.micros % 1000000) * 1000
    return duration_pb2.Duration(seconds=secs, nanos=nanos)

  @staticmethod
  def from_proto(duration_proto):
    """Creates a Duration from a `google.protobuf.duration_pb2`.

    Note that the google has a sub-second resolution of nanoseconds whereas this
    class has a resolution of microsends. This class will truncate the
    nanosecond resolution down to the microsecond.
    """

    if duration_proto.nanos % 1000 != 0:
      # TODO(BEAM-8738): Better define durations.
      raise ValueError(
          "Cannot convert from nanoseconds to microseconds " +
          "because this loses precision. Please make sure that " +
          "this is the correct behavior you want and manually " +
          "truncate the precision to the nearest microseconds. " +
          "See [BEAM-8738] for more information.")

    return Duration(
        seconds=duration_proto.seconds, micros=duration_proto.nanos // 1000)

  def __repr__(self):
    micros = self.micros
    sign = ''
    if micros < 0:
      sign = '-'
      micros = -micros
    int_part = micros // 1000000
    frac_part = micros % 1000000
    if frac_part:
      return 'Duration(%s%d.%06d)' % (sign, int_part, frac_part)
    return 'Duration(%s%d)' % (sign, int_part)

  def __float__(self) -> float:
    # Note that the returned value may have lost precision.
    return self.micros / 1000000

  def __eq__(self, other: object) -> bool:
    # Allow comparisons between Duration and Timestamp values.
    if isinstance(other, (Duration, Timestamp)):
      return self.micros == other.micros
    elif isinstance(other, (int, long, float)):
      return self.micros == Duration.of(other).micros
    else:
      # Support equality with other types
      return NotImplemented

  def __ne__(self, other: Any) -> bool:
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __lt__(self, other: TimestampDurationTypes) -> bool:
    # Allow comparisons between Duration and Timestamp values.
    if not isinstance(other, Timestamp):
      other = Duration.of(other)
    return self.micros < other.micros

  def __gt__(self, other: TimestampDurationTypes) -> bool:
    return not (self < other or self == other)

  def __le__(self, other: TimestampDurationTypes) -> bool:
    return self < other or self == other

  def __ge__(self, other: TimestampDurationTypes) -> bool:
    return not self < other

  def __hash__(self):
    return hash(self.micros)

  def __neg__(self) -> Duration:
    return Duration(micros=-self.micros)

  def __add__(self, other: DurationTypes) -> Duration:
    if isinstance(other, Timestamp):
      # defer to Timestamp.__add__
      return NotImplemented
    other = Duration.of(other)
    return Duration(micros=self.micros + other.micros)

  def __radd__(self, other):
    return self + other

  def __sub__(self, other: DurationTypes) -> Duration:
    other = Duration.of(other)
    return Duration(micros=self.micros - other.micros)

  def __rsub__(self, other):
    return -(self - other)

  def __mul__(self, other: DurationTypes) -> Duration:
    other = Duration.of(other)
    return Duration(micros=self.micros * other.micros // 1000000)

  def __rmul__(self, other):
    return self * other

  def __mod__(self, other: DurationTypes) -> Duration:
    other = Duration.of(other)
    return Duration(micros=self.micros % other.micros)


# The minimum granularity / interval expressible in a Timestamp / Duration
# object.
TIME_GRANULARITY = Duration(micros=1)
