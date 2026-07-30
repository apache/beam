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
# mypy: disallow-untyped-defs

import datetime
import re
import time
from typing import Optional
from typing import Union
from typing import overload

import dateutil.parser
import pytz
from google.protobuf import duration_pb2
from google.protobuf import timestamp_pb2

from apache_beam.portability import common_urns

# types compatible with Timestamp.of()
TimestampTypes = Union[int, float, 'Timestamp']
# types compatible with Duration.of()
DurationTypes = Union[int, float, 'Duration']
TimestampDurationTypes = Union[int, float, 'Duration', 'Timestamp']

# Powers of ten indexed by exponent
_POW_10 = {i: 10**i for i in range(10)}


class Timestamp(object):
  """Represents a Unix second timestamp with configurable subsecond precision.

  Can be treated in common timestamp arithmetic operations as a numeric type.

  Internally stores the timestamp as an int of floored seconds since the
  epoch plus a non-negative int subsecond value interpreted with the
  specified precision where (``0 <= subseconds < 10**precision``).

  Integer storage is necessary since floating point
  values lose precision when storing values, especially after arithmetic
  operations (for example, 10000000 % 0.1 evaluates to 0.0999999994448885).

  ``precision`` is the number of decimal digits used to represent the
  fraction of a second (e.g. 3 for millis, 6 for micros, 9 for
  nanos). Defaults to microseconds.
  If ``seconds`` is a float, the fractional part will be captured up
  to ``precision`` digits.

  Lossy conversion operations will throw an error unless
  ``allow_lossy_conversion=True`` is specified (e.g. see ``to_utc_datetime``).
  """
  MICROS_PRECISION = 6
  NANOS_PRECISION = 9

  def __init__(
      self,
      seconds: Union[int, float] = 0,
      subseconds: Union[int, float] = 0,
      precision: int = MICROS_PRECISION,
      *,
      micros: Optional[Union[int, float]] = None) -> None:
    if not isinstance(seconds, (int, float)):
      raise TypeError(
          'Cannot interpret %s %s as seconds.' % (seconds, type(seconds)))
    if not isinstance(subseconds, (int, float)):
      raise TypeError(
          'Cannot interpret %s %s as subseconds.' %
          (subseconds, type(subseconds)))
    if not isinstance(precision, int):
      raise TypeError(
          'Cannot interpret %s %s as precision.' % (precision, type(precision)))
    if not 0 <= precision <= Timestamp.NANOS_PRECISION:
      raise ValueError(
          'Timestamp precision must be between 0 and %d (inclusive), '
          'but was %d.' % (Timestamp.NANOS_PRECISION, precision))
    if micros is not None:
      if not isinstance(micros, (int, float)):
        raise TypeError(
            'Cannot interpret %s %s as micros.' % (micros, type(micros)))
      if subseconds:
        raise ValueError(
            'micros and subseconds are mutually exclusive, got micros=%s, '
            'subseconds=%s.' % (micros, subseconds))
      if precision != Timestamp.MICROS_PRECISION:
        raise ValueError(
            'micros implies microsecond precision (6) but precision was %d; '
            'use subseconds instead.' % precision)
      subseconds = micros
    self._precision = precision
    total = int(seconds * _POW_10[precision]) + int(subseconds)
    self._seconds, self._subseconds = divmod(total, _POW_10[precision])

  def _total(self, precision: int) -> int:
    """Returns the total time since the epoch in units of 10**-precision
    seconds.

    ``precision`` must be greater than or equal to this timestamp's
    precision, so that scaling up is always lossless.
    """
    return self._seconds * _POW_10[precision] + (
        self._subseconds * _POW_10[precision - self._precision])

  @staticmethod
  def of(seconds: TimestampTypes) -> 'Timestamp':
    """Return the Timestamp for the given number of seconds.

    If the input is already a Timestamp, the input itself will be returned.

    Args:
      seconds: Number of seconds as int, float, long, or Timestamp.

    Returns:
      Corresponding Timestamp object.
    """

    if isinstance(seconds, Timestamp):
      return seconds
    elif isinstance(seconds, (int, float)):
      return Timestamp(seconds)
    elif isinstance(seconds, datetime.datetime):
      return Timestamp.from_utc_datetime(seconds)
    else:
      raise TypeError(
          'Cannot interpret %s %s as Timestamp.' % (seconds, type(seconds)))

  @staticmethod
  def now() -> 'Timestamp':
    return Timestamp(seconds=time.time())

  @staticmethod
  def _epoch_datetime_utc() -> datetime.datetime:
    return datetime.datetime.fromtimestamp(0, pytz.utc)

  @classmethod
  def from_utc_datetime(cls, dt: datetime.datetime) -> 'Timestamp':
    """Create a ``Timestamp`` instance from a ``datetime.datetime`` object.

    Args:
      dt: A ``datetime.datetime`` object in UTC (offset-aware).
    """
    if dt.tzinfo is None:
      raise ValueError(
          "dt has no timezone info " +
          "(https://docs.python.org/3/library/datetime.html" +
          "#aware-and-naive-objects): %s" % dt)
    if dt.tzinfo != pytz.utc and dt.tzinfo != datetime.timezone.utc:
      raise ValueError('dt not in UTC: %s' % dt)
    duration = dt - cls._epoch_datetime_utc()
    return Timestamp(duration.total_seconds())

  @classmethod
  def from_rfc3339(cls, rfc3339: str) -> 'Timestamp':
    """Create a ``Timestamp`` instance from an RFC 3339 compliant string.

    Fractional seconds up to microseconds produce a microsecond-precision
    Timestamp; a longer fraction (up to nanoseconds) produces a Timestamp
    whose precision matches the number of fractional digits.

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
    timestamp = cls.from_utc_datetime(dt)
    # dateutil silently truncates fractional seconds to microseconds; parse
    # any sub-microsecond digits ourselves to avoid losing precision.
    fraction = re.search(r'[0-9]{2}[.,]([0-9]{7,})', rfc3339)
    if fraction:
      digits = fraction.group(1)
      if len(digits) > cls.NANOS_PRECISION:
        raise ValueError(
            "Could not parse RFC 3339 string '%s': fractional seconds "
            'beyond nanosecond precision are not supported.' % rfc3339)
      precision = len(digits)
      sub_micro = int(digits[cls.MICROS_PRECISION:])
      return Timestamp(
          timestamp.seconds(),
          timestamp.subseconds() * _POW_10[precision - cls.MICROS_PRECISION] +
          sub_micro,
          precision)
    return timestamp

  def seconds(self) -> int:
    """Returns the timestamp in seconds."""
    return self._seconds

  def subseconds(self) -> int:
    """Returns the fraction of a second, in units of 10**-precision seconds.

    Always non-negative and less than 10**precision
    """
    return self._subseconds

  def precision(self) -> int:
    """Returns the precision of this Timestamp."""
    return self._precision

  @property
  def micros(self) -> int:
    """Returns the total number of microseconds since the epoch."""
    if self._precision > Timestamp.MICROS_PRECISION:
      raise ValueError(
          '%r has greater than microsecond precision, converting it to '
          'micros may lose precision. Use to_precision(6, '
          'allow_lossy_conversion=True) to explicitly truncate it first, '
          'or use nanos instead.' % self)
    return self._total(Timestamp.MICROS_PRECISION)

  @property
  def nanos(self) -> int:
    """Returns the total number of nanoseconds since the epoch."""
    return self._total(Timestamp.NANOS_PRECISION)

  def to_precision(
      self,
      precision: int,
      allow_lossy_conversion: bool = False) -> 'Timestamp':
    """Returns this Timestamp converted to the given precision.

    Increasing precision is always lossless. Decreasing precision raises
    ValueError if this timestamp has a non-zero component below the target
    precision, unless allow_lossy_conversion is True, in which case the
    timestamp is truncated (floored) to the target precision.
    """
    if precision == self._precision:
      return self
    if not 0 <= precision <= Timestamp.NANOS_PRECISION:
      raise ValueError(
          'Timestamp precision must be between 0 and %d (inclusive), '
          'but was %d.' % (Timestamp.NANOS_PRECISION, precision))
    if precision > self._precision:
      scale = _POW_10[precision - self._precision]
      return Timestamp(self._seconds, self._subseconds * scale, precision)
    scale = _POW_10[self._precision - precision]
    remainder = self._subseconds % scale
    if remainder and not allow_lossy_conversion:
      raise ValueError(
          '%r cannot be represented exactly at precision %d. Set '
          'allow_lossy_conversion=True to truncate it.' % (self, precision))
    return Timestamp(self._seconds, self._subseconds // scale, precision)

  def predecessor(self) -> 'Timestamp':
    """Returns the largest timestamp smaller than self, at this precision."""
    return Timestamp(self._seconds, self._subseconds - 1, self._precision)

  def successor(self) -> 'Timestamp':
    """Returns the smallest timestamp larger than self, at this precision."""
    return Timestamp(self._seconds, self._subseconds + 1, self._precision)

  def __repr__(self) -> str:
    total = self._total(self._precision)
    sign = ''
    if total < 0:
      sign = '-'
      total = -total
    int_part, frac_part = divmod(total, _POW_10[self._precision])
    if frac_part:
      return 'Timestamp(%s%d.%0*d)' % (
          sign, int_part, self._precision, frac_part)
    return 'Timestamp(%s%d)' % (sign, int_part)

  def to_utc_datetime(
      self,
      has_tz: bool = False,
      allow_lossy_conversion: bool = False) -> datetime.datetime:
    """Returns a ``datetime.datetime`` object of UTC for this Timestamp.

    Note that this method returns a ``datetime.datetime`` object without a
    timezone info by default, as builtin `datetime.datetime.utcnow` method. If
    this is used as part of the processed data, one should set has_tz=True to
    avoid offset due to default timezone mismatch.

    Args:
      has_tz: whether the timezone info is attached, default to False.
      allow_lossy_conversion: must be set to True to convert a timestamp
        with precision above microseconds, since ``datetime.datetime`` only
        supports microsecond resolution; the result is truncated (floored)
        to whole microseconds.

    Returns:
      a ``datetime.datetime`` object of UTC for this Timestamp.

    Raises:
      ValueError: if this timestamp has precision above microseconds and
        allow_lossy_conversion is not True.
    """
    if self._precision > Timestamp.MICROS_PRECISION:
      if not allow_lossy_conversion:
        raise ValueError(
            'Converting %r to datetime truncates it to microseconds. Set '
            'allow_lossy_conversion=True to allow this conversion.' % self)
      micros_of_second = self._subseconds // _POW_10[self._precision -
                                                     Timestamp.MICROS_PRECISION]
    else:
      micros_of_second = self._subseconds * _POW_10[Timestamp.MICROS_PRECISION -
                                                    self._precision]
    # We can't easily construct a datetime object from microseconds, so we
    # create one at the epoch and add an appropriate timedelta interval.
    epoch = self._epoch_datetime_utc()
    if not has_tz:
      epoch = epoch.replace(tzinfo=None)
    return epoch + datetime.timedelta(
        seconds=self._seconds, microseconds=micros_of_second)

  def to_rfc3339(self) -> str:
    """Returns an RFC 3339 string for this Timestamp."""
    if self._precision <= Timestamp.MICROS_PRECISION:
      # Append 'Z' for UTC timezone.
      return self.to_utc_datetime().isoformat() + 'Z'
    # format the fractional second manually
    whole_second_datetime = self._epoch_datetime_utc().replace(
        tzinfo=None) + datetime.timedelta(seconds=self._seconds)
    result = whole_second_datetime.isoformat()
    if self._subseconds:
      result = result + '.%0*d' % (self._precision, self._subseconds)

    return result + 'Z'

  def to_proto(self) -> timestamp_pb2.Timestamp:
    """Returns the `google.protobuf.timestamp_pb2` representation."""
    return timestamp_pb2.Timestamp(
        seconds=self._seconds,
        nanos=self._subseconds *
        _POW_10[Timestamp.NANOS_PRECISION - self._precision])

  @staticmethod
  def from_proto(timestamp_proto: timestamp_pb2.Timestamp) -> 'Timestamp':
    """Creates a Timestamp from a `google.protobuf.timestamp_pb2`.

    The returned Timestamp has microsecond precision if the proto's nanos
    are microsecond-aligned, and nanosecond precision otherwise.
    """
    if timestamp_proto.nanos % 1000 != 0:
      return Timestamp(
          timestamp_proto.seconds,
          timestamp_proto.nanos,
          Timestamp.NANOS_PRECISION)
    return Timestamp(
        timestamp_proto.seconds,
        timestamp_proto.nanos // 1000,
        Timestamp.MICROS_PRECISION)

  def __float__(self) -> float:
    # Note that the returned value may have lost precision.
    return self._total(Timestamp.NANOS_PRECISION) / 1000000000

  def __int__(self) -> int:
    # Note that the returned value may have lost precision.
    return self._seconds

  def __eq__(self, other: object) -> bool:
    if isinstance(other, Timestamp):
      if self._precision == other._precision:
        return (
            self._seconds == other._seconds and
            self._subseconds == other._subseconds)
      precision = max(self._precision, other._precision)
      return self._total(precision) == other._total(precision)
    elif isinstance(other, Duration):
      # Allow comparisons between Duration and Timestamp values.
      return self._total(Timestamp.NANOS_PRECISION) == other.micros * 1000
    elif isinstance(other, (int, float)):
      return self == Timestamp.of(other)
    else:
      # Support equality with other types
      return NotImplemented

  def __lt__(self, other: TimestampDurationTypes) -> bool:
    # Allow comparisons between Duration and Timestamp values.
    if isinstance(other, Duration):
      return self._total(Timestamp.NANOS_PRECISION) < other.micros * 1000
    other = Timestamp.of(other)
    if self._seconds != other._seconds:
      return self._seconds < other._seconds
    if self._precision == other._precision:
      return self._subseconds < other._subseconds
    precision = max(self._precision, other._precision)
    return (
        self._subseconds * _POW_10[precision - self._precision]
        < other._subseconds * _POW_10[precision - other._precision])

  def __gt__(self, other: TimestampDurationTypes) -> bool:
    return not (self < other or self == other)

  def __le__(self, other: TimestampDurationTypes) -> bool:
    return self < other or self == other

  def __ge__(self, other: TimestampDurationTypes) -> bool:
    return not self < other

  def __hash__(self) -> int:
    # Normalized to max precision
    return hash(self._total(Timestamp.NANOS_PRECISION))

  def __add__(self, other: DurationTypes) -> 'Timestamp':
    other = Duration.of(other)
    precision = max(self._precision, Timestamp.MICROS_PRECISION)
    return Timestamp(
        subseconds=self._total(precision) +
        other.micros * _POW_10[precision - Timestamp.MICROS_PRECISION],
        precision=precision)

  def __radd__(self, other: DurationTypes) -> 'Timestamp':
    return self + other

  @overload
  def __sub__(self, other: DurationTypes) -> 'Timestamp':
    pass

  @overload
  def __sub__(self, other: 'Timestamp') -> 'Duration':
    pass

  def __sub__(
      self, other: Union[DurationTypes,
                         'Timestamp']) -> Union['Timestamp', 'Duration']:
    if isinstance(other, Timestamp):
      diff_nanos = (
          self._total(Timestamp.NANOS_PRECISION) -
          other._total(Timestamp.NANOS_PRECISION))
      if diff_nanos % 1000 != 0:
        raise ValueError(
            'The difference of %r and %r has sub-microsecond precision, '
            'which Duration cannot represent. Truncate the operands with '
            'to_precision(6, allow_lossy_conversion=True) first.' %
            (self, other))
      return Duration(micros=diff_nanos // 1000)
    other = Duration.of(other)
    precision = max(self._precision, Timestamp.MICROS_PRECISION)
    return Timestamp(
        subseconds=self._total(precision) -
        other.micros * _POW_10[precision - Timestamp.MICROS_PRECISION],
        precision=precision)

  def __mod__(self, other: DurationTypes) -> 'Duration':
    other = Duration.of(other)
    remainder_nanos = self._total(Timestamp.NANOS_PRECISION) % (
        other.micros * 1000)
    if remainder_nanos % 1000 != 0:
      raise ValueError(
          'The remainder of %r modulo %r has sub-microsecond precision, '
          'which Duration cannot represent. Truncate this timestamp with '
          'to_precision(6, allow_lossy_conversion=True) first.' % (self, other))
    return Duration(micros=remainder_nanos // 1000)


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
  def __init__(
      self,
      seconds: Union[int, float] = 0,
      micros: Union[int, float] = 0) -> None:
    self.micros = int(seconds * 1000000) + int(micros)

  @staticmethod
  def of(seconds: DurationTypes) -> 'Duration':
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

  def to_proto(self) -> duration_pb2.Duration:
    """Returns the `google.protobuf.duration_pb2` representation."""
    secs = self.micros // 1000000
    nanos = (self.micros % 1000000) * 1000
    return duration_pb2.Duration(seconds=secs, nanos=nanos)

  @staticmethod
  def from_proto(duration_proto: duration_pb2.Duration) -> 'Duration':
    """Creates a Duration from a `google.protobuf.duration_pb2`.

    Note that the google has a sub-second resolution of nanoseconds whereas this
    class has a resolution of microsends. This class will truncate the
    nanosecond resolution down to the microsecond.
    """

    if duration_proto.nanos % 1000 != 0:
      # TODO(https://github.com/apache/beam/issues/19922): Better define
      # durations.
      raise ValueError(
          "Cannot convert from nanoseconds to microseconds " +
          "because this loses precision. Please make sure that " +
          "this is the correct behavior you want and manually " +
          "truncate the precision to the nearest microseconds. " +
          "See [https://github.com/apache/beam/issues/19922] for " +
          "more information.")

    return Duration(
        seconds=duration_proto.seconds, micros=duration_proto.nanos // 1000)

  def __repr__(self) -> str:
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
    if isinstance(other, Duration):
      return self.micros == other.micros
    elif isinstance(other, Timestamp):
      return self.micros * 1000 == other.nanos
    elif isinstance(other, (int, float)):
      return self.micros == Duration.of(other).micros
    else:
      # Support equality with other types
      return NotImplemented

  def __lt__(self, other: TimestampDurationTypes) -> bool:
    # Allow comparisons between Duration and Timestamp values.
    if isinstance(other, Timestamp):
      return self.micros * 1000 < other.nanos
    other = Duration.of(other)
    return self.micros < other.micros

  def __gt__(self, other: TimestampDurationTypes) -> bool:
    return not (self < other or self == other)

  def __le__(self, other: TimestampDurationTypes) -> bool:
    return self < other or self == other

  def __ge__(self, other: TimestampDurationTypes) -> bool:
    return not self < other

  def __hash__(self) -> int:
    # Timestamps hash on their total nanoseconds. Hash equivalently so that
    # a Duration and Timestamp that compare equal also hash equal.
    return hash(self.micros * 1000)

  def __neg__(self) -> 'Duration':
    return Duration(micros=-self.micros)

  def __add__(self, other: DurationTypes) -> 'Duration':
    if isinstance(other, Timestamp):
      # defer to Timestamp.__add__
      return NotImplemented
    other = Duration.of(other)
    return Duration(micros=self.micros + other.micros)

  def __radd__(self, other: DurationTypes) -> 'Duration':
    return self + other

  def __sub__(self, other: DurationTypes) -> 'Duration':
    other = Duration.of(other)
    return Duration(micros=self.micros - other.micros)

  def __rsub__(self, other: DurationTypes) -> 'Duration':
    return -(self - other)

  def __mul__(self, other: DurationTypes) -> 'Duration':
    other = Duration.of(other)
    return Duration(micros=self.micros * other.micros // 1000000)

  def __rmul__(self, other: DurationTypes) -> 'Duration':
    return self * other

  def __mod__(self, other: DurationTypes) -> 'Duration':
    other = Duration.of(other)
    return Duration(micros=self.micros % other.micros)


# The minimum granularity / interval expressible in a Timestamp / Duration
# object.
TIME_GRANULARITY = Duration(micros=1)
