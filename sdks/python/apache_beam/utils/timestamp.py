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

from __future__ import absolute_import
from __future__ import division

import datetime


class Timestamp(object):
  """Represents a Unix second timestamp with microsecond granularity.

  Can be treated in common timestamp arithmetic operations as a numeric type.

  Internally stores a time interval as an int of microseconds. This strategy
  is necessary since floating point values lose precision when storing values,
  especially after arithmetic operations (for example, 10000000 % 0.1 evaluates
  to 0.0999999994448885).
  """

  def __init__(self, seconds=0, micros=0):
    self.micros = int(seconds * 1000000) + int(micros)

  @staticmethod
  def of(seconds):
    """Return the Timestamp for the given number of seconds.

    If the input is already a Timestamp, the input itself will be returned.

    Args:
      seconds: Number of seconds as int, float or Timestamp.

    Returns:
      Corresponding Timestamp object.
    """

    if isinstance(seconds, Duration):
      raise TypeError('Can\'t interpret %s as Timestamp.' % seconds)
    if isinstance(seconds, Timestamp):
      return seconds
    return Timestamp(seconds)

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
    epoch = datetime.datetime.utcfromtimestamp(0)
    # We can't easily construct a datetime object from microseconds, so we
    # create one at the epoch and add an appropriate timedelta interval.
    return epoch + datetime.timedelta(microseconds=self.micros)

  def isoformat(self):
    # Append 'Z' for UTC timezone.
    return self.to_utc_datetime().isoformat() + 'Z'

  def __float__(self):
    # Note that the returned value may have lost precision.
    return self.micros / 1000000

  def __int__(self):
    # Note that the returned value may have lost precision.
    return self.micros // 1000000

  def __cmp__(self, other):
    # Allow comparisons between Duration and Timestamp values.
    if not isinstance(other, Duration):
      other = Timestamp.of(other)
    return cmp(self.micros, other.micros)

  def __hash__(self):
    return hash(self.micros)

  def __add__(self, other):
    other = Duration.of(other)
    return Timestamp(micros=self.micros + other.micros)

  def __radd__(self, other):
    return self + other

  def __sub__(self, other):
    other = Duration.of(other)
    return Timestamp(micros=self.micros - other.micros)

  def __mod__(self, other):
    other = Duration.of(other)
    return Duration(micros=self.micros % other.micros)


MIN_TIMESTAMP = Timestamp(micros=-0x7fffffffffffffff - 1)
MAX_TIMESTAMP = Timestamp(micros=0x7fffffffffffffff)


class Duration(object):
  """Represents a second duration with microsecond granularity.

  Can be treated in common arithmetic operations as a numeric type.

  Internally stores a time interval as an int of microseconds. This strategy
  is necessary since floating point values lose precision when storing values,
  especially after arithmetic operations (for example, 10000000 % 0.1 evaluates
  to 0.0999999994448885).
  """

  def __init__(self, seconds=0, micros=0):
    self.micros = int(seconds * 1000000) + int(micros)

  @staticmethod
  def of(seconds):
    """Return the Duration for the given number of seconds since Unix epoch.

    If the input is already a Duration, the input itself will be returned.

    Args:
      seconds: Number of seconds as int, float or Duration.

    Returns:
      Corresponding Duration object.
    """

    if isinstance(seconds, Timestamp):
      raise TypeError('Can\'t interpret %s as Duration.' % seconds)
    if isinstance(seconds, Duration):
      return seconds
    return Duration(seconds)

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

  def __float__(self):
    # Note that the returned value may have lost precision.
    return self.micros / 1000000

  def __cmp__(self, other):
    # Allow comparisons between Duration and Timestamp values.
    if not isinstance(other, Timestamp):
      other = Duration.of(other)
    return cmp(self.micros, other.micros)

  def __hash__(self):
    return hash(self.micros)

  def __neg__(self):
    return Duration(micros=-self.micros)

  def __add__(self, other):
    if isinstance(other, Timestamp):
      return other + self
    other = Duration.of(other)
    return Duration(micros=self.micros + other.micros)

  def __radd__(self, other):
    return self + other

  def __sub__(self, other):
    other = Duration.of(other)
    return Duration(micros=self.micros - other.micros)

  def __rsub__(self, other):
    return -(self - other)

  def __mul__(self, other):
    other = Duration.of(other)
    return Duration(micros=self.micros * other.micros // 1000000)

  def __rmul__(self, other):
    return self * other

  def __mod__(self, other):
    other = Duration.of(other)
    return Duration(micros=self.micros % other.micros)


# The minimum granularity / interval expressible in a Timestamp / Duration
# object.
TIME_GRANULARITY = Duration(micros=1)
