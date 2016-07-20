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

"""Core windowing data structures.
"""

# This module is carefully crafted to have optimal performance when
# compiled whiel still being valid Python.  Care needs to be taken when
# editing this file as WindowedValues are created for every element for
# every step in a Beam pipeline.

#cython: profile=True

from apache_beam.transforms.timeutil import MAX_TIMESTAMP
from apache_beam.transforms.timeutil import MIN_TIMESTAMP
from apache_beam.transforms.timeutil import Timestamp


class WindowedValue(object):
  """A windowed value having a value, a timestamp and set of windows.

  Attributes:
    value: The underlying value of a windowed value.
    timestamp: Timestamp associated with the value as seconds since Unix epoch.
    windows: A set (iterable) of window objects for the value. The window
      object are descendants of the BoundedWindow class.
  """

  def __init__(self, value, timestamp, windows):
    # For performance reasons, only timestamp_micros is stored by default
    # (as a C int). The Timestamp object is created on demand below.
    self.value = value
    if isinstance(timestamp, int):
      self.timestamp_micros = timestamp * 1000000
    else:
      self.timestamp_object = (timestamp if isinstance(timestamp, Timestamp)
                               else Timestamp.of(timestamp))
      self.timestamp_micros = self.timestamp_object.micros
    self.windows = windows

  @property
  def timestamp(self):
    if self.timestamp_object is None:
      self.timestamp_object = Timestamp(0, self.timestamp_micros)
    return self.timestamp_object

  def __repr__(self):
    return '(%s, %s, %s)' % (
        repr(self.value),
        'MIN_TIMESTAMP' if self.timestamp == MIN_TIMESTAMP else
        'MAX_TIMESTAMP' if self.timestamp == MAX_TIMESTAMP else
        float(self.timestamp),
        self.windows)

  def __hash__(self):
    return hash(self.value) + 3 * self.timestamp_micros + 7 * hash(self.windows)

  # We'd rather implement __eq__, but Cython supports that via __richcmp__
  # instead.  Fortunately __cmp__ is understood by both (but not by Python 3).
  def __cmp__(left, right):  # pylint: disable=no-self-argument
    """Compares left and right for equality.

    For performance reasons, doesn't actually impose an ordering
    on unequal values (always returning 1).
    """
    if type(left) is not type(right):
      return cmp(type(left), type(right))
    else:
      # TODO(robertwb): Avoid the type checks?
      # Returns False (0) if equal, and True (1) if not.
      return not WindowedValue._typed_eq(left, right)

  @staticmethod
  def _typed_eq(left, right):
    if (left.timestamp_micros == right.timestamp_micros
        and left.value == right.value
        and left.windows == right.windows):
      return True
    else:
      return False

  def with_value(self, new_value):
    """Creates a new WindowedValue with the same timestamps and windows as this.

    This is the fasted way to create a new WindowedValue.
    """
    return create(new_value, self.timestamp_micros, self.windows)

  def __reduce__(self):
    return WindowedValue, (self.value, self.timestamp, self.windows)


# TODO(robertwb): Move this to a static method.
def create(value, timestamp_micros, windows):
  wv = WindowedValue.__new__(WindowedValue)
  wv.value = value
  wv.timestamp_micros = timestamp_micros
  wv.windows = windows
  return wv


try:
  WindowedValue.timestamp_object = None
except TypeError:
  # When we're compiled, we can't dynamically add attributes to
  # the cdef class, but in this case it's OK as it's already present
  # on each instance.
  pass
