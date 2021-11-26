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

"""Dataflow native sources and sinks.

For internal use only; no backwards-compatibility guarantees.
"""

# pytype: skip-file

import logging
from typing import TYPE_CHECKING
from typing import Optional

from apache_beam import pvalue
from apache_beam.io import iobase
from apache_beam.transforms import ptransform
from apache_beam.transforms.display import HasDisplayData

if TYPE_CHECKING:
  from apache_beam import coders

_LOGGER = logging.getLogger(__name__)


def _dict_printable_fields(dict_object, skip_fields):
  """Returns a list of strings for the interesting fields of a dict."""
  return [
      '%s=%r' % (name, value) for name,
      value in dict_object.items()
      # want to output value 0 but not None nor []
      if (value or value == 0) and name not in skip_fields
  ]


_minor_fields = [
    'coder',
    'key_coder',
    'value_coder',
    'config_bytes',
    'elements',
    'append_trailing_newlines',
    'strip_trailing_newlines',
    'compression_type'
]


class NativeSource(iobase.SourceBase):
  """A source implemented by Dataflow service.

  This class is to be only inherited by sources natively implemented by Cloud
  Dataflow service, hence should not be sub-classed by users.

  This class is deprecated and should not be used to define new sources.
  """
  coder = None  # type: Optional[coders.Coder]

  def reader(self):
    """Returns a NativeSourceReader instance associated with this source."""
    raise NotImplementedError

  def is_bounded(self):
    return True

  def __repr__(self):
    return '<{name} {vals}>'.format(
        name=self.__class__.__name__,
        vals=', '.join(_dict_printable_fields(self.__dict__, _minor_fields)))


class NativeSourceReader(object):
  """A reader for a source implemented by Dataflow service."""
  def __enter__(self):
    """Opens everything necessary for a reader to function properly."""
    raise NotImplementedError

  def __exit__(self, exception_type, exception_value, traceback):
    """Cleans up after a reader executed."""
    raise NotImplementedError

  def __iter__(self):
    """Returns an iterator over all the records of the source."""
    raise NotImplementedError

  @property
  def returns_windowed_values(self):
    """Returns whether this reader returns windowed values."""
    return False

  def get_progress(self):
    """Returns a representation of how far the reader has read.

    Returns:
      A SourceReaderProgress object that gives the current progress of the
      reader.
    """

  def request_dynamic_split(self, dynamic_split_request):
    """Attempts to split the input in two parts.

    The two parts are named the "primary" part and the "residual" part. The
    current 'NativeSourceReader' keeps processing the primary part, while the
    residual part will be processed elsewhere (e.g. perhaps on a different
    worker).

    The primary and residual parts, if concatenated, must represent the
    same input as the current input of this 'NativeSourceReader' before this
    call.

    The boundary between the primary part and the residual part is
    specified in a framework-specific way using 'DynamicSplitRequest' e.g.,
    if the framework supports the notion of positions, it might be a
    position at which the input is asked to split itself (which is not
    necessarily the same position at which it *will* split itself); it
    might be an approximate fraction of input, or something else.

    This function returns a 'DynamicSplitResult', which encodes, in a
    framework-specific way, the information sufficient to construct a
    description of the resulting primary and residual inputs. For example, it
    might, again, be a position demarcating these parts, or it might be a pair
    of fully-specified input descriptions, or something else.

    After a successful call to 'request_dynamic_split()', subsequent calls
    should be interpreted relative to the new primary.

    Args:
      dynamic_split_request: A 'DynamicSplitRequest' describing the split
        request.

    Returns:
      'None' if the 'DynamicSplitRequest' cannot be honored (in that
      case the input represented by this 'NativeSourceReader' stays the same),
      or a 'DynamicSplitResult' describing how the input was split into a
      primary and residual part.
    """
    _LOGGER.debug(
        'SourceReader %r does not support dynamic splitting. Ignoring dynamic '
        'split request: %r',
        self,
        dynamic_split_request)


class ReaderProgress(object):
  """A representation of how far a NativeSourceReader has read."""
  def __init__(
      self,
      position=None,
      percent_complete=None,
      remaining_time=None,
      consumed_split_points=None,
      remaining_split_points=None):

    self._position = position

    if percent_complete is not None:
      percent_complete = float(percent_complete)
      if percent_complete < 0 or percent_complete > 1:
        raise ValueError(
            'The percent_complete argument was %f. Must be in range [0, 1].' %
            percent_complete)
    self._percent_complete = percent_complete

    self._remaining_time = remaining_time
    self._consumed_split_points = consumed_split_points
    self._remaining_split_points = remaining_split_points

  @property
  def position(self):
    """Returns progress, represented as a ReaderPosition object."""
    return self._position

  @property
  def percent_complete(self):
    """Returns progress, represented as a percentage of total work.

    Progress range from 0.0 (beginning, nothing complete) to 1.0 (end of the
    work range, entire WorkItem complete).

    Returns:
      Progress represented as a percentage of total work.
    """
    return self._percent_complete

  @property
  def remaining_time(self):
    """Returns progress, represented as an estimated time remaining."""
    return self._remaining_time

  @property
  def consumed_split_points(self):
    return self._consumed_split_points

  @property
  def remaining_split_points(self):
    return self._remaining_split_points


class ReaderPosition(object):
  """A representation of position in an iteration of a 'NativeSourceReader'."""
  def __init__(
      self,
      end=None,
      key=None,
      byte_offset=None,
      record_index=None,
      shuffle_position=None,
      concat_position=None):
    """Initializes ReaderPosition.

    A ReaderPosition may get instantiated for one of these position types. Only
    one of these should be specified.

    Args:
      end: position is past all other positions. For example, this may be used
        to represent the end position of an unbounded range.
      key: position is a string key.
      byte_offset: position is a byte offset.
      record_index: position is a record index
      shuffle_position: position is a base64 encoded shuffle position.
      concat_position: position is a 'ConcatPosition'.
    """

    self.end = end
    self.key = key
    self.byte_offset = byte_offset
    self.record_index = record_index
    self.shuffle_position = shuffle_position

    if concat_position is not None:
      assert isinstance(concat_position, ConcatPosition)
    self.concat_position = concat_position


class ConcatPosition(object):
  """A position that encapsulate an inner position and an index.

  This is used to represent the position of a source that encapsulate several
  other sources.
  """
  def __init__(self, index, position):
    """Initializes ConcatPosition.

    Args:
      index: index of the source currently being read.
      position: inner position within the source currently being read.
    """

    if position is not None:
      assert isinstance(position, ReaderPosition)
    self.index = index
    self.position = position


class DynamicSplitRequest(object):
  """Specifies how 'NativeSourceReader.request_dynamic_split' should split.
  """
  def __init__(self, progress):
    assert isinstance(progress, ReaderProgress)
    self.progress = progress


class DynamicSplitResult(object):
  pass


class DynamicSplitResultWithPosition(DynamicSplitResult):
  def __init__(self, stop_position):
    assert isinstance(stop_position, ReaderPosition)
    self.stop_position = stop_position


class NativeSink(HasDisplayData):
  """A sink implemented by Dataflow service.

  This class is to be only inherited by sinks natively implemented by Cloud
  Dataflow service, hence should not be sub-classed by users.
  """
  def writer(self):
    """Returns a SinkWriter for this source."""
    raise NotImplementedError

  def __repr__(self):
    return '<{name} {vals}>'.format(
        name=self.__class__.__name__,
        vals=_dict_printable_fields(self.__dict__, _minor_fields))


class NativeSinkWriter(object):
  """A writer for a sink implemented by Dataflow service."""
  def __enter__(self):
    """Opens everything necessary for a writer to function properly."""
    raise NotImplementedError

  def __exit__(self, exception_type, exception_value, traceback):
    """Cleans up after a writer executed."""
    raise NotImplementedError

  @property
  def takes_windowed_values(self):
    """Returns whether this writer takes windowed values."""
    return False

  def Write(self, o):  # pylint: disable=invalid-name
    """Writes a record to the sink associated with this writer."""
    raise NotImplementedError


class _NativeWrite(ptransform.PTransform):
  """A PTransform for writing to a Dataflow native sink.

  These are sinks that are implemented natively by the Dataflow service
  and hence should not be updated by users. These sinks are processed
  using a Dataflow native write transform.

  Applying this transform results in a ``pvalue.PDone``.
  """
  def __init__(self, sink):
    """Initializes a Write transform.

    Args:
      sink: Sink to use for the write
    """
    super().__init__()
    self.sink = sink

  def expand(self, pcoll):
    self._check_pcollection(pcoll)
    return pvalue.PDone(pcoll.pipeline)
