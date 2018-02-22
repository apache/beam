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

"""Coder implementations.

The actual encode/decode implementations are split off from coders to
allow conditional (compiled/pure) implementations, which can be used to
encode many elements with minimal overhead.

This module may be optionally compiled with Cython, using the corresponding
coder_impl.pxd file for type hints.

For internal use only; no backwards-compatibility guarantees.
"""
from __future__ import absolute_import

from types import NoneType

import six

from apache_beam.coders import observable
from apache_beam.utils import windowed_value
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from .stream import InputStream as create_InputStream
  from .stream import OutputStream as create_OutputStream
  from .stream import ByteCountingOutputStream
  from .stream import get_varint_size
  globals()['create_InputStream'] = create_InputStream
  globals()['create_OutputStream'] = create_OutputStream
  globals()['ByteCountingOutputStream'] = ByteCountingOutputStream
except ImportError:
  from .slow_stream import InputStream as create_InputStream
  from .slow_stream import OutputStream as create_OutputStream
  from .slow_stream import ByteCountingOutputStream
  from .slow_stream import get_varint_size
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

try:
  long        # Python 2
except NameError:
  long = int  # Python 3


class CoderImpl(object):
  """For internal use only; no backwards-compatibility guarantees."""

  def encode_to_stream(self, value, stream, nested):
    """Reads object from potentially-nested encoding in stream."""
    raise NotImplementedError

  def decode_from_stream(self, stream, nested):
    """Reads object from potentially-nested encoding in stream."""
    raise NotImplementedError

  def encode(self, value):
    """Encodes an object to an unnested string."""
    raise NotImplementedError

  def decode(self, encoded):
    """Decodes an object to an unnested string."""
    raise NotImplementedError

  def encode_nested(self, value):
    out = create_OutputStream()
    self.encode_to_stream(value, out, True)
    return out.get()

  def decode_nested(self, encoded):
    return self.decode_from_stream(create_InputStream(encoded), True)

  def estimate_size(self, value, nested=False):
    """Estimates the encoded size of the given value, in bytes."""
    return self._get_nested_size(len(self.encode(value)), nested)

  def _get_nested_size(self, inner_size, nested):
    if not nested:
      return inner_size
    varint_size = get_varint_size(inner_size)
    return varint_size + inner_size

  def get_estimated_size_and_observables(self, value, nested=False):
    """Returns estimated size of value along with any nested observables.

    The list of nested observables is returned as a list of 2-tuples of
    (obj, coder_impl), where obj is an instance of observable.ObservableMixin,
    and coder_impl is the CoderImpl that can be used to encode elements sent by
    obj to its observers.

    Arguments:
      value: the value whose encoded size is to be estimated.
      nested: whether the value is nested.

    Returns:
      The estimated encoded size of the given value and a list of observables
      whose elements are 2-tuples of (obj, coder_impl) as described above.
    """
    return self.estimate_size(value, nested), []


class SimpleCoderImpl(CoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  Subclass of CoderImpl implementing stream methods using encode/decode."""

  def encode_to_stream(self, value, stream, nested):
    """Reads object from potentially-nested encoding in stream."""
    stream.write(self.encode(value), nested)

  def decode_from_stream(self, stream, nested):
    """Reads object from potentially-nested encoding in stream."""
    return self.decode(stream.read_all(nested))


class StreamCoderImpl(CoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  Subclass of CoderImpl implementing encode/decode using stream methods."""

  def encode(self, value):
    out = create_OutputStream()
    self.encode_to_stream(value, out, False)
    return out.get()

  def decode(self, encoded):
    return self.decode_from_stream(create_InputStream(encoded), False)

  def estimate_size(self, value, nested=False):
    """Estimates the encoded size of the given value, in bytes."""
    out = ByteCountingOutputStream()
    self.encode_to_stream(value, out, nested)
    return out.get_count()


class CallbackCoderImpl(CoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  A CoderImpl that calls back to the _impl methods on the Coder itself.

  This is the default implementation used if Coder._get_impl()
  is not overwritten.
  """

  def __init__(self, encoder, decoder, size_estimator=None):
    self._encoder = encoder
    self._decoder = decoder
    self._size_estimator = size_estimator or self._default_size_estimator

  def _default_size_estimator(self, value):
    return len(self.encode(value))

  def encode_to_stream(self, value, stream, nested):
    return stream.write(self._encoder(value), nested)

  def decode_from_stream(self, stream, nested):
    return self._decoder(stream.read_all(nested))

  def encode(self, value):
    return self._encoder(value)

  def decode(self, encoded):
    return self._decoder(encoded)

  def estimate_size(self, value, nested=False):
    return self._get_nested_size(self._size_estimator(value), nested)

  def get_estimated_size_and_observables(self, value, nested=False):
    # TODO(robertwb): Remove this once all coders are correct.
    if isinstance(value, observable.ObservableMixin):
      # CallbackCoderImpl can presumably encode the elements too.
      return 1, [(value, self)]

    return self.estimate_size(value, nested), []


class DeterministicFastPrimitivesCoderImpl(CoderImpl):
  """For internal use only; no backwards-compatibility guarantees."""

  def __init__(self, coder, step_label):
    self._underlying_coder = coder
    self._step_label = step_label

  def _check_safe(self, value):
    if isinstance(value, (str, six.text_type, long, int, float)):
      pass
    elif value is None:
      pass
    elif isinstance(value, (tuple, list)):
      for x in value:
        self._check_safe(x)
    else:
      raise TypeError(
          "Unable to deterministically code '%s' of type '%s', "
          "please provide a type hint for the input of '%s'" % (
              value, type(value), self._step_label))

  def encode_to_stream(self, value, stream, nested):
    self._check_safe(value)
    return self._underlying_coder.encode_to_stream(value, stream, nested)

  def decode_from_stream(self, stream, nested):
    return self._underlying_coder.decode_from_stream(stream, nested)

  def encode(self, value):
    self._check_safe(value)
    return self._underlying_coder.encode(value)

  def decode(self, encoded):
    return self._underlying_coder.decode(encoded)

  def estimate_size(self, value, nested=False):
    return self._underlying_coder.estimate_size(value, nested)

  def get_estimated_size_and_observables(self, value, nested=False):
    return self._underlying_coder.get_estimated_size_and_observables(
        value, nested)


class ProtoCoderImpl(SimpleCoderImpl):
  """For internal use only; no backwards-compatibility guarantees."""

  def __init__(self, proto_message_type):
    self.proto_message_type = proto_message_type

  def encode(self, value):
    return value.SerializeToString()

  def decode(self, encoded):
    proto_message = self.proto_message_type()
    proto_message.ParseFromString(encoded)
    return proto_message


UNKNOWN_TYPE = 0xFF
NONE_TYPE = 0
INT_TYPE = 1
FLOAT_TYPE = 2
STR_TYPE = 3
UNICODE_TYPE = 4
BOOL_TYPE = 9
LIST_TYPE = 5
TUPLE_TYPE = 6
DICT_TYPE = 7
SET_TYPE = 8


class FastPrimitivesCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees."""

  def __init__(self, fallback_coder_impl):
    self.fallback_coder_impl = fallback_coder_impl

  def get_estimated_size_and_observables(self, value, nested=False):
    if isinstance(value, observable.ObservableMixin):
      # FastPrimitivesCoderImpl can presumably encode the elements too.
      return 1, [(value, self)]

    out = ByteCountingOutputStream()
    self.encode_to_stream(value, out, nested)
    return out.get_count(), []

  def encode_to_stream(self, value, stream, nested):
    t = type(value)
    if t is NoneType:
      stream.write_byte(NONE_TYPE)
    elif t is int:
      stream.write_byte(INT_TYPE)
      stream.write_var_int64(value)
    elif t is float:
      stream.write_byte(FLOAT_TYPE)
      stream.write_bigendian_double(value)
    elif t is str:
      stream.write_byte(STR_TYPE)
      stream.write(value, nested)
    elif t is six.text_type:
      unicode_value = value  # for typing
      stream.write_byte(UNICODE_TYPE)
      stream.write(unicode_value.encode('utf-8'), nested)
    elif t is list or t is tuple or t is set:
      stream.write_byte(
          LIST_TYPE if t is list else TUPLE_TYPE if t is tuple else SET_TYPE)
      stream.write_var_int64(len(value))
      for e in value:
        self.encode_to_stream(e, stream, True)
    elif t is dict:
      dict_value = value  # for typing
      stream.write_byte(DICT_TYPE)
      stream.write_var_int64(len(dict_value))
      for k, v in dict_value.iteritems():
        self.encode_to_stream(k, stream, True)
        self.encode_to_stream(v, stream, True)
    elif t is bool:
      stream.write_byte(BOOL_TYPE)
      stream.write_byte(value)
    else:
      stream.write_byte(UNKNOWN_TYPE)
      self.fallback_coder_impl.encode_to_stream(value, stream, nested)

  def decode_from_stream(self, stream, nested):
    t = stream.read_byte()
    if t == NONE_TYPE:
      return None
    elif t == INT_TYPE:
      return stream.read_var_int64()
    elif t == FLOAT_TYPE:
      return stream.read_bigendian_double()
    elif t == STR_TYPE:
      return stream.read_all(nested)
    elif t == UNICODE_TYPE:
      return stream.read_all(nested).decode('utf-8')
    elif t == LIST_TYPE or t == TUPLE_TYPE or t == SET_TYPE:
      vlen = stream.read_var_int64()
      vlist = [self.decode_from_stream(stream, True) for _ in range(vlen)]
      if t == LIST_TYPE:
        return vlist
      elif t == TUPLE_TYPE:
        return tuple(vlist)
      return set(vlist)
    elif t == DICT_TYPE:
      vlen = stream.read_var_int64()
      v = {}
      for _ in range(vlen):
        k = self.decode_from_stream(stream, True)
        v[k] = self.decode_from_stream(stream, True)
      return v
    elif t == BOOL_TYPE:
      return not not stream.read_byte()

    return self.fallback_coder_impl.decode_from_stream(stream, nested)


class BytesCoderImpl(CoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  A coder for bytes/str objects."""

  def encode_to_stream(self, value, out, nested):
    out.write(value, nested)

  def decode_from_stream(self, in_stream, nested):
    return in_stream.read_all(nested)

  def encode(self, value):
    assert isinstance(value, bytes), (value, type(value))
    return value

  def decode(self, encoded):
    return encoded


class FloatCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees."""

  def encode_to_stream(self, value, out, nested):
    out.write_bigendian_double(value)

  def decode_from_stream(self, in_stream, nested):
    return in_stream.read_bigendian_double()

  def estimate_size(self, unused_value, nested=False):
    # A double is encoded as 8 bytes, regardless of nesting.
    return 8


class IntervalWindowCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees."""

  # TODO: Fn Harness only supports millis. Is this important enough to fix?
  def _to_normal_time(self, value):
    """Convert "lexicographically ordered unsigned" to signed."""
    return value - (1 << 63)

  def _from_normal_time(self, value):
    """Convert signed to "lexicographically ordered unsigned"."""
    return value + (1 << 63)

  def encode_to_stream(self, value, out, nested):
    span_micros = value.end.micros - value.start.micros
    out.write_bigendian_uint64(self._from_normal_time(value.end.micros / 1000))
    out.write_var_int64(span_micros / 1000)

  def decode_from_stream(self, in_, nested):
    end_millis = self._to_normal_time(in_.read_bigendian_uint64())
    start_millis = end_millis - in_.read_var_int64()
    from apache_beam.transforms.window import IntervalWindow
    ret = IntervalWindow(start=Timestamp(micros=start_millis * 1000),
                         end=Timestamp(micros=end_millis * 1000))
    return ret

  def estimate_size(self, value, nested=False):
    # An IntervalWindow is context-insensitive, with a timestamp (8 bytes)
    # and a varint timespam.
    span = value.end.micros - value.start.micros
    return 8 + get_varint_size(span / 1000)


class TimestampCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees."""

  def encode_to_stream(self, value, out, nested):
    out.write_bigendian_int64(value.micros)

  def decode_from_stream(self, in_stream, nested):
    return Timestamp(micros=in_stream.read_bigendian_int64())

  def estimate_size(self, unused_value, nested=False):
    # A Timestamp is encoded as a 64-bit integer in 8 bytes, regardless of
    # nesting.
    return 8


small_ints = [chr(_) for _ in range(128)]


class VarIntCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  A coder for long/int objects."""

  def encode_to_stream(self, value, out, nested):
    out.write_var_int64(value)

  def decode_from_stream(self, in_stream, nested):
    return in_stream.read_var_int64()

  def encode(self, value):
    ivalue = value  # type cast
    if 0 <= ivalue < len(small_ints):
      return small_ints[ivalue]
    return StreamCoderImpl.encode(self, value)

  def decode(self, encoded):
    if len(encoded) == 1:
      i = ord(encoded)
      if 0 <= i < 128:
        return i
    return StreamCoderImpl.decode(self, encoded)

  def estimate_size(self, value, nested=False):
    # Note that VarInts are encoded the same way regardless of nesting.
    return get_varint_size(value)


class SingletonCoderImpl(CoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  A coder that always encodes exactly one value."""

  def __init__(self, value):
    self._value = value

  def encode_to_stream(self, value, stream, nested):
    pass

  def decode_from_stream(self, stream, nested):
    return self._value

  def encode(self, value):
    b = ''  # avoid byte vs str vs unicode error
    return b

  def decode(self, encoded):
    return self._value

  def estimate_size(self, value, nested=False):
    return 0


class AbstractComponentCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  CoderImpl for coders that are comprised of several component coders."""

  def __init__(self, coder_impls):
    for c in coder_impls:
      assert isinstance(c, CoderImpl), c
    self._coder_impls = tuple(coder_impls)

  def _extract_components(self, value):
    raise NotImplementedError

  def _construct_from_components(self, components):
    raise NotImplementedError

  def encode_to_stream(self, value, out, nested):
    values = self._extract_components(value)
    if len(self._coder_impls) != len(values):
      raise ValueError(
          'Number of components does not match number of coders.')
    for i in range(0, len(self._coder_impls)):
      c = self._coder_impls[i]   # type cast
      c.encode_to_stream(values[i], out,
                         nested or i + 1 < len(self._coder_impls))

  def decode_from_stream(self, in_stream, nested):
    return self._construct_from_components(
        [c.decode_from_stream(in_stream,
                              nested or i + 1 < len(self._coder_impls))
         for i, c in enumerate(self._coder_impls)])

  def estimate_size(self, value, nested=False):
    """Estimates the encoded size of the given value, in bytes."""
    # TODO(ccy): This ignores sizes of observable components.
    estimated_size, _ = (
        self.get_estimated_size_and_observables(value))
    return estimated_size

  def get_estimated_size_and_observables(self, value, nested=False):
    """Returns estimated size of value along with any nested observables."""
    values = self._extract_components(value)
    estimated_size = 0
    observables = []
    for i in range(0, len(self._coder_impls)):
      c = self._coder_impls[i]  # type cast
      child_size, child_observables = (
          c.get_estimated_size_and_observables(
              values[i], nested=nested or i + 1 < len(self._coder_impls)))
      estimated_size += child_size
      observables += child_observables
    return estimated_size, observables


class TupleCoderImpl(AbstractComponentCoderImpl):
  """A coder for tuple objects."""

  def _extract_components(self, value):
    return value

  def _construct_from_components(self, components):
    return tuple(components)


class SequenceCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  A coder for sequences.

  If the length of the sequence in known we encode the length as a 32 bit
  ``int`` followed by the encoded bytes.

  If the length of the sequence is unknown, we encode the length as ``-1``
  followed by the encoding of elements buffered up to 64K bytes before prefixing
  the count of number of elements. A ``0`` is encoded at the end to indicate the
  end of stream.

  The resulting encoding would look like this::

    -1
    countA element(0) element(1) ... element(countA - 1)
    countB element(0) element(1) ... element(countB - 1)
    ...
    countX element(0) element(1) ... element(countX - 1)
    0

  """

  # Default buffer size of 64kB of handling iterables of unknown length.
  _DEFAULT_BUFFER_SIZE = 64 * 1024

  def __init__(self, elem_coder):
    self._elem_coder = elem_coder

  def _construct_from_sequence(self, values):
    raise NotImplementedError

  def encode_to_stream(self, value, out, nested):
    # Compatible with Java's IterableLikeCoder.
    if hasattr(value, '__len__'):
      out.write_bigendian_int32(len(value))
      for elem in value:
        self._elem_coder.encode_to_stream(elem, out, True)
    else:
      # We don't know the size without traversing it so use a fixed size buffer
      # and encode as many elements as possible into it before outputting
      # the size followed by the elements.

      # -1 to indicate that the length is not known.
      out.write_bigendian_int32(-1)
      buffer = create_OutputStream()
      prev_index = index = -1
      for index, elem in enumerate(value):
        self._elem_coder.encode_to_stream(elem, buffer, True)
        if out.size() > self._DEFAULT_BUFFER_SIZE:
          out.write_var_int64(index - prev_index)
          out.write(buffer.get())
          prev_index = index
          buffer = create_OutputStream()
      if index > prev_index:
        out.write_var_int64(index - prev_index)
        out.write(buffer.get())
      out.write_var_int64(0)

  def decode_from_stream(self, in_stream, nested):
    size = in_stream.read_bigendian_int32()

    if size >= 0:
      elements = [self._elem_coder.decode_from_stream(in_stream, True)
                  for _ in range(size)]
    else:
      elements = []
      count = in_stream.read_var_int64()
      while count > 0:
        for _ in range(count):
          elements.append(self._elem_coder.decode_from_stream(in_stream, True))
        count = in_stream.read_var_int64()

    return self._construct_from_sequence(elements)

  def estimate_size(self, value, nested=False):
    """Estimates the encoded size of the given value, in bytes."""
    # TODO(ccy): This ignores element sizes.
    estimated_size, _ = (
        self.get_estimated_size_and_observables(value))
    return estimated_size

  def get_estimated_size_and_observables(self, value, nested=False):
    """Returns estimated size of value along with any nested observables."""
    estimated_size = 0
    # Size of 32-bit integer storing number of elements.
    estimated_size += 4
    if isinstance(value, observable.ObservableMixin):
      return estimated_size, [(value, self._elem_coder)]

    observables = []
    for elem in value:
      child_size, child_observables = (
          self._elem_coder.get_estimated_size_and_observables(
              elem, nested=True))
      estimated_size += child_size
      observables += child_observables
    # TODO: (BEAM-1537) Update to use an accurate count depending on size and
    # count, currently we are underestimating the size by up to 10 bytes
    # per block of data since we are not including the count prefix which
    # occurs at most once per 64k of data and is upto 10 bytes long. The upper
    # bound of the underestimate is 10 / 65536 ~= 0.0153% of the actual size.
    return estimated_size, observables


class TupleSequenceCoderImpl(SequenceCoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  A coder for homogeneous tuple objects."""

  def _construct_from_sequence(self, components):
    return tuple(components)


class IterableCoderImpl(SequenceCoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  A coder for homogeneous iterable objects."""

  def _construct_from_sequence(self, components):
    return components


class WindowedValueCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  A coder for windowed values."""

  # Ensure that lexicographic ordering of the bytes corresponds to
  # chronological order of timestamps.
  # TODO(BEAM-1524): Clean this up once we have a BEAM wide consensus on
  # byte representation of timestamps.
  def _to_normal_time(self, value):
    """Convert "lexicographically ordered unsigned" to signed."""
    return value - (1 << 63)

  def _from_normal_time(self, value):
    """Convert signed to "lexicographically ordered unsigned"."""
    return value + (1 << 63)

  def __init__(self, value_coder, timestamp_coder, window_coder):
    # TODO(lcwik): Remove the timestamp coder field
    self._value_coder = value_coder
    self._timestamp_coder = timestamp_coder
    self._windows_coder = TupleSequenceCoderImpl(window_coder)

  def encode_to_stream(self, value, out, nested):
    wv = value  # type cast
    # Avoid creation of Timestamp object.
    restore_sign = -1 if wv.timestamp_micros < 0 else 1
    out.write_bigendian_uint64(
        # Convert to postive number and divide, since python rounds off to the
        # lower negative number. For ex: -3 / 2 = -2, but we expect it to be -1,
        # to be consistent across SDKs.
        # TODO(BEAM-1524): Clean this up once we have a BEAM wide consensus on
        # precision of timestamps.
        self._from_normal_time(
            restore_sign * (abs(wv.timestamp_micros) / 1000)))
    self._windows_coder.encode_to_stream(wv.windows, out, True)
    # Default PaneInfo encoded byte representing NO_FIRING.
    # TODO(BEAM-1522): Remove the hard coding here once PaneInfo is supported.
    out.write_byte(0xF)
    self._value_coder.encode_to_stream(wv.value, out, nested)

  def decode_from_stream(self, in_stream, nested):
    timestamp = self._to_normal_time(in_stream.read_bigendian_uint64())
    # Restore MIN/MAX timestamps to their actual values as encoding incurs loss
    # of precision while converting to millis.
    # Note: This is only a best effort here as there is no way to know if these
    # were indeed MIN/MAX timestamps.
    # TODO(BEAM-1524): Clean this up once we have a BEAM wide consensus on
    # precision of timestamps.
    if timestamp == -(abs(MIN_TIMESTAMP.micros) / 1000):
      timestamp = MIN_TIMESTAMP.micros
    elif timestamp == (MAX_TIMESTAMP.micros / 1000):
      timestamp = MAX_TIMESTAMP.micros
    else:
      timestamp *= 1000
      if timestamp > MAX_TIMESTAMP.micros:
        timestamp = MAX_TIMESTAMP.micros
      if timestamp < MIN_TIMESTAMP.micros:
        timestamp = MIN_TIMESTAMP.micros

    windows = self._windows_coder.decode_from_stream(in_stream, True)
    # Read PaneInfo encoded byte.
    # TODO(BEAM-1522): Ignored for now but should be converted to pane info once
    # it is supported.
    in_stream.read_byte()
    value = self._value_coder.decode_from_stream(in_stream, nested)
    return windowed_value.create(
        value,
        # Avoid creation of Timestamp object.
        timestamp,
        windows)

  def get_estimated_size_and_observables(self, value, nested=False):
    """Returns estimated size of value along with any nested observables."""
    if isinstance(value, observable.ObservableMixin):
      # Should never be here.
      # TODO(robertwb): Remove when coders are set correctly.
      return 0, [(value, self._value_coder)]
    estimated_size = 0
    observables = []
    value_estimated_size, value_observables = (
        self._value_coder.get_estimated_size_and_observables(
            value.value, nested=nested))
    estimated_size += value_estimated_size
    observables += value_observables
    estimated_size += (
        self._timestamp_coder.estimate_size(value.timestamp, nested=True))
    estimated_size += (
        self._windows_coder.estimate_size(value.windows, nested=True))
    # for pane info
    estimated_size += 1
    return estimated_size, observables


class LengthPrefixCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  Coder which prefixes the length of the encoded object in the stream."""

  def __init__(self, value_coder):
    self._value_coder = value_coder

  def encode_to_stream(self, value, out, nested):
    encoded_value = self._value_coder.encode(value)
    out.write_var_int64(len(encoded_value))
    out.write(encoded_value)

  def decode_from_stream(self, in_stream, nested):
    value_length = in_stream.read_var_int64()
    return self._value_coder.decode(in_stream.read(value_length))

  def estimate_size(self, value, nested=False):
    value_size = self._value_coder.estimate_size(value)
    return get_varint_size(value_size) + value_size
