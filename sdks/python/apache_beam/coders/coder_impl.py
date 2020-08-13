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

# cython: language_level=3

"""Coder implementations.

The actual encode/decode implementations are split off from coders to
allow conditional (compiled/pure) implementations, which can be used to
encode many elements with minimal overhead.

This module may be optionally compiled with Cython, using the corresponding
coder_impl.pxd file for type hints.

Py2/3 porting: Native range is used on both python versions instead of
future.builtins.range to avoid performance regression in Cython compiled code.

For internal use only; no backwards-compatibility guarantees.
"""
# pytype: skip-file

from __future__ import absolute_import
from __future__ import division

import json
from builtins import chr
from builtins import object
from io import BytesIO
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

from fastavro import parse_schema
from fastavro import schemaless_reader
from fastavro import schemaless_writer
from past.builtins import unicode as past_unicode
from past.builtins import long

from apache_beam.coders import observable
from apache_beam.coders.avro_record import AvroRecord
from apache_beam.utils import windowed_value
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

if TYPE_CHECKING:
  from apache_beam.transforms.window import IntervalWindow

try:
  from . import stream  # pylint: disable=unused-import
except ImportError:
  SLOW_STREAM = True
else:
  SLOW_STREAM = False

if TYPE_CHECKING or SLOW_STREAM:
  from .slow_stream import InputStream as create_InputStream
  from .slow_stream import OutputStream as create_OutputStream
  from .slow_stream import ByteCountingOutputStream
  from .slow_stream import get_varint_size

  if False:  # pylint: disable=using-constant-test
    # This clause is interpreted by the compiler.
    from cython import compiled as is_compiled
  else:
    is_compiled = False
    fits_in_64_bits = lambda x: -(1 << 63) <= x <= (1 << 63) - 1

else:
  # pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
  from .stream import InputStream as create_InputStream
  from .stream import OutputStream as create_OutputStream
  from .stream import ByteCountingOutputStream
  from .stream import get_varint_size
  # Make it possible to import create_InputStream and other cdef-classes
  # from apache_beam.coders.coder_impl when Cython codepath is used.
  globals()['create_InputStream'] = create_InputStream
  globals()['create_OutputStream'] = create_OutputStream
  globals()['ByteCountingOutputStream'] = ByteCountingOutputStream
  # pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

_TIME_SHIFT = 1 << 63
MIN_TIMESTAMP_micros = MIN_TIMESTAMP.micros
MAX_TIMESTAMP_micros = MAX_TIMESTAMP.micros

IterableStateReader = Callable[[bytes, 'CoderImpl'], Iterable]
IterableStateWriter = Callable[[Iterable, 'CoderImpl'], bytes]
Observables = List[Tuple[observable.ObservableMixin, 'CoderImpl']]


class CoderImpl(object):
  """For internal use only; no backwards-compatibility guarantees."""
  def encode_to_stream(self, value, stream, nested):
    # type: (Any, create_OutputStream, bool) -> None

    """Reads object from potentially-nested encoding in stream."""
    raise NotImplementedError

  def decode_from_stream(self, stream, nested):
    # type: (create_InputStream, bool) -> Any

    """Reads object from potentially-nested encoding in stream."""
    raise NotImplementedError

  def encode(self, value):
    # type: (Any) -> bytes

    """Encodes an object to an unnested string."""
    raise NotImplementedError

  def decode(self, encoded):
    # type: (bytes) -> Any

    """Decodes an object to an unnested string."""
    raise NotImplementedError

  def encode_all(self, values):
    # type: (Iterable[Any]) -> bytes
    out = create_OutputStream()
    for value in values:
      self.encode_to_stream(value, out, True)
    return out.get()

  def decode_all(self, encoded):
    # type: (bytes) -> Iterator[Any]
    input_stream = create_InputStream(encoded)
    while input_stream.size() > 0:
      yield self.decode_from_stream(input_stream, True)

  def encode_nested(self, value):
    # type: (Any) -> bytes
    out = create_OutputStream()
    self.encode_to_stream(value, out, True)
    return out.get()

  def decode_nested(self, encoded):
    # type: (bytes) -> Any
    return self.decode_from_stream(create_InputStream(encoded), True)

  def estimate_size(self, value, nested=False):
    # type: (Any, bool) -> int

    """Estimates the encoded size of the given value, in bytes."""
    out = ByteCountingOutputStream()
    self.encode_to_stream(value, out, nested)
    return out.get_count()

  def _get_nested_size(self, inner_size, nested):
    if not nested:
      return inner_size
    varint_size = get_varint_size(inner_size)
    return varint_size + inner_size

  def get_estimated_size_and_observables(self, value, nested=False):
    # type: (Any, bool) -> Tuple[int, Observables]

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
    # type: (Any, create_OutputStream, bool) -> None

    """Reads object from potentially-nested encoding in stream."""
    stream.write(self.encode(value), nested)

  def decode_from_stream(self, stream, nested):
    # type: (create_InputStream, bool) -> Any

    """Reads object from potentially-nested encoding in stream."""
    return self.decode(stream.read_all(nested))


class StreamCoderImpl(CoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  Subclass of CoderImpl implementing encode/decode using stream methods."""
  def encode(self, value):
    # type: (Any) -> bytes
    out = create_OutputStream()
    self.encode_to_stream(value, out, False)
    return out.get()

  def decode(self, encoded):
    # type: (bytes) -> Any
    return self.decode_from_stream(create_InputStream(encoded), False)

  def estimate_size(self, value, nested=False):
    # type: (Any, bool) -> int

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
    # type: (Any, create_OutputStream, bool) -> None
    return stream.write(self._encoder(value), nested)

  def decode_from_stream(self, stream, nested):
    # type: (create_InputStream, bool) -> Any
    return self._decoder(stream.read_all(nested))

  def encode(self, value):
    return self._encoder(value)

  def decode(self, encoded):
    return self._decoder(encoded)

  def estimate_size(self, value, nested=False):
    # type: (Any, bool) -> int
    return self._get_nested_size(self._size_estimator(value), nested)

  def get_estimated_size_and_observables(self, value, nested=False):
    # type: (Any, bool) -> Tuple[int, Observables]
    # TODO(robertwb): Remove this once all coders are correct.
    if isinstance(value, observable.ObservableMixin):
      # CallbackCoderImpl can presumably encode the elements too.
      return 1, [(value, self)]

    return self.estimate_size(value, nested), []

  def __repr__(self):
    return 'CallbackCoderImpl[encoder=%s, decoder=%s]' % (
        self._encoder, self._decoder)


class DeterministicFastPrimitivesCoderImpl(CoderImpl):
  """For internal use only; no backwards-compatibility guarantees."""
  def __init__(self, coder, step_label):
    self._underlying_coder = coder
    self._step_label = step_label

  def _check_safe(self, value):
    if isinstance(value, (bytes, past_unicode, long, int, float)):
      pass
    elif value is None:
      pass
    elif isinstance(value, (tuple, list)):
      for x in value:
        self._check_safe(x)
    else:
      raise TypeError(
          "Unable to deterministically code '%s' of type '%s', "
          "please provide a type hint for the input of '%s'" %
          (value, type(value), self._step_label))

  def encode_to_stream(self, value, stream, nested):
    # type: (Any, create_OutputStream, bool) -> None
    self._check_safe(value)
    return self._underlying_coder.encode_to_stream(value, stream, nested)

  def decode_from_stream(self, stream, nested):
    # type: (create_InputStream, bool) -> Any
    return self._underlying_coder.decode_from_stream(stream, nested)

  def encode(self, value):
    self._check_safe(value)
    return self._underlying_coder.encode(value)

  def decode(self, encoded):
    return self._underlying_coder.decode(encoded)

  def estimate_size(self, value, nested=False):
    # type: (Any, bool) -> int
    return self._underlying_coder.estimate_size(value, nested)

  def get_estimated_size_and_observables(self, value, nested=False):
    # type: (Any, bool) -> Tuple[int, Observables]
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


class DeterministicProtoCoderImpl(ProtoCoderImpl):
  """For internal use only; no backwards-compatibility guarantees."""
  def encode(self, value):
    return value.SerializeToString(deterministic=True)


UNKNOWN_TYPE = 0xFF
NONE_TYPE = 0
INT_TYPE = 1
FLOAT_TYPE = 2
BYTES_TYPE = 3
UNICODE_TYPE = 4
BOOL_TYPE = 9
LIST_TYPE = 5
TUPLE_TYPE = 6
DICT_TYPE = 7
SET_TYPE = 8
ITERABLE_LIKE_TYPE = 10

# Types that can be encoded as iterables, but are not literally
# lists, etc. due to being lazy.  The actual type is not preserved
# through encoding, only the elements. This is particularly useful
# for the value list types created in GroupByKey.
_ITERABLE_LIKE_TYPES = set()


class FastPrimitivesCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees."""
  def __init__(self, fallback_coder_impl):
    self.fallback_coder_impl = fallback_coder_impl
    self.iterable_coder_impl = IterableCoderImpl(self)

  @staticmethod
  def register_iterable_like_type(t):
    _ITERABLE_LIKE_TYPES.add(t)

  def get_estimated_size_and_observables(self, value, nested=False):
    # type: (Any, bool) -> Tuple[int, Observables]
    if isinstance(value, observable.ObservableMixin):
      # FastPrimitivesCoderImpl can presumably encode the elements too.
      return 1, [(value, self)]

    out = ByteCountingOutputStream()
    self.encode_to_stream(value, out, nested)
    return out.get_count(), []

  def encode_to_stream(self, value, stream, nested):
    # type: (Any, create_OutputStream, bool) -> None
    t = type(value)
    if value is None:
      stream.write_byte(NONE_TYPE)
    elif t is int:
      # In Python 3, an int may be larger than 64 bits.
      # We need to check whether value fits into a 64 bit integer before
      # writing the marker byte.
      try:
        # In Cython-compiled code this will throw an overflow error
        # when value does not fit into int64.
        int_value = value
        # If Cython is not used, we must do a (slower) check ourselves.
        if not TYPE_CHECKING and not is_compiled:
          if not fits_in_64_bits(value):
            raise OverflowError()
        stream.write_byte(INT_TYPE)
        stream.write_var_int64(int_value)
      except OverflowError:
        stream.write_byte(UNKNOWN_TYPE)
        self.fallback_coder_impl.encode_to_stream(value, stream, nested)
    elif t is float:
      stream.write_byte(FLOAT_TYPE)
      stream.write_bigendian_double(value)
    elif t is bytes:
      stream.write_byte(BYTES_TYPE)
      stream.write(value, nested)
    elif t is past_unicode:
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
      for k, v in dict_value.items():
        self.encode_to_stream(k, stream, True)
        self.encode_to_stream(v, stream, True)
    elif t is bool:
      stream.write_byte(BOOL_TYPE)
      stream.write_byte(value)
    elif t in _ITERABLE_LIKE_TYPES:
      stream.write_byte(ITERABLE_LIKE_TYPE)
      self.iterable_coder_impl.encode_to_stream(value, stream, nested)
    else:
      stream.write_byte(UNKNOWN_TYPE)
      self.fallback_coder_impl.encode_to_stream(value, stream, nested)

  def decode_from_stream(self, stream, nested):
    # type: (create_InputStream, bool) -> Any
    t = stream.read_byte()
    if t == NONE_TYPE:
      return None
    elif t == INT_TYPE:
      return stream.read_var_int64()
    elif t == FLOAT_TYPE:
      return stream.read_bigendian_double()
    elif t == BYTES_TYPE:
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
    elif t == ITERABLE_LIKE_TYPE:
      return self.iterable_coder_impl.decode_from_stream(stream, nested)
    elif t == UNKNOWN_TYPE:
      return self.fallback_coder_impl.decode_from_stream(stream, nested)
    else:
      raise ValueError('Unknown type tag %x' % t)


class BytesCoderImpl(CoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  A coder for bytes/str objects."""
  def encode_to_stream(self, value, out, nested):
    # type: (bytes, create_OutputStream, bool) -> None
    out.write(value, nested)

  def decode_from_stream(self, in_stream, nested):
    # type: (create_InputStream, bool) -> bytes
    return in_stream.read_all(nested)

  def encode(self, value):
    assert isinstance(value, bytes), (value, type(value))
    return value

  def decode(self, encoded):
    return encoded


class BooleanCoderImpl(CoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  A coder for bool objects."""
  def encode_to_stream(self, value, out, nested):
    out.write_byte(1 if value else 0)

  def decode_from_stream(self, in_stream, nested):
    value = in_stream.read_byte()
    if value == 0:
      return False
    elif value == 1:
      return True
    raise ValueError("Expected 0 or 1, got %s" % value)

  def encode(self, value):
    return b'\x01' if value else b'\x00'

  def decode(self, encoded):
    value = ord(encoded)
    if value == 0:
      return False
    elif value == 1:
      return True
    raise ValueError("Expected 0 or 1, got %s" % value)

  def estimate_size(self, unused_value, nested=False):
    # Note that booleans are encoded the same way regardless of nesting.
    return 1


class MapCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  Note this implementation always uses nested context when encoding keys
  and values. This differs from Java's MapCoder, which uses
  nested=False if possible for the last value encoded.

  This difference is acceptable because MapCoder is not standard. It is only
  used in a standard context by RowCoder which always uses nested context for
  attribute values.

  A coder for typing.Mapping objects."""
  def __init__(
      self,
      key_coder,  # type: CoderImpl
      value_coder  # type: CoderImpl
  ):
    self._key_coder = key_coder
    self._value_coder = value_coder

  def encode_to_stream(self, dict_value, out, nested):
    out.write_bigendian_int32(len(dict_value))
    for key, value in dict_value.items():
      # Note this implementation always uses nested context when encoding keys
      # and values which differs from Java. See note in docstring.
      self._key_coder.encode_to_stream(key, out, True)
      self._value_coder.encode_to_stream(value, out, True)

  def decode_from_stream(self, in_stream, nested):
    size = in_stream.read_bigendian_int32()
    result = {}
    for _ in range(size):
      # Note this implementation always uses nested context when encoding keys
      # and values which differs from Java. See note in docstring.
      key = self._key_coder.decode_from_stream(in_stream, True)
      value = self._value_coder.decode_from_stream(in_stream, True)
      result[key] = value

    return result

  def estimate_size(self, unused_value, nested=False):
    estimate = 4  # 4 bytes for int32 size prefix
    for key, value in unused_value.items():
      estimate += self._key_coder.estimate_size(key, True)
      estimate += self._value_coder.estimate_size(value, True)
    return estimate


class NullableCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  A coder for typing.Optional objects."""

  ENCODE_NULL = 0
  ENCODE_PRESENT = 1

  def __init__(
      self,
      value_coder  # type: CoderImpl
  ):
    self._value_coder = value_coder

  def encode_to_stream(self, value, out, nested):
    if value is None:
      out.write_byte(self.ENCODE_NULL)
    else:
      out.write_byte(self.ENCODE_PRESENT)
      self._value_coder.encode_to_stream(value, out, nested)

  def decode_from_stream(self, in_stream, nested):
    null_indicator = in_stream.read_byte()
    if null_indicator == self.ENCODE_NULL:
      return None
    elif null_indicator == self.ENCODE_PRESENT:
      return self._value_coder.decode_from_stream(in_stream, nested)
    else:
      raise ValueError(
          "Encountered unexpected value for null indicator: '%s'" %
          null_indicator)

  def estimate_size(self, unused_value, nested=False):
    return 1 + (
        self._value_coder.estimate_size(unused_value)
        if unused_value is not None else 0)


class FloatCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees."""
  def encode_to_stream(self, value, out, nested):
    # type: (float, create_OutputStream, bool) -> None
    out.write_bigendian_double(value)

  def decode_from_stream(self, in_stream, nested):
    # type: (create_InputStream, bool) -> float
    return in_stream.read_bigendian_double()

  def estimate_size(self, unused_value, nested=False):
    # type: (Any, bool) -> int
    # A double is encoded as 8 bytes, regardless of nesting.
    return 8


if not TYPE_CHECKING:
  IntervalWindow = None


class IntervalWindowCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees."""

  # TODO: Fn Harness only supports millis. Is this important enough to fix?
  def _to_normal_time(self, value):
    """Convert "lexicographically ordered unsigned" to signed."""
    return value - _TIME_SHIFT

  def _from_normal_time(self, value):
    """Convert signed to "lexicographically ordered unsigned"."""
    return value + _TIME_SHIFT

  def encode_to_stream(self, value, out, nested):
    # type: (IntervalWindow, create_OutputStream, bool) -> None
    typed_value = value
    span_millis = (
        typed_value._end_micros // 1000 - typed_value._start_micros // 1000)
    out.write_bigendian_uint64(
        self._from_normal_time(typed_value._end_micros // 1000))
    out.write_var_int64(span_millis)

  def decode_from_stream(self, in_, nested):
    # type: (create_InputStream, bool) -> IntervalWindow
    if not TYPE_CHECKING:
      global IntervalWindow
      if IntervalWindow is None:
        from apache_beam.transforms.window import IntervalWindow
    # instantiating with None is not part of the public interface
    typed_value = IntervalWindow(None, None)  # type: ignore[arg-type]
    typed_value._end_micros = (
        1000 * self._to_normal_time(in_.read_bigendian_uint64()))
    typed_value._start_micros = (
        typed_value._end_micros - 1000 * in_.read_var_int64())
    return typed_value

  def estimate_size(self, value, nested=False):
    # type: (Any, bool) -> int
    # An IntervalWindow is context-insensitive, with a timestamp (8 bytes)
    # and a varint timespam.
    typed_value = value
    span_millis = (
        typed_value._end_micros // 1000 - typed_value._start_micros // 1000)
    return 8 + get_varint_size(span_millis)


class TimestampCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  TODO: SDK agnostic encoding
  For interoperability with Java SDK, encoding needs to match
  that of the Java SDK InstantCoder.
  https://github.com/apache/beam/blob/f5029b4f0dfff404310b2ef55e2632bbacc7b04f/sdks/java/core/src/main/java/org/apache/beam/sdk/coders/InstantCoder.java#L79
  """
  def encode_to_stream(self, value, out, nested):
    # type: (Timestamp, create_OutputStream, bool) -> None
    millis = value.micros // 1000
    if millis >= 0:
      millis = millis - _TIME_SHIFT
    else:
      millis = millis + _TIME_SHIFT
    out.write_bigendian_int64(millis)

  def decode_from_stream(self, in_stream, nested):
    # type: (create_InputStream, bool) -> Timestamp
    millis = in_stream.read_bigendian_int64()
    if millis < 0:
      millis = millis + _TIME_SHIFT
    else:
      millis = millis - _TIME_SHIFT
    return Timestamp(micros=millis * 1000)

  def estimate_size(self, unused_value, nested=False):
    # A Timestamp is encoded as a 64-bit integer in 8 bytes, regardless of
    # nesting.
    return 8


class TimerCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees."""
  def __init__(self, key_coder_impl, window_coder_impl):
    self._timestamp_coder_impl = TimestampCoderImpl()
    self._boolean_coder_impl = BooleanCoderImpl()
    self._pane_info_coder_impl = PaneInfoCoderImpl()
    self._key_coder_impl = key_coder_impl
    self._windows_coder_impl = TupleSequenceCoderImpl(window_coder_impl)
    from apache_beam.coders.coders import StrUtf8Coder
    self._tag_coder_impl = StrUtf8Coder().get_impl()

  def encode_to_stream(self, value, out, nested):
    # type: (dict, create_OutputStream, bool) -> None
    self._key_coder_impl.encode_to_stream(value.user_key, out, True)
    self._tag_coder_impl.encode_to_stream(value.dynamic_timer_tag, out, True)
    self._windows_coder_impl.encode_to_stream(value.windows, out, True)
    self._boolean_coder_impl.encode_to_stream(value.clear_bit, out, True)
    if not value.clear_bit:
      self._timestamp_coder_impl.encode_to_stream(
          value.fire_timestamp, out, True)
      self._timestamp_coder_impl.encode_to_stream(
          value.hold_timestamp, out, True)
      self._pane_info_coder_impl.encode_to_stream(value.paneinfo, out, True)

  def decode_from_stream(self, in_stream, nested):
    # type: (create_InputStream, bool) -> dict
    from apache_beam.transforms import userstate
    user_key = self._key_coder_impl.decode_from_stream(in_stream, True)
    dynamic_timer_tag = self._tag_coder_impl.decode_from_stream(in_stream, True)
    windows = self._windows_coder_impl.decode_from_stream(in_stream, True)
    clear_bit = self._boolean_coder_impl.decode_from_stream(in_stream, True)
    if clear_bit:
      return userstate.Timer(
          user_key=user_key,
          dynamic_timer_tag=dynamic_timer_tag,
          windows=windows,
          clear_bit=clear_bit,
          fire_timestamp=None,
          hold_timestamp=None,
          paneinfo=None)

    return userstate.Timer(
        user_key=user_key,
        dynamic_timer_tag=dynamic_timer_tag,
        windows=windows,
        clear_bit=clear_bit,
        fire_timestamp=self._timestamp_coder_impl.decode_from_stream(
            in_stream, True),
        hold_timestamp=self._timestamp_coder_impl.decode_from_stream(
            in_stream, True),
        paneinfo=self._pane_info_coder_impl.decode_from_stream(in_stream, True))


small_ints = [chr(_).encode('latin-1') for _ in range(128)]


class VarIntCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  A coder for long/int objects."""
  def encode_to_stream(self, value, out, nested):
    # type: (int, create_OutputStream, bool) -> None
    out.write_var_int64(value)

  def decode_from_stream(self, in_stream, nested):
    # type: (create_InputStream, bool) -> int
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
    # type: (Any, bool) -> int
    # Note that VarInts are encoded the same way regardless of nesting.
    return get_varint_size(value)


class SingletonCoderImpl(CoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  A coder that always encodes exactly one value."""
  def __init__(self, value):
    self._value = value

  def encode_to_stream(self, value, stream, nested):
    # type: (Any, create_OutputStream, bool) -> None
    pass

  def decode_from_stream(self, stream, nested):
    # type: (create_InputStream, bool) -> Any
    return self._value

  def encode(self, value):
    b = b''  # avoid byte vs str vs unicode error
    return b

  def decode(self, encoded):
    return self._value

  def estimate_size(self, value, nested=False):
    # type: (Any, bool) -> int
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
    # type: (Any, create_OutputStream, bool) -> None
    values = self._extract_components(value)
    if len(self._coder_impls) != len(values):
      raise ValueError('Number of components does not match number of coders.')
    for i in range(0, len(self._coder_impls)):
      c = self._coder_impls[i]  # type cast
      c.encode_to_stream(
          values[i], out, nested or i + 1 < len(self._coder_impls))

  def decode_from_stream(self, in_stream, nested):
    # type: (create_InputStream, bool) -> Any
    return self._construct_from_components([
        c.decode_from_stream(
            in_stream, nested or i + 1 < len(self._coder_impls)) for i,
        c in enumerate(self._coder_impls)
    ])

  def estimate_size(self, value, nested=False):
    # type: (Any, bool) -> int

    """Estimates the encoded size of the given value, in bytes."""
    # TODO(ccy): This ignores sizes of observable components.
    estimated_size, _ = (self.get_estimated_size_and_observables(value))
    return estimated_size

  def get_estimated_size_and_observables(self, value, nested=False):
    # type: (Any, bool) -> Tuple[int, Observables]

    """Returns estimated size of value along with any nested observables."""
    values = self._extract_components(value)
    estimated_size = 0
    observables = []  # type: Observables
    for i in range(0, len(self._coder_impls)):
      c = self._coder_impls[i]  # type cast
      child_size, child_observables = (
          c.get_estimated_size_and_observables(
              values[i], nested=nested or i + 1 < len(self._coder_impls)))
      estimated_size += child_size
      observables += child_observables
    return estimated_size, observables


class AvroCoderImpl(SimpleCoderImpl):
  """For internal use only; no backwards-compatibility guarantees."""
  def __init__(self, schema):
    self.parsed_schema = parse_schema(json.loads(schema))

  def encode(self, value):
    assert issubclass(type(value), AvroRecord)
    with BytesIO() as buf:
      schemaless_writer(buf, self.parsed_schema, value.record)
      return buf.getvalue()

  def decode(self, encoded):
    with BytesIO(encoded) as buf:
      return AvroRecord(schemaless_reader(buf, self.parsed_schema))


class TupleCoderImpl(AbstractComponentCoderImpl):
  """A coder for tuple objects."""
  def _extract_components(self, value):
    return value

  def _construct_from_components(self, components):
    return tuple(components)


class _ConcatSequence(object):
  def __init__(self, head, tail):
    # type: (Iterable[Any], Iterable[Any]) -> None
    self._head = head
    self._tail = tail

  def __iter__(self):
    # type: () -> Iterator[Any]
    for elem in self._head:
      yield elem
    for elem in self._tail:
      yield elem

  def __eq__(self, other):
    return list(self) == list(other)

  def __hash__(self):
    raise NotImplementedError

  def __reduce__(self):
    return list, (list(self), )


FastPrimitivesCoderImpl.register_iterable_like_type(_ConcatSequence)


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

  If writing to state is enabled, the final terminating 0 will instead be
  repaced with::

    varInt64(-1)
    len(state_token)
    state_token

  where state_token is a bytes object used to retrieve the remainder of the
  iterable via the state API.
  """

  # Default buffer size of 64kB of handling iterables of unknown length.
  _DEFAULT_BUFFER_SIZE = 64 * 1024

  def __init__(self,
               elem_coder,  # type: CoderImpl
               read_state=None,  # type: Optional[IterableStateReader]
               write_state=None,  # type: Optional[IterableStateWriter]
               write_state_threshold=0  # type: int
              ):
    self._elem_coder = elem_coder
    self._read_state = read_state
    self._write_state = write_state
    self._write_state_threshold = write_state_threshold

  def _construct_from_sequence(self, values):
    raise NotImplementedError

  def encode_to_stream(self, value, out, nested):
    # type: (Sequence, create_OutputStream, bool) -> None
    # Compatible with Java's IterableLikeCoder.
    if hasattr(value, '__len__') and self._write_state is None:
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
      if self._write_state is None:
        target_buffer_size = self._DEFAULT_BUFFER_SIZE
      else:
        target_buffer_size = min(
            self._DEFAULT_BUFFER_SIZE, self._write_state_threshold)
      prev_index = index = -1
      # Don't want to miss out on fast list iteration optimization.
      value_iter = value if isinstance(value, (list, tuple)) else iter(value)
      start_size = out.size()
      for elem in value_iter:
        index += 1
        self._elem_coder.encode_to_stream(elem, buffer, True)
        if buffer.size() > target_buffer_size:
          out.write_var_int64(index - prev_index)
          out.write(buffer.get())
          prev_index = index
          buffer = create_OutputStream()
          if (self._write_state is not None and
              out.size() - start_size > self._write_state_threshold):
            tail = (
                value_iter[index +
                           1:] if isinstance(value_iter,
                                             (list, tuple)) else value_iter)
            state_token = self._write_state(tail, self._elem_coder)
            out.write_var_int64(-1)
            out.write(state_token, True)
            break
      else:
        if index > prev_index:
          out.write_var_int64(index - prev_index)
          out.write(buffer.get())
        out.write_var_int64(0)

  def decode_from_stream(self, in_stream, nested):
    # type: (create_InputStream, bool) -> Sequence
    size = in_stream.read_bigendian_int32()

    if size >= 0:
      elements = [
          self._elem_coder.decode_from_stream(in_stream, True)
          for _ in range(size)
      ]  # type: Iterable[Any]
    else:
      elements = []
      count = in_stream.read_var_int64()
      while count > 0:
        for _ in range(count):
          elements.append(self._elem_coder.decode_from_stream(in_stream, True))
        count = in_stream.read_var_int64()

      if count == -1:
        if self._read_state is None:
          raise ValueError(
              'Cannot read state-written iterable without state reader.')

        state_token = in_stream.read_all(True)
        elements = _ConcatSequence(
            elements, self._read_state(state_token, self._elem_coder))

    return self._construct_from_sequence(elements)

  def estimate_size(self, value, nested=False):
    # type: (Any, bool) -> int

    """Estimates the encoded size of the given value, in bytes."""
    # TODO(ccy): This ignores element sizes.
    estimated_size, _ = (self.get_estimated_size_and_observables(value))
    return estimated_size

  def get_estimated_size_and_observables(self, value, nested=False):
    # type: (Any, bool) -> Tuple[int, Observables]

    """Returns estimated size of value along with any nested observables."""
    estimated_size = 0
    # Size of 32-bit integer storing number of elements.
    estimated_size += 4
    if isinstance(value, observable.ObservableMixin):
      return estimated_size, [(value, self._elem_coder)]

    observables = []  # type: Observables
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
    # TODO: More efficient size estimation in the case of state-backed
    # iterables.
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


class PaneInfoEncoding(object):
  """For internal use only; no backwards-compatibility guarantees.

  Encoding used to describe a PaneInfo descriptor.  A PaneInfo descriptor
  can be encoded in three different ways: with a single byte (FIRST), with a
  single byte followed by a varint describing a single index (ONE_INDEX) or
  with a single byte followed by two varints describing two separate indices:
  the index and nonspeculative index.
  """

  FIRST = 0
  ONE_INDEX = 1
  TWO_INDICES = 2


# These are cdef'd to ints to optimized the common case.
PaneInfoTiming_UNKNOWN = windowed_value.PaneInfoTiming.UNKNOWN
PaneInfoEncoding_FIRST = PaneInfoEncoding.FIRST


class PaneInfoCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  Coder for a PaneInfo descriptor."""
  def _choose_encoding(self, value):
    if ((value._index == 0 and value._nonspeculative_index == 0) or
        value._timing == PaneInfoTiming_UNKNOWN):
      return PaneInfoEncoding_FIRST
    elif (value._index == value._nonspeculative_index or
          value._timing == windowed_value.PaneInfoTiming.EARLY):
      return PaneInfoEncoding.ONE_INDEX
    else:
      return PaneInfoEncoding.TWO_INDICES

  def encode_to_stream(self, value, out, nested):
    # type: (windowed_value.PaneInfo, create_OutputStream, bool) -> None
    pane_info = value  # cast
    encoding_type = self._choose_encoding(pane_info)
    out.write_byte(pane_info._encoded_byte | (encoding_type << 4))
    if encoding_type == PaneInfoEncoding_FIRST:
      return
    elif encoding_type == PaneInfoEncoding.ONE_INDEX:
      out.write_var_int64(value.index)
    elif encoding_type == PaneInfoEncoding.TWO_INDICES:
      out.write_var_int64(value.index)
      out.write_var_int64(value.nonspeculative_index)
    else:
      raise NotImplementedError('Invalid PaneInfoEncoding: %s' % encoding_type)

  def decode_from_stream(self, in_stream, nested):
    # type: (create_InputStream, bool) -> windowed_value.PaneInfo
    encoded_first_byte = in_stream.read_byte()
    base = windowed_value._BYTE_TO_PANE_INFO[encoded_first_byte & 0xF]
    assert base is not None
    encoding_type = encoded_first_byte >> 4
    if encoding_type == PaneInfoEncoding_FIRST:
      return base
    elif encoding_type == PaneInfoEncoding.ONE_INDEX:
      index = in_stream.read_var_int64()
      if base.timing == windowed_value.PaneInfoTiming.EARLY:
        nonspeculative_index = -1
      else:
        nonspeculative_index = index
    elif encoding_type == PaneInfoEncoding.TWO_INDICES:
      index = in_stream.read_var_int64()
      nonspeculative_index = in_stream.read_var_int64()
    else:
      raise NotImplementedError('Invalid PaneInfoEncoding: %s' % encoding_type)
    return windowed_value.PaneInfo(
        base.is_first, base.is_last, base.timing, index, nonspeculative_index)

  def estimate_size(self, value, nested=False):
    # type: (Any, bool) -> int

    """Estimates the encoded size of the given value, in bytes."""
    size = 1
    encoding_type = self._choose_encoding(value)
    if encoding_type == PaneInfoEncoding.ONE_INDEX:
      size += get_varint_size(value.index)
    elif encoding_type == PaneInfoEncoding.TWO_INDICES:
      size += get_varint_size(value.index)
      size += get_varint_size(value.nonspeculative_index)
    return size


class WindowedValueCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  A coder for windowed values."""

  # Ensure that lexicographic ordering of the bytes corresponds to
  # chronological order of timestamps.
  # TODO(BEAM-1524): Clean this up once we have a BEAM wide consensus on
  # byte representation of timestamps.
  def _to_normal_time(self, value):
    """Convert "lexicographically ordered unsigned" to signed."""
    return value - _TIME_SHIFT

  def _from_normal_time(self, value):
    """Convert signed to "lexicographically ordered unsigned"."""
    return value + _TIME_SHIFT

  def __init__(self, value_coder, timestamp_coder, window_coder):
    # TODO(lcwik): Remove the timestamp coder field
    self._value_coder = value_coder
    self._timestamp_coder = timestamp_coder
    self._windows_coder = TupleSequenceCoderImpl(window_coder)
    self._pane_info_coder = PaneInfoCoderImpl()

  def encode_to_stream(self, value, out, nested):
    # type: (windowed_value.WindowedValue, create_OutputStream, bool) -> None
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
            restore_sign * (
                abs(
                    MIN_TIMESTAMP_micros if wv.timestamp_micros <
                    MIN_TIMESTAMP_micros else wv.timestamp_micros) // 1000)))
    self._windows_coder.encode_to_stream(wv.windows, out, True)
    # Default PaneInfo encoded byte representing NO_FIRING.
    self._pane_info_coder.encode_to_stream(wv.pane_info, out, True)
    self._value_coder.encode_to_stream(wv.value, out, nested)

  def decode_from_stream(self, in_stream, nested):
    # type: (create_InputStream, bool) -> windowed_value.WindowedValue
    timestamp = self._to_normal_time(in_stream.read_bigendian_uint64())
    # Restore MIN/MAX timestamps to their actual values as encoding incurs loss
    # of precision while converting to millis.
    # Note: This is only a best effort here as there is no way to know if these
    # were indeed MIN/MAX timestamps.
    # TODO(BEAM-1524): Clean this up once we have a BEAM wide consensus on
    # precision of timestamps.
    if timestamp <= -(abs(MIN_TIMESTAMP_micros) // 1000):
      timestamp = MIN_TIMESTAMP_micros
    elif timestamp >= MAX_TIMESTAMP_micros // 1000:
      timestamp = MAX_TIMESTAMP_micros
    else:
      timestamp *= 1000

    windows = self._windows_coder.decode_from_stream(in_stream, True)
    # Read PaneInfo encoded byte.
    pane_info = self._pane_info_coder.decode_from_stream(in_stream, True)
    value = self._value_coder.decode_from_stream(in_stream, nested)
    return windowed_value.create(
        value,
        # Avoid creation of Timestamp object.
        timestamp,
        windows,
        pane_info)

  def get_estimated_size_and_observables(self, value, nested=False):
    # type: (Any, bool) -> Tuple[int, Observables]

    """Returns estimated size of value along with any nested observables."""
    if isinstance(value, observable.ObservableMixin):
      # Should never be here.
      # TODO(robertwb): Remove when coders are set correctly.
      return 0, [(value, self._value_coder)]
    estimated_size = 0
    observables = []  # type: Observables
    value_estimated_size, value_observables = (
        self._value_coder.get_estimated_size_and_observables(
            value.value, nested=nested))
    estimated_size += value_estimated_size
    observables += value_observables
    estimated_size += (
        self._timestamp_coder.estimate_size(value.timestamp, nested=True))
    estimated_size += (
        self._windows_coder.estimate_size(value.windows, nested=True))
    estimated_size += (
        self._pane_info_coder.estimate_size(value.pane_info, nested=True))
    return estimated_size, observables


class ParamWindowedValueCoderImpl(WindowedValueCoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  A coder for windowed values with constant timestamp, windows and
  pane info. The coder drops timestamp, windows and pane info during
  encoding, and uses the supplied parameterized timestamp, windows
  and pane info values during decoding when reconstructing the windowed
  value."""
  def __init__(self, value_coder, window_coder, payload):
    super(ParamWindowedValueCoderImpl,
          self).__init__(value_coder, TimestampCoderImpl(), window_coder)
    self._timestamp, self._windows, self._pane_info = self._from_proto(
        payload, window_coder)

  def _from_proto(self, payload, window_coder):
    windowed_value_coder = WindowedValueCoderImpl(
        BytesCoderImpl(), TimestampCoderImpl(), window_coder)
    wv = windowed_value_coder.decode(payload)
    return wv.timestamp_micros, wv.windows, wv.pane_info

  def encode_to_stream(self, value, out, nested):
    wv = value  # type cast
    self._value_coder.encode_to_stream(wv.value, out, nested)

  def decode_from_stream(self, in_stream, nested):
    value = self._value_coder.decode_from_stream(in_stream, nested)
    return windowed_value.create(
        value, self._timestamp, self._windows, self._pane_info)

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
    return estimated_size, observables


class LengthPrefixCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees.

  Coder which prefixes the length of the encoded object in the stream."""
  def __init__(self, value_coder):
    # type: (CoderImpl) -> None
    self._value_coder = value_coder

  def encode_to_stream(self, value, out, nested):
    # type: (Any, create_OutputStream, bool) -> None
    encoded_value = self._value_coder.encode(value)
    out.write_var_int64(len(encoded_value))
    out.write(encoded_value)

  def decode_from_stream(self, in_stream, nested):
    # type: (create_InputStream, bool) -> Any
    value_length = in_stream.read_var_int64()
    return self._value_coder.decode(in_stream.read(value_length))

  def estimate_size(self, value, nested=False):
    # type: (Any, bool) -> int
    value_size = self._value_coder.estimate_size(value)
    return get_varint_size(value_size) + value_size
