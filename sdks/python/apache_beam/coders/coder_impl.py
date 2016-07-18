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
"""

import collections

from apache_beam.coders import observable


# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  # Don't depend on the full dataflow sdk to test coders.
  from apache_beam.transforms.window import WindowedValue
except ImportError:
  WindowedValue = collections.namedtuple(
      'WindowedValue', ('value', 'timestamp', 'windows'))

try:
  from stream import InputStream as create_InputStream
  from stream import OutputStream as create_OutputStream
except ImportError:
  from slow_stream import InputStream as create_InputStream
  from slow_stream import OutputStream as create_OutputStream
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports


class CoderImpl(object):

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
    """Encodes an object to an unnested string."""
    raise NotImplementedError

  def estimate_size(self, value):
    """Estimates the encoded size of the given value, in bytes."""
    return len(self.encode(value))

  def get_estimated_size_and_observables(self, value):
    """Returns estimated size of value along with any nested observables.

    The list of nested observables is returned as a list of 2-tuples of
    (obj, coder_impl), where obj is an instance of observable.ObservableMixin,
    and coder_impl is the CoderImpl that can be used to encode elements sent by
    obj to its observers.

    Arguments:
      value: the value whose encoded size is to be estimated.

    Returns:
      The estimated encoded size of the given value and a list of observables
      whose elements are 2-tuples of (obj, coder_impl) as described above.
    """
    return self.estimate_size(value), []


class SimpleCoderImpl(CoderImpl):
  """Subclass of CoderImpl implementing stream methods using encode/decode."""

  def encode_to_stream(self, value, stream, nested):
    """Reads object from potentially-nested encoding in stream."""
    stream.write(self.encode(value), nested)

  def decode_from_stream(self, stream, nested):
    """Reads object from potentially-nested encoding in stream."""
    return self.decode(stream.read_all(nested))


class StreamCoderImpl(CoderImpl):
  """Subclass of CoderImpl implementing encode/decode using stream methods."""

  def encode(self, value):
    out = create_OutputStream()
    self.encode_to_stream(value, out, False)
    return out.get()

  def decode(self, encoded):
    return self.decode_from_stream(create_InputStream(encoded), False)


class CallbackCoderImpl(CoderImpl):
  """A CoderImpl that calls back to the _impl methods on the Coder itself.

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

  def estimate_size(self, value):
    return self._size_estimator(value)


class DeterministicPickleCoderImpl(CoderImpl):

  def __init__(self, pickle_coder, step_label):
    self._pickle_coder = pickle_coder
    self._step_label = step_label

  def _check_safe(self, value):
    if isinstance(value, (str, unicode, long, int, float)):
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
    return self._pickle_coder.encode_to_stream(value, stream, nested)

  def decode_from_stream(self, stream, nested):
    return self._pickle_coder.decode_from_stream(stream, nested)

  def encode(self, value):
    self._check_safe(value)
    return self._pickle_coder.encode(value)

  def decode(self, encoded):
    return self._pickle_coder.decode(encoded)


class BytesCoderImpl(CoderImpl):
  """A coder for bytes/str objects."""

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

  def encode_to_stream(self, value, out, nested):
    out.write_bigendian_double(value)

  def decode_from_stream(self, in_stream, nested):
    return in_stream.read_bigendian_double()

  def estimate_size(self, unused_value):
    # A double is encoded as 8 bytes.
    return 8


class TimestampCoderImpl(StreamCoderImpl):

  def __init__(self, timestamp_class):
    self.timestamp_class = timestamp_class

  def encode_to_stream(self, value, out, nested):
    out.write_bigendian_int64(value.micros)

  def decode_from_stream(self, in_stream, nested):
    return self.timestamp_class(micros=in_stream.read_bigendian_int64())

  def estimate_size(self, unused_value):
    # A Timestamp is encoded as a 64-bit integer in 8 bytes.
    return 8


small_ints = [chr(_) for _ in range(128)]


class VarIntCoderImpl(StreamCoderImpl):
  """A coder for long/int objects."""

  def encode_to_stream(self, value, out, nested):
    out.write_var_int64(value)

  def decode_from_stream(self, in_stream, nested):
    return in_stream.read_var_int64()

  def encode(self, value):
    ivalue = value  # type cast
    if 0 <= ivalue < len(small_ints):
      return small_ints[ivalue]
    else:
      return StreamCoderImpl.encode(self, value)

  def decode(self, encoded):
    if len(encoded) == 1:
      i = ord(encoded)
      if 0 <= i < 128:
        return i
    return StreamCoderImpl.decode(self, encoded)


class SingletonCoderImpl(CoderImpl):
  """A coder that always encodes exactly one value."""

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


# Number of bytes of overhead estimated for encoding the nested size of a
# component as a VarInt64.
ESTIMATED_NESTED_OVERHEAD = 2


class AbstractComponentCoderImpl(StreamCoderImpl):
  """CoderImpl for coders that are comprised of several component coders."""

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
      c.encode_to_stream(values[i], out, True)

  def decode_from_stream(self, in_stream, nested):
    return self._construct_from_components(
        [c.decode_from_stream(in_stream, True) for c in self._coder_impls])

  def estimate_size(self, value):
    """Estimates the encoded size of the given value, in bytes."""
    estimated_size, _ = (
        self.get_estimated_size_and_observables(value))
    return estimated_size

  def get_estimated_size_and_observables(self, value):
    """Returns estimated size of value along with any nested observables."""
    values = self._extract_components(value)
    estimated_size = 0
    observables = []
    for i in range(0, len(self._coder_impls)):
      child_value = values[i]
      if isinstance(child_value, observable.ObservableMixin):
        observables.append((child_value, self._coder_impls[i]))
      else:
        c = self._coder_impls[i]  # type cast
        child_size, child_observables = (
            c.get_estimated_size_and_observables(child_value))
        estimated_size += child_size + ESTIMATED_NESTED_OVERHEAD
        observables += child_observables
    return estimated_size, observables


class TupleCoderImpl(AbstractComponentCoderImpl):
  """A coder for tuple objects."""

  def _extract_components(self, value):
    return value

  def _construct_from_components(self, components):
    return tuple(components)


class SequenceCoderImpl(StreamCoderImpl):
  """A coder for sequences of known length."""

  def __init__(self, elem_coder):
    self._elem_coder = elem_coder

  def _construct_from_sequence(self, values):
    raise NotImplementedError

  def encode_to_stream(self, value, out, nested):
    # Compatible with Java's IterableLikeCoder.
    out.write_bigendian_int32(len(value))
    for elem in value:
      self._elem_coder.encode_to_stream(elem, out, True)

  def decode_from_stream(self, in_stream, nested):
    size = in_stream.read_bigendian_int32()
    return self._construct_from_sequence(
        [self._elem_coder.decode_from_stream(in_stream, True)
         for _ in range(size)])


class TupleSequenceCoderImpl(SequenceCoderImpl):
  """A coder for homogeneous tuple objects."""

  def _construct_from_sequence(self, components):
    return tuple(components)


class WindowedValueCoderImpl(StreamCoderImpl):
  """A coder for windowed values."""

  def __init__(self, value_coder, timestamp_coder, window_coder):
    self._value_coder = value_coder
    self._timestamp_coder = timestamp_coder
    self._windows_coder = TupleSequenceCoderImpl(window_coder)

  def encode_to_stream(self, value, out, nested):
    self._value_coder.encode_to_stream(value.value, out, True)
    self._timestamp_coder.encode_to_stream(value.timestamp, out, True)
    self._windows_coder.encode_to_stream(value.windows, out, True)

  def decode_from_stream(self, in_stream, nested):
    return WindowedValue(
        self._value_coder.decode_from_stream(in_stream, True),
        self._timestamp_coder.decode_from_stream(in_stream, True),
        self._windows_coder.decode_from_stream(in_stream, True))

  def get_estimated_size_and_observables(self, value):
    """Returns estimated size of value along with any nested observables."""
    estimated_size = 0
    observables = []
    if isinstance(value.value, observable.ObservableMixin):
      observables.append((value.value, self._value_coder))
    else:
      c = self._value_coder  # type cast
      value_estimated_size, value_observables = (
          c.get_estimated_size_and_observables(value.value))
      estimated_size += value_estimated_size
      observables += value_observables
    estimated_size += self._timestamp_coder.estimate_size(value.timestamp)
    estimated_size += self._windows_coder.estimate_size(value.windows)
    return estimated_size, observables
