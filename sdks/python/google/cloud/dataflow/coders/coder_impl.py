# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Coder implementations.

The actual encode/decode implementations are split off from coders to
allow conditional (compiled/pure) implementations, which can be used to
encode many elements with minimial overhead.

This module may be optionally compiled with Cython, using the corresponding
coder_impl.pxd file for type hints.
"""

import collections
from cPickle import loads, dumps


# pylint: disable=g-import-not-at-top
try:
  # Don't depend on the full dataflow sdk to test coders.
  from google.cloud.dataflow.transforms.window import WindowedValue
except ImportError:
  WindowedValue = collections.namedtuple(
      'WindowedValue', ('value', 'timestamp', 'windows'))

try:
  from stream import InputStream as create_InputStream
  from stream import OutputStream as create_OutputStream
except ImportError:
  from slow_stream import InputStream as create_InputStream
  from slow_stream import OutputStream as create_OutputStream
# pylint: enable=g-import-not-at-top


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

  def __init__(self, encoder, decoder):
    self._encoder = encoder
    self._decoder = decoder

  def encode_to_stream(self, value, stream, nested):
    return stream.write(self._encoder(value), nested)

  def decode_from_stream(self, stream, nested):
    return self._decoder(stream.read_all(nested))

  def encode(self, value):
    return self._encoder(value)

  def decode(self, encoded):
    return self._decoder(encoded)


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


class TimestampCoderImpl(StreamCoderImpl):

  def __init__(self, timestamp_class):
    self.timestamp_class = timestamp_class

  def encode_to_stream(self, value, out, nested):
    out.write_bigendian_int64(value.micros)

  def decode_from_stream(self, in_stream, nested):
    return self.timestamp_class(micros=in_stream.read_bigendian_int64())


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


class AbstractComponentCoderImpl(StreamCoderImpl):

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


class TupleCoderImpl(AbstractComponentCoderImpl):
  """A coder for tuple objects."""

  def _extract_components(self, value):
    return value

  def _construct_from_components(self, components):
    return tuple(components)


class WindowedValueCoderImpl(AbstractComponentCoderImpl):
  """A coder for windowed values."""

  def __init__(self, wrapped_value_coder, timestamp_coder, window_coder):
    super(WindowedValueCoderImpl, self).__init__(
        (wrapped_value_coder, timestamp_coder, window_coder))
    self.wrapped_value_coder = wrapped_value_coder
    self.timestamp_coder = timestamp_coder
    self.window_coder = window_coder

  def _extract_components(self, value):
    return [value.value, value.timestamp, value.windows]

  def _construct_from_components(self, components):
    return WindowedValue(components[0],  # value
                         components[1],  # timestamp
                         components[2])  # windows
