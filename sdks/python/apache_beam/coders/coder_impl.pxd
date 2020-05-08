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

# cython: profile=True

cimport cython

cimport cpython.ref
cimport cpython.tuple
cimport libc.stdint
cimport libc.stdlib
cimport libc.string

cdef extern from "math.h":
  libc.stdint.int64_t abs "llabs"(libc.stdint.int64_t)

from .stream cimport InputStream, OutputStream
from apache_beam.utils cimport windowed_value


cdef object loads, dumps, create_InputStream, create_OutputStream, ByteCountingOutputStream, get_varint_size, past_unicode
# Temporarily untyped to allow monkeypatching on failed import.
#cdef type WindowedValue


cdef class CoderImpl(object):
  cpdef encode_to_stream(self, value, OutputStream stream, bint nested)
  cpdef decode_from_stream(self, InputStream stream, bint nested)
  cpdef bytes encode(self, value)
  cpdef decode(self, bytes encoded)
  cpdef bytes encode_nested(self, value)
  cpdef decode_nested(self, bytes encoded)
  cpdef estimate_size(self, value, bint nested=?)
  @cython.locals(varint_size=int, bits=libc.stdint.uint64_t)
  @cython.overflowcheck(False)
  cpdef int _get_nested_size(self, int inner_size, bint nested)
  cpdef get_estimated_size_and_observables(self, value, bint nested=?)


cdef class SimpleCoderImpl(CoderImpl):
  pass


cdef class StreamCoderImpl(CoderImpl):
  pass


cdef class CallbackCoderImpl(CoderImpl):
  cdef object _encoder
  cdef object _decoder
  cdef object _size_estimator


cdef class DeterministicFastPrimitivesCoderImpl(CoderImpl):
  cdef CoderImpl _underlying_coder
  cdef object _step_label
  cdef bint _check_safe(self, value) except -1


cdef object NoneType
cdef unsigned char UNKNOWN_TYPE, NONE_TYPE, INT_TYPE, FLOAT_TYPE, BOOL_TYPE
cdef unsigned char BYTES_TYPE, UNICODE_TYPE, LIST_TYPE, TUPLE_TYPE, DICT_TYPE
cdef unsigned char SET_TYPE, ITERABLE_LIKE_TYPE

cdef set _ITERABLE_LIKE_TYPES

cdef class FastPrimitivesCoderImpl(StreamCoderImpl):
  cdef CoderImpl fallback_coder_impl
  cdef CoderImpl iterable_coder_impl
  @cython.locals(dict_value=dict, int_value=libc.stdint.int64_t,
                 unicode_value=unicode)
  cpdef encode_to_stream(self, value, OutputStream stream, bint nested)
  @cython.locals(t=int)
  cpdef decode_from_stream(self, InputStream stream, bint nested)


cdef class BytesCoderImpl(CoderImpl):
  pass


cdef class BooleanCoderImpl(CoderImpl):
  pass


cdef class FloatCoderImpl(StreamCoderImpl):
  pass


cdef class TimestampCoderImpl(StreamCoderImpl):
  cdef object timestamp_class


cdef list small_ints
cdef class VarIntCoderImpl(StreamCoderImpl):
  @cython.locals(ivalue=libc.stdint.int64_t)
  cpdef bytes encode(self, value)


cdef class SingletonCoderImpl(CoderImpl):
  cdef object _value


cdef class AbstractComponentCoderImpl(StreamCoderImpl):
  cdef tuple _coder_impls

  cpdef _extract_components(self, value)
  cpdef _construct_from_components(self, components)

  @cython.locals(c=CoderImpl)
  cpdef encode_to_stream(self, value, OutputStream stream, bint nested)
  @cython.locals(c=CoderImpl)
  cpdef decode_from_stream(self, InputStream stream, bint nested)

  @cython.locals(c=CoderImpl)
  cpdef get_estimated_size_and_observables(self, value, bint nested=?)


cdef class TupleCoderImpl(AbstractComponentCoderImpl):
  pass


cdef class SequenceCoderImpl(StreamCoderImpl):
  cdef CoderImpl _elem_coder
  cdef object _read_state
  cdef object _write_state
  cdef int _write_state_threshold

  cpdef _construct_from_sequence(self, values)

  @cython.locals(buffer=OutputStream, target_buffer_size=libc.stdint.int64_t,
                 index=libc.stdint.int64_t, prev_index=libc.stdint.int64_t)
  cpdef encode_to_stream(self, value, OutputStream stream, bint nested)


cdef class TupleSequenceCoderImpl(SequenceCoderImpl):
  pass


cdef class IterableCoderImpl(SequenceCoderImpl):
  pass


cdef object IntervalWindow

cdef class IntervalWindowCoderImpl(StreamCoderImpl):
  cdef libc.stdint.uint64_t _to_normal_time(self, libc.stdint.int64_t value)
  cdef libc.stdint.int64_t _from_normal_time(self, libc.stdint.uint64_t value)

  @cython.locals(typed_value=windowed_value._IntervalWindowBase,
                 span_millis=libc.stdint.int64_t)
  cpdef encode_to_stream(self, value, OutputStream stream, bint nested)

  @cython.locals(typed_value=windowed_value._IntervalWindowBase)
  cpdef decode_from_stream(self, InputStream stream, bint nested)

  @cython.locals(typed_value=windowed_value._IntervalWindowBase,
                 span_millis=libc.stdint.int64_t)
  cpdef estimate_size(self, value, bint nested=?)


cdef int PaneInfoTiming_UNKNOWN
cdef int PaneInfoEncoding_FIRST


cdef class PaneInfoCoderImpl(StreamCoderImpl):
  cdef int _choose_encoding(self, windowed_value.PaneInfo value)

  @cython.locals(pane_info=windowed_value.PaneInfo, encoding_type=int)
  cpdef encode_to_stream(self, value, OutputStream stream, bint nested)

  @cython.locals(encoded_first_byte=int, encoding_type=int)
  cpdef decode_from_stream(self, InputStream stream, bint nested)


cdef libc.stdint.uint64_t _TIME_SHIFT
cdef libc.stdint.int64_t MIN_TIMESTAMP_micros
cdef libc.stdint.int64_t MAX_TIMESTAMP_micros


cdef class WindowedValueCoderImpl(StreamCoderImpl):
  """A coder for windowed values."""
  cdef CoderImpl _value_coder
  cdef CoderImpl _timestamp_coder
  cdef CoderImpl _windows_coder
  cdef CoderImpl _pane_info_coder

  cdef libc.stdint.uint64_t _to_normal_time(self, libc.stdint.int64_t value)
  cdef libc.stdint.int64_t _from_normal_time(self, libc.stdint.uint64_t value)

  @cython.locals(c=CoderImpl)
  cpdef get_estimated_size_and_observables(self, value, bint nested=?)

  @cython.locals(timestamp=libc.stdint.int64_t)
  cpdef decode_from_stream(self, InputStream stream, bint nested)

  @cython.locals(wv=windowed_value.WindowedValue, restore_sign=int)
  cpdef encode_to_stream(self, value, OutputStream stream, bint nested)


cdef class ParamWindowedValueCoderImpl(WindowedValueCoderImpl):
  """A coder for windowed values with constant timestamp, windows and pane info."""
  cdef readonly libc.stdint.int64_t _timestamp
  cdef readonly object _windows
  cdef readonly windowed_value.PaneInfo _pane_info


cdef class LengthPrefixCoderImpl(StreamCoderImpl):
  cdef CoderImpl _value_coder
