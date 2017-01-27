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

from .stream cimport InputStream, OutputStream


cdef object loads, dumps, create_InputStream, create_OutputStream
cdef type WindowedValue


cdef class CoderImpl(object):
  cpdef encode_to_stream(self, value, OutputStream stream, bint nested)
  cpdef decode_from_stream(self, InputStream stream, bint nested)
  cpdef bytes encode(self, value)
  cpdef decode(self, bytes encoded)


cdef class SimpleCoderImpl(CoderImpl):
  pass


cdef class StreamCoderImpl(CoderImpl):
  pass


cdef class CallbackCoderImpl(CoderImpl):
  cdef object _encoder
  cdef object _decoder


cdef class DeterministicPickleCoderImpl(CoderImpl):
  cdef CoderImpl _pickle_coder
  cdef object _step_label
  cdef bint _check_safe(self, value) except -1


cdef class BytesCoderImpl(CoderImpl):
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


cdef class TupleCoderImpl(AbstractComponentCoderImpl):
  pass


cdef class SequenceCoderImpl(StreamCoderImpl):
  cdef CoderImpl _elem_coder
  cpdef _construct_from_sequence(self, values)


cdef class TupleSequenceCoderImpl(SequenceCoderImpl):
  pass


cdef class WindowedValueCoderImpl(StreamCoderImpl):
  """A coder for windowed values."""
  cdef CoderImpl _value_coder
  cdef CoderImpl _timestamp_coder
  cdef CoderImpl _windows_coder
