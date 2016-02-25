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

"""A pure Python implementation of stream.pyx."""


class OutputStream(object):
  """A pure Python implementation of stream.OutputStream."""

  def __init__(self):
    self.data = []

  def write(self, b, nested=False):
    assert isinstance(b, str)
    if nested:
      self.write_var_int64(len(b))
    self.data.append(b)

  def write_byte(self, val):
    self.data.append(chr(val))

  def write_var_int64(self, v):
    if v < 0:
      v += 1 << 64
      if v <= 0:
        raise ValueError('Value too large (negative).')
    while True:
      bits = v & 0x7F
      v >>= 7
      if v:
        bits |= 0x80
      self.write_byte(bits)
      if not v:
        break

  def get(self):
    return ''.join(self.data)


class InputStream(object):
  """A pure Python implementation of stream.InputStream."""

  def __init__(self, data):
    self.data = data
    self.pos = 0

  def size(self):
    return len(self.data) - self.pos

  def read(self, size):
    self.pos += size
    return self.data[self.pos - size : self.pos]

  def read_all(self, nested):
    return self.read(self.read_var_int64() if nested else self.size())

  def read_byte(self):
    self.pos += 1
    return ord(self.data[self.pos - 1])

  def read_var_int64(self):
    shift = 0
    result = 0
    while True:
      byte = self.read_byte()
      if byte < 0:
        raise RuntimeError('VarLong not terminated.')

      bits = byte & 0x7F
      if shift >= 64 or (shift >= 63 and bits > 1):
        raise RuntimeError('VarLong too long.')
      result |= bits << shift
      shift += 7
      if not byte & 0x80:
        break
    if result >= 1 << 63:
      result -= 1 << 64
    return result
