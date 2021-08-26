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

"""Tests for the stream implementations."""
# pytype: skip-file

import logging
import math
import unittest

from apache_beam.coders import slow_stream


class StreamTest(unittest.TestCase):
  # pylint: disable=invalid-name
  InputStream = slow_stream.InputStream
  OutputStream = slow_stream.OutputStream
  ByteCountingOutputStream = slow_stream.ByteCountingOutputStream

  # pylint: enable=invalid-name

  def test_read_write(self):
    out_s = self.OutputStream()
    out_s.write(b'abc')
    out_s.write(b'\0\t\n')
    out_s.write(b'xyz', True)
    out_s.write(b'', True)
    in_s = self.InputStream(out_s.get())
    self.assertEqual(b'abc\0\t\n', in_s.read(6))
    self.assertEqual(b'xyz', in_s.read_all(True))
    self.assertEqual(b'', in_s.read_all(True))

  def test_read_all(self):
    out_s = self.OutputStream()
    out_s.write(b'abc')
    in_s = self.InputStream(out_s.get())
    self.assertEqual(b'abc', in_s.read_all(False))

  def test_read_write_byte(self):
    out_s = self.OutputStream()
    out_s.write_byte(1)
    out_s.write_byte(0)
    out_s.write_byte(0xFF)
    in_s = self.InputStream(out_s.get())
    self.assertEqual(1, in_s.read_byte())
    self.assertEqual(0, in_s.read_byte())
    self.assertEqual(0xFF, in_s.read_byte())

  def test_read_write_large(self):
    values = range(4 * 1024)
    out_s = self.OutputStream()
    for v in values:
      out_s.write_bigendian_int64(v)
    in_s = self.InputStream(out_s.get())
    for v in values:
      self.assertEqual(v, in_s.read_bigendian_int64())

  def run_read_write_var_int64(self, values):
    out_s = self.OutputStream()
    for v in values:
      out_s.write_var_int64(v)
    in_s = self.InputStream(out_s.get())
    for v in values:
      self.assertEqual(v, in_s.read_var_int64())

  def test_small_var_int64(self):
    self.run_read_write_var_int64(range(-10, 30))

  def test_medium_var_int64(self):
    base = -1.7
    self.run_read_write_var_int64([
        int(base**pow)
        for pow in range(1, int(63 * math.log(2) / math.log(-base)))
    ])

  def test_large_var_int64(self):
    self.run_read_write_var_int64([0, 2**63 - 1, -2**63, 2**63 - 3])

  def test_read_write_double(self):
    values = 0, 1, -1, 1e100, 1.0 / 3, math.pi, float('inf')
    out_s = self.OutputStream()
    for v in values:
      out_s.write_bigendian_double(v)
    in_s = self.InputStream(out_s.get())
    for v in values:
      self.assertEqual(v, in_s.read_bigendian_double())

  def test_read_write_bigendian_int64(self):
    values = 0, 1, -1, 2**63 - 1, -2**63, int(2**61 * math.pi)
    out_s = self.OutputStream()
    for v in values:
      out_s.write_bigendian_int64(v)
    in_s = self.InputStream(out_s.get())
    for v in values:
      self.assertEqual(v, in_s.read_bigendian_int64())

  def test_read_write_bigendian_uint64(self):
    values = 0, 1, 2**64 - 1, int(2**61 * math.pi)
    out_s = self.OutputStream()
    for v in values:
      out_s.write_bigendian_uint64(v)
    in_s = self.InputStream(out_s.get())
    for v in values:
      self.assertEqual(v, in_s.read_bigendian_uint64())

  def test_read_write_bigendian_int32(self):
    values = 0, 1, -1, 2**31 - 1, -2**31, int(2**29 * math.pi)
    out_s = self.OutputStream()
    for v in values:
      out_s.write_bigendian_int32(v)
    in_s = self.InputStream(out_s.get())
    for v in values:
      self.assertEqual(v, in_s.read_bigendian_int32())

  def test_byte_counting(self):
    bc_s = self.ByteCountingOutputStream()
    self.assertEqual(0, bc_s.get_count())
    bc_s.write(b'def')
    self.assertEqual(3, bc_s.get_count())
    bc_s.write(b'')
    self.assertEqual(3, bc_s.get_count())
    bc_s.write_byte(10)
    self.assertEqual(4, bc_s.get_count())
    # "nested" also writes the length of the string, which should
    # cause 1 extra byte to be counted.
    bc_s.write(b'2345', nested=True)
    self.assertEqual(9, bc_s.get_count())
    bc_s.write_var_int64(63)
    self.assertEqual(10, bc_s.get_count())
    bc_s.write_bigendian_int64(42)
    self.assertEqual(18, bc_s.get_count())
    bc_s.write_bigendian_int32(36)
    self.assertEqual(22, bc_s.get_count())
    bc_s.write_bigendian_double(6.25)
    self.assertEqual(30, bc_s.get_count())
    bc_s.write_bigendian_uint64(47)
    self.assertEqual(38, bc_s.get_count())


try:
  # pylint: disable=wrong-import-position
  from apache_beam.coders import stream

  class FastStreamTest(StreamTest):
    """Runs the test with the compiled stream classes."""
    InputStream = stream.InputStream
    OutputStream = stream.OutputStream
    ByteCountingOutputStream = stream.ByteCountingOutputStream

  class SlowFastStreamTest(StreamTest):
    """Runs the test with compiled and uncompiled stream classes."""
    InputStream = stream.InputStream
    OutputStream = slow_stream.OutputStream
    ByteCountingOutputStream = slow_stream.ByteCountingOutputStream

  class FastSlowStreamTest(StreamTest):
    """Runs the test with uncompiled and compiled stream classes."""
    InputStream = slow_stream.InputStream
    OutputStream = stream.OutputStream
    ByteCountingOutputStream = stream.ByteCountingOutputStream

except ImportError:
  pass

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
