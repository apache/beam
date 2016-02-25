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

"""Tests for the stream implementations."""

import math
import unittest


from google.cloud.dataflow.coders import slow_stream


class StreamTest(unittest.TestCase):
  InputStream = slow_stream.InputStream
  OutputStream = slow_stream.OutputStream

  def test_read_write(self):
    out_s = self.OutputStream()
    out_s.write('abc')
    out_s.write('\0\t\n')
    out_s.write('xyz', True)
    out_s.write('', True)
    in_s = self.InputStream(out_s.get())
    self.assertEquals('abc\0\t\n', in_s.read(6))
    self.assertEquals('xyz', in_s.read_all(True))
    self.assertEquals('', in_s.read_all(True))

  def test_read_all(self):
    out_s = self.OutputStream()
    out_s.write('abc')
    in_s = self.InputStream(out_s.get())
    self.assertEquals('abc', in_s.read_all(False))

  def test_read_write_byte(self):
    out_s = self.OutputStream()
    out_s.write_byte(1)
    out_s.write_byte(0)
    out_s.write_byte(0xFF)
    in_s = self.InputStream(out_s.get())
    self.assertEquals(1, in_s.read_byte())
    self.assertEquals(0, in_s.read_byte())
    self.assertEquals(0xFF, in_s.read_byte())

  def run_read_write_var_int64(self, values):
    out_s = self.OutputStream()
    for v in values:
      out_s.write_var_int64(v)
    in_s = self.InputStream(out_s.get())
    for v in values:
      self.assertEquals(v, in_s.read_var_int64())

  def test_small_var_int64(self):
    self.run_read_write_var_int64(range(-10, 30))

  def test_medium_var_int64(self):
    base = -1.7
    self.run_read_write_var_int64(
        [int(base**pow)
          for pow in range(1, int(63 * math.log(2) / math.log(-base)))])

  def test_large_var_int64(self):
    self.run_read_write_var_int64([0, 2**63 - 1, -2**63, 2**63 - 3])


try:
  # pylint: disable=g-import-not-at-top
  from google.cloud.dataflow.coders import stream

  class FastStreamTest(StreamTest):
    """Runs the test with the compiled stream classes."""
    InputStream = stream.InputStream
    OutputStream = stream.OutputStream


  class SlowFastStreamTest(StreamTest):
    """Runs the test with compiled and uncompiled stream classes."""
    InputStream = stream.InputStream
    OutputStream = slow_stream.OutputStream


  class FastSlowStreamTest(StreamTest):
    """Runs the test with uncompiled and compiled stream classes."""
    InputStream = slow_stream.InputStream
    OutputStream = stream.OutputStream

except ImportError:
  pass


if __name__ == '__main__':
  unittest.main()
