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

import random
import unittest
from apache_beam import coders
from apache_beam.coders import slow_stream
from apache_beam.coders import coder_impl


class BufferedElementCountingOutputStreamTest(unittest.TestCase):

  _BUFFER_SIZE = 8

  def test_single_value(self):
    self._test_values(['abc'])

  def test_values_greater_than_size_of_buffer(self):
    self._test_values(['abcdefghijklmnopqrstuvwxyz'])

  def test_multiple_values_less_than_buffer(self):
    self._test_values(['abc', 'def', 'xyz'])

  def test_multiple_values_that_become_greater_than_buffer(self):
    self._test_values(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'l',
                       'm', 'n', 'o', 'p', 'r', 's', 't', 'u', 'v', 'x', 'y',
                       'z'])

  def test_multiple_random_sizes(self):
    for _ in range(1000):
      input = []
      for _ in range(100):
        random_size = random.randint(1, 20)
        random_data = (str(bytearray(random.getrandbits(8)
                                     for _ in xrange(random_size))))
        input.append(random_data)
      self._test_values(input)

  def test_adding_element_when_finished_raises(self):
    with self.assertRaisesRegexp(ValueError, "Stream has been finished"):
      self._test_values(['a']).mark_element_start()

  def test_writing_byte_when_finished_raises(self):
    with self.assertRaisesRegexp(ValueError, "Stream has been finished"):
      self._test_values(['a']).write_byte(1)

  def test_writing_bytes_when_finished_raises(self):
    with self.assertRaisesRegexp(ValueError, "Stream has been finished"):
      self._test_values(['a']).write('abcde')

  def _test_values(self, expected_values):
    op_stream = coder_impl.create_OutputStream()
    buffered_stream = self._create_and_write_values(expected_values, op_stream,
                                                    coders.BytesCoder())
    buffered_stream.finish()
    in_stream = coder_impl.create_InputStream(buffered_stream.get())
    self._verify_values(expected_values, in_stream, coders.BytesCoder())
    return buffered_stream

  def _create_and_write_values(self, values, op_stream, value_coder):
    buffered_stream = slow_stream.BufferedElementCountingOutputStream(
        op_stream, self._BUFFER_SIZE)
    for value in values:
      buffered_stream.mark_element_start()
      value_coder.get_impl().encode_to_stream(value, buffered_stream,
                                              nested=True)
    return buffered_stream

  def _verify_values(self, expected_values, in_stream, value_coder):
    actual_values = []
    count = in_stream.read_var_int64()
    while count > 0:
      actual_values.append(
          value_coder.get_impl().decode_from_stream(in_stream, True))
      count -= 1
      if not count:
        count = in_stream.read_var_int64()
    self.assertItemsEqual(expected_values, actual_values)
