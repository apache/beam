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

"""Unit tests for the typecoders module."""

import unittest

from apache_beam.coders import coders
from apache_beam.coders import typecoders
from apache_beam.internal import pickler
from apache_beam.typehints import typehints


class CustomClass(object):

  def __init__(self, n):
    self.number = n

  def __eq__(self, other):
    return self.number == other.number


class CustomCoder(coders.Coder):

  def encode(self, value):
    return str(value.number)

  def decode(self, encoded):
    return CustomClass(int(encoded))

  def is_deterministic(self):
    # This coder is deterministic. Though we don't use need this coder to be
    # deterministic for this test, we annotate this as such to follow best
    # practices.
    return True


class TypeCodersTest(unittest.TestCase):

  def test_register_non_type_coder(self):
    coder = CustomCoder()
    with self.assertRaises(TypeError) as e:
      # When registering a coder the coder class must be specified.
      typecoders.registry.register_coder(CustomClass, coder)
    self.assertEqual(e.exception.message,
                     'Coder registration requires a coder class object. '
                     'Received %r instead.' % coder)

  def test_get_coder_with_custom_coder(self):
    typecoders.registry.register_coder(CustomClass, CustomCoder)
    self.assertEqual(CustomCoder,
                     typecoders.registry.get_coder(CustomClass).__class__)

  def test_get_coder_with_composite_custom_coder(self):
    typecoders.registry.register_coder(CustomClass, CustomCoder)
    coder = typecoders.registry.get_coder(typehints.KV[CustomClass, str])
    revived_coder = pickler.loads(pickler.dumps(coder))
    self.assertEqual(
        (CustomClass(123), 'abc'),
        revived_coder.decode(revived_coder.encode((CustomClass(123), 'abc'))))

  def test_get_coder_with_standard_coder(self):
    self.assertEqual(coders.BytesCoder,
                     typecoders.registry.get_coder(str).__class__)

  def test_fallbackcoder(self):
    coder = typecoders.registry.get_coder(typehints.Any)
    self.assertEqual(('abc', 123), coder.decode(coder.encode(('abc', 123))))

  def test_get_coder_can_be_pickled(self):
    coder = typecoders.registry.get_coder(typehints.Tuple[str, int])
    revived_coder = pickler.loads(pickler.dumps(coder))
    self.assertEqual(('abc', 123),
                     revived_coder.decode(revived_coder.encode(('abc', 123))))

  def test_standard_int_coder(self):
    real_coder = typecoders.registry.get_coder(int)
    expected_coder = coders.VarIntCoder()
    self.assertEqual(
        real_coder.encode(0x0404), expected_coder.encode(0x0404))
    self.assertEqual(0x0404, real_coder.decode(real_coder.encode(0x0404)))
    self.assertEqual(
        real_coder.encode(0x040404040404),
        expected_coder.encode(0x040404040404))
    self.assertEqual(0x040404040404,
                     real_coder.decode(real_coder.encode(0x040404040404)))

  def test_standard_str_coder(self):
    real_coder = typecoders.registry.get_coder(str)
    expected_coder = coders.BytesCoder()
    self.assertEqual(
        real_coder.encode('abc'), expected_coder.encode('abc'))
    self.assertEqual('abc', real_coder.decode(real_coder.encode('abc')))

    real_coder = typecoders.registry.get_coder(bytes)
    expected_coder = coders.BytesCoder()
    self.assertEqual(
        real_coder.encode('abc'), expected_coder.encode('abc'))
    self.assertEqual('abc', real_coder.decode(real_coder.encode('abc')))


if __name__ == '__main__':
  unittest.main()
