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
# pytype: skip-file

import unittest

from apache_beam.coders import coders
from apache_beam.coders import typecoders
from apache_beam.internal import pickler
from apache_beam.typehints import typehints
from apache_beam.utils import windowed_value


class CustomClass(object):
  def __init__(self, n):
    self.number = n

  def __eq__(self, other):
    return self.number == other.number

  def __hash__(self):
    return self.number


class CustomCoder(coders.Coder):
  def encode(self, value):
    return str(value.number).encode('ASCII')

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
    with self.assertRaisesRegex(
        TypeError,
        ('Coder registration requires a coder class object. '
         'Received %r instead.' % coder)):

      # When registering a coder the coder class must be specified.
      typecoders.registry.register_coder(CustomClass, coder)

  def test_get_coder_with_custom_coder(self):
    typecoders.registry.register_coder(CustomClass, CustomCoder)
    self.assertEqual(
        CustomCoder, typecoders.registry.get_coder(CustomClass).__class__)

  def test_get_coder_with_composite_custom_coder(self):
    typecoders.registry.register_coder(CustomClass, CustomCoder)
    coder = typecoders.registry.get_coder(typehints.KV[CustomClass, str])
    revived_coder = pickler.loads(pickler.dumps(coder))
    self.assertEqual(
        (CustomClass(123), 'abc'),
        revived_coder.decode(revived_coder.encode((CustomClass(123), 'abc'))))

  def test_get_coder_with_standard_coder(self):
    self.assertEqual(
        coders.BytesCoder, typecoders.registry.get_coder(bytes).__class__)

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
    self.assertEqual(real_coder.encode(0x0404), expected_coder.encode(0x0404))
    self.assertEqual(0x0404, real_coder.decode(real_coder.encode(0x0404)))
    self.assertEqual(
        real_coder.encode(0x040404040404),
        expected_coder.encode(0x040404040404))
    self.assertEqual(
        0x040404040404, real_coder.decode(real_coder.encode(0x040404040404)))

  def test_standard_str_coder(self):
    real_coder = typecoders.registry.get_coder(bytes)
    expected_coder = coders.BytesCoder()
    self.assertEqual(real_coder.encode(b'abc'), expected_coder.encode(b'abc'))
    self.assertEqual(b'abc', real_coder.decode(real_coder.encode(b'abc')))

  def test_standard_bool_coder(self):
    real_coder = typecoders.registry.get_coder(bool)
    expected_coder = coders.BooleanCoder()
    self.assertEqual(real_coder.encode(True), expected_coder.encode(True))
    self.assertEqual(True, real_coder.decode(real_coder.encode(True)))
    self.assertEqual(real_coder.encode(False), expected_coder.encode(False))
    self.assertEqual(False, real_coder.decode(real_coder.encode(False)))

  def test_iterable_coder(self):
    real_coder = typecoders.registry.get_coder(typehints.Iterable[bytes])
    expected_coder = coders.IterableCoder(coders.BytesCoder())
    values = [b'abc', b'xyz']
    self.assertEqual(expected_coder, real_coder)
    self.assertEqual(real_coder.encode(values), expected_coder.encode(values))

  @unittest.skip('https://github.com/apache/beam/issues/21658')
  def test_list_coder(self):
    real_coder = typecoders.registry.get_coder(typehints.List[bytes])
    expected_coder = coders.IterableCoder(coders.BytesCoder())
    values = [b'abc', b'xyz']
    self.assertEqual(expected_coder, real_coder)
    self.assertEqual(real_coder.encode(values), expected_coder.encode(values))
    # IterableCoder.decode() always returns a list.  Its implementation,
    # IterableCoderImpl, *can* return a non-list if it is provided a read_state
    # object, but this is not possible using the atomic IterableCoder interface.
    self.assertIs(
        list, type(expected_coder.decode(expected_coder.encode(values))))

  def test_nullable_coder(self):
    expected_coder = coders.NullableCoder(coders.BytesCoder())
    real_coder = typecoders.registry.get_coder(typehints.Optional[bytes])
    self.assertEqual(expected_coder, real_coder)
    self.assertEqual(expected_coder.encode(None), real_coder.encode(None))
    self.assertEqual(expected_coder.encode(b'abc'), real_coder.encode(b'abc'))

  def test_paneinfo_coder(self):
    expected_coder = coders.PaneInfoCoder()
    real_coder = typecoders.registry.get_coder(windowed_value.PaneInfo)
    self.assertEqual(expected_coder, real_coder)
    for i in range(10):
      pane_info = windowed_value.PaneInfo(
          is_first=i == 0,
          is_last=i == 9,
          timing=windowed_value.PaneInfoTiming.EARLY,  # 0
          index=i,
          nonspeculative_index=-1)

      encoded = real_coder.encode(pane_info)

      self.assertEqual(expected_coder.encode(pane_info), encoded)
      self.assertEqual(pane_info, real_coder.decode(encoded))


if __name__ == '__main__':
  unittest.main()
