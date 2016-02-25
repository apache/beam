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

"""Tests common to all coder implementations."""

import logging
import math
import sys
import unittest

import coders


class CodersTest(unittest.TestCase):

  # These class methods ensure that we test each defined coder in both
  # nested and unnested context.

  @classmethod
  def setUpClass(cls):
    cls.seen = set()
    cls.seen_nested = set()

  @classmethod
  def tearDownClass(cls):
    standard = set(
        c for c in coders.__dict__.values()
        if isinstance(c, type) and issubclass(c, coders.Coder))
    standard -= set([coders.Coder, coders.ToStringCoder, coders.FloatCoder,
                     coders.Base64PickleCoder, coders.FastCoder,
                     coders.WindowCoder, coders.WindowedValueCoder])
    assert not standard - cls.seen, standard - cls.seen
    assert not standard - cls.seen_nested, standard - cls.seen_nested

  @classmethod
  def _observe(cls, coder):
    cls.seen.add(type(coder))
    cls._observe_nested(coder)

  @classmethod
  def _observe_nested(cls, coder):
    if isinstance(coder, coders.TupleCoder):
      for c in coder.coders():
        cls.seen_nested.add(type(c))
        cls._observe_nested(c)

  def check_coder(self, coder, *values):
    self._observe(coder)
    for v in values:
      self.assertEqual(v, coder.decode(coder.encode(v)))

  def test_custom_coder(self):
    class CustomCoder(coders.Coder):

      def encode(self, x):
        return str(x+1)

      def decode(self, encoded):
        return int(encoded) - 1

    self.check_coder(CustomCoder(), 1, -10, 5)
    self.check_coder(coders.TupleCoder((CustomCoder(), coders.BytesCoder())),
                     (1, 'a'), (-10, 'b'), (5, 'c'))

  def test_pickle_coder(self):
    self.check_coder(coders.PickleCoder(), 'a', 1, 1.5, (1, 2, 3))

  def test_deterministic_pickle_coder(self):
    coder = coders.DeterministicPickleCoder(coders.PickleCoder(), 'step')
    self.check_coder(coder, 'a', 1, 1.5, (1, 2, 3))
    with self.assertRaises(TypeError):
      self.check_coder(coder, dict())
    with self.assertRaises(TypeError):
      self.check_coder(coder, [1, dict()])

    self.check_coder(coders.TupleCoder((coder, coders.PickleCoder())),
                     (1, dict()), ('a', [dict()]))

  def test_bytes_coder(self):
    self.check_coder(coders.BytesCoder(), 'a', '\0', 'z' * 1000)

  def test_varint_coder(self):
    # Small ints.
    self.check_coder(coders.VarIntCoder(), *range(-10, 10))
    # Multi-byte encoding starts at 128
    self.check_coder(coders.VarIntCoder(), *range(120, 140))
    # Large values
    self.check_coder(coders.VarIntCoder(),
                     *[int(math.pow(-1, k) * math.exp(k))
                       for k in range(0, int(math.log(sys.maxint)))])

  def test_float_coder(self):
    self.check_coder(coders.FloatCoder(),
                     *[float(0.1 * x) for x in range(-100, 100)])
    self.check_coder(coders.FloatCoder(),
                     *[float(2 ** (0.1 * x)) for x in range(-100, 100)])
    self.check_coder(coders.FloatCoder(), float('-Inf'), float('Inf'))

  def test_tuple_coder(self):
    self.check_coder(
        coders.TupleCoder((coders.VarIntCoder(), coders.BytesCoder())),
        (1, 'a'),
        (-2, 'a' * 100),
        (300, 'abc\0' * 5))
    self.check_coder(
        coders.TupleCoder(
            (coders.TupleCoder((coders.PickleCoder(), coders.VarIntCoder())),
             coders.StrUtf8Coder())),
        ((1, 2), 'a'),
        ((-2, 5), u'a\u0101' * 100),
        ((300, 1), 'abc\0' * 5))

  def test_base64_pickle_coder(self):
    self.check_coder(coders.Base64PickleCoder(), 'a', 1, 1.5, (1, 2, 3))

  def test_utf8_coder(self):
    self.check_coder(coders.StrUtf8Coder(), 'a', u'ab\u00FF', u'\u0101\0')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
