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

"""Tests common to all coder implementations."""

import logging
import math
import unittest

import dill

import coders
import observable


# Defined out of line for picklability.
class CustomCoder(coders.Coder):

  def encode(self, x):
    return str(x+1)

  def decode(self, encoded):
    return int(encoded) - 1


class CodersTest(unittest.TestCase):

  # These class methods ensure that we test each defined coder in both
  # nested and unnested context.

  @classmethod
  def setUpClass(cls):
    cls.seen = set()
    cls.seen_nested = set()

  @classmethod
  def tearDownClass(cls):
    standard = set(c
                   for c in coders.__dict__.values()
                   if isinstance(c, type) and issubclass(c, coders.Coder) and
                   'Base' not in c.__name__)
    standard -= set([coders.Coder,
                     coders.FastCoder,
                     coders.Base64PickleCoder,
                     coders.FloatCoder,
                     coders.TimestampCoder,
                     coders.ToStringCoder,
                     coders.WindowCoder,
                     coders.WindowedValueCoder])
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
      self.assertEqual(coder.estimate_size(v),
                       len(coder.encode(v)))
      self.assertEqual(coder.estimate_size(v),
                       coder.get_impl().estimate_size(v))
      self.assertEqual(coder.get_impl().get_estimated_size_and_observables(v),
                       (coder.get_impl().estimate_size(v), []))
    copy1 = dill.loads(dill.dumps(coder))
    copy2 = dill.loads(dill.dumps(coder))
    for v in values:
      self.assertEqual(v, copy1.decode(copy2.encode(v)))

  def test_custom_coder(self):

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

  def test_dill_coder(self):
    cell_value = (lambda x: lambda: x)(0).func_closure[0]
    self.check_coder(coders.DillCoder(), 'a', 1, cell_value)
    self.check_coder(
        coders.TupleCoder((coders.VarIntCoder(), coders.DillCoder())),
        (1, cell_value))

  def test_fast_primitives_coder(self):
    coder = coders.FastPrimitivesCoder(coders.SingletonCoder(len))
    self.check_coder(coder, None, 1, -1, 1.5, 'str\0str', u'unicode\0\u0101')
    self.check_coder(coder, (), (1, 2, 3))
    self.check_coder(coder, [], [1, 2, 3])
    self.check_coder(coder, dict(), {'a': 'b'}, {0: dict(), 1: len})
    self.check_coder(coder, len)
    self.check_coder(coders.TupleCoder((coder,)), ('a',), (1,))

  def test_bytes_coder(self):
    self.check_coder(coders.BytesCoder(), 'a', '\0', 'z' * 1000)

  def test_varint_coder(self):
    # Small ints.
    self.check_coder(coders.VarIntCoder(), *range(-10, 10))
    # Multi-byte encoding starts at 128
    self.check_coder(coders.VarIntCoder(), *range(120, 140))
    # Large values
    MAX_64_BIT_INT = 0x7fffffffffffffff
    self.check_coder(coders.VarIntCoder(),
                     *[int(math.pow(-1, k) * math.exp(k))
                       for k in range(0, int(math.log(MAX_64_BIT_INT)))])

  def test_float_coder(self):
    self.check_coder(coders.FloatCoder(),
                     *[float(0.1 * x) for x in range(-100, 100)])
    self.check_coder(coders.FloatCoder(),
                     *[float(2 ** (0.1 * x)) for x in range(-100, 100)])
    self.check_coder(coders.FloatCoder(), float('-Inf'), float('Inf'))

  def test_singleton_coder(self):
    a = 'anything'
    b = 'something else'
    self.check_coder(coders.SingletonCoder(a), a)
    self.check_coder(coders.SingletonCoder(b), b)
    self.check_coder(coders.TupleCoder((coders.SingletonCoder(a),
                                        coders.SingletonCoder(b))), (a, b))

  def test_timestamp_coder(self):
    self.check_coder(coders.TimestampCoder(),
                     *[coders.Timestamp(micros=x) for x in range(-100, 100)])
    self.check_coder(coders.TimestampCoder(),
                     coders.Timestamp(micros=-1234567890),
                     coders.Timestamp(micros=1234567890))
    self.check_coder(coders.TimestampCoder(),
                     coders.Timestamp(micros=-1234567890123456789),
                     coders.Timestamp(micros=1234567890123456789))

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

  def test_tuple_sequence_coder(self):
    int_tuple_coder = coders.TupleSequenceCoder(coders.VarIntCoder())
    self.check_coder(int_tuple_coder, (1, -1, 0), (), tuple(range(1000)))
    self.check_coder(
        coders.TupleCoder((coders.VarIntCoder(), int_tuple_coder)),
        (1, (1, 2, 3)))

  def test_base64_pickle_coder(self):
    self.check_coder(coders.Base64PickleCoder(), 'a', 1, 1.5, (1, 2, 3))

  def test_utf8_coder(self):
    self.check_coder(coders.StrUtf8Coder(), 'a', u'ab\u00FF', u'\u0101\0')

  def test_nested_observables(self):
    class FakeObservableIterator(observable.ObservableMixin):

      def __iter__(self):
        return iter([1, 2, 3])

    # Coder for elements from the observable iterator.
    iter_coder = coders.VarIntCoder()

    # Test nested WindowedValue observable.
    coder = coders.WindowedValueCoder(iter_coder)
    observ = FakeObservableIterator()
    try:
      value = coders.coder_impl.WindowedValue(observ)
    except TypeError:
      # We are running tests with a fake WindowedValue implementation so as to
      # not pull in the rest of the SDK.
      value = coders.coder_impl.WindowedValue(observ, 0, [])
    self.assertEqual(
        coder.get_impl().get_estimated_size_and_observables(value)[1],
        [(observ, iter_coder.get_impl())])

    # Test nested tuple observable.
    coder = coders.TupleCoder((coders.StrUtf8Coder(), iter_coder))
    value = (u'123', observ)
    self.assertEqual(
        coder.get_impl().get_estimated_size_and_observables(value)[1],
        [(observ, iter_coder.get_impl())])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
