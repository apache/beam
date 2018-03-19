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
from __future__ import absolute_import

import logging
import math
import unittest

import dill

from apache_beam.coders import proto2_coder_test_messages_pb2 as test_message
from apache_beam.coders import coders
from apache_beam.runners import pipeline_context
from apache_beam.transforms import window
from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils import timestamp
from apache_beam.utils import windowed_value
from apache_beam.utils.timestamp import MIN_TIMESTAMP

from . import observable


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
                     coders.ProtoCoder,
                     coders.ToStringCoder])
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
    context = pipeline_context.PipelineContext()
    copy2 = coders.Coder.from_runner_api(coder.to_runner_api(context), context)
    for v in values:
      self.assertEqual(v, copy1.decode(copy2.encode(v)))
      if coder.is_deterministic():
        self.assertEqual(copy1.encode(v), copy2.encode(v))

  def test_custom_coder(self):

    self.check_coder(CustomCoder(), 1, -10, 5)
    self.check_coder(coders.TupleCoder((CustomCoder(), coders.BytesCoder())),
                     (1, 'a'), (-10, 'b'), (5, 'c'))

  def test_pickle_coder(self):
    self.check_coder(coders.PickleCoder(), 'a', 1, 1.5, (1, 2, 3))

  def test_deterministic_coder(self):
    coder = coders.FastPrimitivesCoder()
    deterministic_coder = coders.DeterministicFastPrimitivesCoder(coder, 'step')
    self.check_coder(deterministic_coder, 'a', 1, 1.5, (1, 2, 3))
    with self.assertRaises(TypeError):
      self.check_coder(deterministic_coder, dict())
    with self.assertRaises(TypeError):
      self.check_coder(deterministic_coder, [1, dict()])

    self.check_coder(coders.TupleCoder((deterministic_coder, coder)),
                     (1, dict()), ('a', [dict()]))

  def test_dill_coder(self):
    cell_value = (lambda x: lambda: x)(0).__closure__[0]
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
    self.check_coder(coder, set(), {'a', 'b'})
    self.check_coder(coder, True, False)
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
    self.check_coder(
        coders.TupleCoder((coders.FloatCoder(), coders.FloatCoder())),
        (0, 1), (-100, 100), (0.5, 0.25))

  def test_singleton_coder(self):
    a = 'anything'
    b = 'something else'
    self.check_coder(coders.SingletonCoder(a), a)
    self.check_coder(coders.SingletonCoder(b), b)
    self.check_coder(coders.TupleCoder((coders.SingletonCoder(a),
                                        coders.SingletonCoder(b))), (a, b))

  def test_interval_window_coder(self):
    self.check_coder(coders.IntervalWindowCoder(),
                     *[window.IntervalWindow(x, y)
                       for x in [-2**52, 0, 2**52]
                       for y in range(-100, 100)])
    self.check_coder(
        coders.TupleCoder((coders.IntervalWindowCoder(),)),
        (window.IntervalWindow(0, 10),))

  def test_timestamp_coder(self):
    self.check_coder(coders.TimestampCoder(),
                     *[timestamp.Timestamp(micros=x) for x in range(-100, 100)])
    self.check_coder(coders.TimestampCoder(),
                     timestamp.Timestamp(micros=-1234567890),
                     timestamp.Timestamp(micros=1234567890))
    self.check_coder(coders.TimestampCoder(),
                     timestamp.Timestamp(micros=-1234567890123456789),
                     timestamp.Timestamp(micros=1234567890123456789))
    self.check_coder(
        coders.TupleCoder((coders.TimestampCoder(), coders.BytesCoder())),
        (timestamp.Timestamp.of(27), 'abc'))

  def test_tuple_coder(self):
    kv_coder = coders.TupleCoder((coders.VarIntCoder(), coders.BytesCoder()))
    # Verify cloud object representation
    self.assertEqual(
        {
            '@type': 'kind:pair',
            'is_pair_like': True,
            'component_encodings': [
                coders.VarIntCoder().as_cloud_object(),
                coders.BytesCoder().as_cloud_object()],
        },
        kv_coder.as_cloud_object())
    # Test binary representation
    self.assertEqual(
        '\x04abc',
        kv_coder.encode((4, 'abc')))
    # Test unnested
    self.check_coder(
        kv_coder,
        (1, 'a'),
        (-2, 'a' * 100),
        (300, 'abc\0' * 5))
    # Test nested
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

  def test_iterable_coder(self):
    iterable_coder = coders.IterableCoder(coders.VarIntCoder())
    # Verify cloud object representation
    self.assertEqual(
        {
            '@type': 'kind:stream',
            'is_stream_like': True,
            'component_encodings': [coders.VarIntCoder().as_cloud_object()]
        },
        iterable_coder.as_cloud_object())
    # Test unnested
    self.check_coder(iterable_coder,
                     [1], [-1, 0, 100])
    # Test nested
    self.check_coder(
        coders.TupleCoder((coders.VarIntCoder(),
                           coders.IterableCoder(coders.VarIntCoder()))),
        (1, [1, 2, 3]))

  def test_iterable_coder_unknown_length(self):
    # Empty
    self._test_iterable_coder_of_unknown_length(0)
    # Single element
    self._test_iterable_coder_of_unknown_length(1)
    # Multiple elements
    self._test_iterable_coder_of_unknown_length(100)
    # Multiple elements with underlying stream buffer overflow.
    self._test_iterable_coder_of_unknown_length(80000)

  def _test_iterable_coder_of_unknown_length(self, count):
    def iter_generator(count):
      for i in range(count):
        yield i

    iterable_coder = coders.IterableCoder(coders.VarIntCoder())
    self.assertItemsEqual(list(iter_generator(count)),
                          iterable_coder.decode(
                              iterable_coder.encode(iter_generator(count))))

  def test_windowedvalue_coder_paneinfo(self):
    coder = coders.WindowedValueCoder(coders.VarIntCoder(),
                                      coders.GlobalWindowCoder())
    test_paneinfo_values = [
        windowed_value.PANE_INFO_UNKNOWN,
        windowed_value.PaneInfo(
            True, True, windowed_value.PaneInfoTiming.EARLY, 0, -1),
        windowed_value.PaneInfo(
            True, False, windowed_value.PaneInfoTiming.ON_TIME, 0, 0),
        windowed_value.PaneInfo(
            True, False, windowed_value.PaneInfoTiming.ON_TIME, 10, 0),
        windowed_value.PaneInfo(
            False, True, windowed_value.PaneInfoTiming.ON_TIME, 0, 23),
        windowed_value.PaneInfo(
            False, True, windowed_value.PaneInfoTiming.ON_TIME, 12, 23),
        windowed_value.PaneInfo(
            False, False, windowed_value.PaneInfoTiming.LATE, 0, 123),]

    test_values = [windowed_value.WindowedValue(123, 234, (GlobalWindow(),), p)
                   for p in test_paneinfo_values]

    # Test unnested.
    self.check_coder(coder, windowed_value.WindowedValue(
        123, 234, (GlobalWindow(),), windowed_value.PANE_INFO_UNKNOWN))
    for value in test_values:
      self.check_coder(coder, value)

    # Test nested.
    for value1 in test_values:
      for value2 in test_values:
        self.check_coder(coders.TupleCoder((coder, coder)), (value1, value2))

  def test_windowed_value_coder(self):
    coder = coders.WindowedValueCoder(coders.VarIntCoder(),
                                      coders.GlobalWindowCoder())
    # Verify cloud object representation
    self.assertEqual(
        {
            '@type': 'kind:windowed_value',
            'is_wrapper': True,
            'component_encodings': [
                coders.VarIntCoder().as_cloud_object(),
                coders.GlobalWindowCoder().as_cloud_object(),
            ],
        },
        coder.as_cloud_object())
    # Test binary representation
    self.assertEqual('\x7f\xdf;dZ\x1c\xac\t\x00\x00\x00\x01\x0f\x01',
                     coder.encode(window.GlobalWindows.windowed_value(1)))

    # Test decoding large timestamp
    self.assertEqual(
        coder.decode('\x7f\xdf;dZ\x1c\xac\x08\x00\x00\x00\x01\x0f\x00'),
        windowed_value.create(0, MIN_TIMESTAMP.micros, (GlobalWindow(),)))

    # Test unnested
    self.check_coder(
        coders.WindowedValueCoder(coders.VarIntCoder()),
        windowed_value.WindowedValue(3, -100, ()),
        windowed_value.WindowedValue(-1, 100, (1, 2, 3)))

    # Test Global Window
    self.check_coder(
        coders.WindowedValueCoder(coders.VarIntCoder(),
                                  coders.GlobalWindowCoder()),
        window.GlobalWindows.windowed_value(1))

    # Test nested
    self.check_coder(
        coders.TupleCoder((
            coders.WindowedValueCoder(coders.FloatCoder()),
            coders.WindowedValueCoder(coders.StrUtf8Coder()))),
        (windowed_value.WindowedValue(1.5, 0, ()),
         windowed_value.WindowedValue("abc", 10, ('window',))))

  def test_proto_coder(self):
    # For instructions on how these test proto message were generated,
    # see coders_test.py
    ma = test_message.MessageA()
    mab = ma.field2.add()
    mab.field1 = True
    ma.field1 = u'hello world'

    mb = test_message.MessageA()
    mb.field1 = u'beam'

    proto_coder = coders.ProtoCoder(ma.__class__)
    self.check_coder(proto_coder, ma)
    self.check_coder(coders.TupleCoder((proto_coder, coders.BytesCoder())),
                     (ma, 'a'), (mb, 'b'))

  def test_global_window_coder(self):
    coder = coders.GlobalWindowCoder()
    value = window.GlobalWindow()
    # Verify cloud object representation
    self.assertEqual({'@type': 'kind:global_window'},
                     coder.as_cloud_object())
    # Test binary representation
    self.assertEqual('', coder.encode(value))
    self.assertEqual(value, coder.decode(''))
    # Test unnested
    self.check_coder(coder, value)
    # Test nested
    self.check_coder(coders.TupleCoder((coder, coder)),
                     (value, value))

  def test_length_prefix_coder(self):
    coder = coders.LengthPrefixCoder(coders.BytesCoder())
    # Verify cloud object representation
    self.assertEqual(
        {
            '@type': 'kind:length_prefix',
            'component_encodings': [coders.BytesCoder().as_cloud_object()]
        },
        coder.as_cloud_object())
    # Test binary representation
    self.assertEqual('\x00', coder.encode(''))
    self.assertEqual('\x01a', coder.encode('a'))
    self.assertEqual('\x02bc', coder.encode('bc'))
    self.assertEqual('\xff\x7f' + 'z' * 16383, coder.encode('z' * 16383))
    # Test unnested
    self.check_coder(coder, '', 'a', 'bc', 'def')
    # Test nested
    self.check_coder(coders.TupleCoder((coder, coder)),
                     ('', 'a'),
                     ('bc', 'def'))

  def test_nested_observables(self):
    class FakeObservableIterator(observable.ObservableMixin):

      def __iter__(self):
        return iter([1, 2, 3])

    # Coder for elements from the observable iterator.
    elem_coder = coders.VarIntCoder()
    iter_coder = coders.TupleSequenceCoder(elem_coder)

    # Test nested WindowedValue observable.
    coder = coders.WindowedValueCoder(iter_coder)
    observ = FakeObservableIterator()
    value = windowed_value.WindowedValue(observ, 0, ())
    self.assertEqual(
        coder.get_impl().get_estimated_size_and_observables(value)[1],
        [(observ, elem_coder.get_impl())])

    # Test nested tuple observable.
    coder = coders.TupleCoder((coders.StrUtf8Coder(), iter_coder))
    value = (u'123', observ)
    self.assertEqual(
        coder.get_impl().get_estimated_size_and_observables(value)[1],
        [(observ, elem_coder.get_impl())])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
