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
# pytype: skip-file

import base64
import collections
import enum
import logging
import math
import unittest
from decimal import Decimal
from typing import Any
from typing import List
from typing import NamedTuple

import pytest

from apache_beam.coders import proto2_coder_test_messages_pb2 as test_message
from apache_beam.coders import coders
from apache_beam.coders import typecoders
from apache_beam.internal import pickler
from apache_beam.runners import pipeline_context
from apache_beam.transforms import userstate
from apache_beam.transforms import window
from apache_beam.transforms.window import GlobalWindow
from apache_beam.typehints import sharded_key_type
from apache_beam.typehints import typehints
from apache_beam.utils import timestamp
from apache_beam.utils import windowed_value
from apache_beam.utils.sharded_key import ShardedKey
from apache_beam.utils.timestamp import MIN_TIMESTAMP

from . import observable

try:
  import dataclasses
except ImportError:
  dataclasses = None  # type: ignore

MyNamedTuple = collections.namedtuple('A', ['x', 'y'])
MyTypedNamedTuple = NamedTuple('MyTypedNamedTuple', [('f1', int), ('f2', str)])


class MyEnum(enum.Enum):
  E1 = 5
  E2 = enum.auto()
  E3 = 'abc'


MyIntEnum = enum.IntEnum('MyIntEnum', 'I1 I2 I3')
MyIntFlag = enum.IntFlag('MyIntFlag', 'F1 F2 F3')
MyFlag = enum.Flag('MyFlag', 'F1 F2 F3')  # pylint: disable=too-many-function-args


class DefinesGetState:
  def __init__(self, value):
    self.value = value

  def __getstate__(self):
    return self.value

  def __eq__(self, other):
    return type(other) is type(self) and other.value == self.value


class DefinesGetAndSetState(DefinesGetState):
  def __setstate__(self, value):
    self.value = value


# Defined out of line for picklability.
class CustomCoder(coders.Coder):
  def encode(self, x):
    return str(x + 1).encode('utf-8')

  def decode(self, encoded):
    return int(encoded) - 1


if dataclasses is not None:

  @dataclasses.dataclass(frozen=True)
  class FrozenDataClass:
    a: Any
    b: int

  @dataclasses.dataclass
  class UnFrozenDataClass:
    x: int
    y: int


# These tests need to all be run in the same process due to the asserts
# in tearDownClass.
@pytest.mark.no_xdist
class CodersTest(unittest.TestCase):

  # These class methods ensure that we test each defined coder in both
  # nested and unnested context.

  # Common test values representing Python's built-in types.
  test_values_deterministic: List[Any] = [
      None,
      1,
      -1,
      1.5,
      b'str\0str',
      u'unicode\0\u0101',
      (),
      (1, 2, 3),
      [],
      [1, 2, 3],
      True,
      False,
  ]
  test_values = test_values_deterministic + [
      {},
      {
          'a': 'b'
      },
      {
          0: {}, 1: len
      },
      set(),
      {'a', 'b'},
      len,
  ]

  @classmethod
  def setUpClass(cls):
    cls.seen = set()
    cls.seen_nested = set()

  @classmethod
  def tearDownClass(cls):
    standard = set(
        c for c in coders.__dict__.values() if isinstance(c, type) and
        issubclass(c, coders.Coder) and 'Base' not in c.__name__)
    standard -= set([
        coders.Coder,
        coders.AvroGenericCoder,
        coders.DeterministicProtoCoder,
        coders.FastCoder,
        coders.ListLikeCoder,
        coders.ProtoCoder,
        coders.ProtoPlusCoder,
        coders.SinglePrecisionFloatCoder,
        coders.ToBytesCoder,
        coders.BigIntegerCoder, # tested in DecimalCoder
    ])
    cls.seen_nested -= set(
        [coders.ProtoCoder, coders.ProtoPlusCoder, CustomCoder])
    assert not standard - cls.seen, str(standard - cls.seen)
    assert not cls.seen_nested - standard, str(cls.seen_nested - standard)

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

  def check_coder(self, coder, *values, **kwargs):
    context = kwargs.pop('context', pipeline_context.PipelineContext())
    test_size_estimation = kwargs.pop('test_size_estimation', True)
    assert not kwargs
    self._observe(coder)
    for v in values:
      self.assertEqual(v, coder.decode(coder.encode(v)))
      if test_size_estimation:
        self.assertEqual(coder.estimate_size(v), len(coder.encode(v)))
        self.assertEqual(
            coder.estimate_size(v), coder.get_impl().estimate_size(v))
        self.assertEqual(
            coder.get_impl().get_estimated_size_and_observables(v),
            (coder.get_impl().estimate_size(v), []))
      copy1 = pickler.loads(pickler.dumps(coder))
    copy2 = coders.Coder.from_runner_api(coder.to_runner_api(context), context)
    for v in values:
      self.assertEqual(v, copy1.decode(copy2.encode(v)))
      if coder.is_deterministic():
        self.assertEqual(copy1.encode(v), copy2.encode(v))

  def test_custom_coder(self):

    self.check_coder(CustomCoder(), 1, -10, 5)
    self.check_coder(
        coders.TupleCoder((CustomCoder(), coders.BytesCoder())), (1, b'a'),
        (-10, b'b'), (5, b'c'))

  def test_pickle_coder(self):
    coder = coders.PickleCoder()
    self.check_coder(coder, *self.test_values)

  def test_memoizing_pickle_coder(self):
    coder = coders._MemoizingPickleCoder()
    self.check_coder(coder, *self.test_values)

  def test_deterministic_coder(self):
    coder = coders.FastPrimitivesCoder()
    deterministic_coder = coders.DeterministicFastPrimitivesCoder(coder, 'step')
    self.check_coder(deterministic_coder, *self.test_values_deterministic)
    for v in self.test_values_deterministic:
      self.check_coder(coders.TupleCoder((deterministic_coder, )), (v, ))
    self.check_coder(
        coders.TupleCoder(
            (deterministic_coder, ) * len(self.test_values_deterministic)),
        tuple(self.test_values_deterministic))

    self.check_coder(deterministic_coder, {})
    self.check_coder(deterministic_coder, {2: 'x', 1: 'y'})
    with self.assertRaises(TypeError):
      self.check_coder(deterministic_coder, {1: 'x', 'y': 2})
    self.check_coder(deterministic_coder, [1, {}])
    with self.assertRaises(TypeError):
      self.check_coder(deterministic_coder, [1, {1: 'x', 'y': 2}])

    self.check_coder(
        coders.TupleCoder((deterministic_coder, coder)), (1, {}), ('a', [{}]))

    self.check_coder(deterministic_coder, test_message.MessageA(field1='value'))

    self.check_coder(
        deterministic_coder, [MyNamedTuple(1, 2), MyTypedNamedTuple(1, 'a')])

    if dataclasses is not None:
      self.check_coder(deterministic_coder, FrozenDataClass(1, 2))

      with self.assertRaises(TypeError):
        self.check_coder(deterministic_coder, UnFrozenDataClass(1, 2))
      with self.assertRaises(TypeError):
        self.check_coder(
            deterministic_coder, FrozenDataClass(UnFrozenDataClass(1, 2), 3))
      with self.assertRaises(TypeError):
        self.check_coder(
            deterministic_coder, MyNamedTuple(UnFrozenDataClass(1, 2), 3))

    self.check_coder(deterministic_coder, list(MyEnum))
    self.check_coder(deterministic_coder, list(MyIntEnum))
    self.check_coder(deterministic_coder, list(MyIntFlag))
    self.check_coder(deterministic_coder, list(MyFlag))

    self.check_coder(
        deterministic_coder,
        [DefinesGetAndSetState(1), DefinesGetAndSetState((1, 2, 3))])

    with self.assertRaises(TypeError):
      self.check_coder(deterministic_coder, DefinesGetState(1))
    with self.assertRaises(TypeError):
      self.check_coder(
          deterministic_coder, DefinesGetAndSetState({
              1: 'x', 'y': 2
          }))

  def test_dill_coder(self):
    cell_value = (lambda x: lambda: x)(0).__closure__[0]
    self.check_coder(coders.DillCoder(), 'a', 1, cell_value)
    self.check_coder(
        coders.TupleCoder((coders.VarIntCoder(), coders.DillCoder())),
        (1, cell_value))

  def test_fast_primitives_coder(self):
    coder = coders.FastPrimitivesCoder(coders.SingletonCoder(len))
    self.check_coder(coder, *self.test_values)
    for v in self.test_values:
      self.check_coder(coders.TupleCoder((coder, )), (v, ))

  def test_fast_primitives_coder_large_int(self):
    coder = coders.FastPrimitivesCoder()
    self.check_coder(coder, 10**100)

  def test_fake_deterministic_fast_primitives_coder(self):
    coder = coders.FakeDeterministicFastPrimitivesCoder(coders.PickleCoder())
    self.check_coder(coder, *self.test_values)
    for v in self.test_values:
      self.check_coder(coders.TupleCoder((coder, )), (v, ))

  def test_bytes_coder(self):
    self.check_coder(coders.BytesCoder(), b'a', b'\0', b'z' * 1000)

  def test_bool_coder(self):
    self.check_coder(coders.BooleanCoder(), True, False)

  def test_varint_coder(self):
    # Small ints.
    self.check_coder(coders.VarIntCoder(), *range(-10, 10))
    # Multi-byte encoding starts at 128
    self.check_coder(coders.VarIntCoder(), *range(120, 140))
    # Large values
    MAX_64_BIT_INT = 0x7fffffffffffffff
    self.check_coder(
        coders.VarIntCoder(),
        *[
            int(math.pow(-1, k) * math.exp(k))
            for k in range(0, int(math.log(MAX_64_BIT_INT)))
        ])

  def test_float_coder(self):
    self.check_coder(
        coders.FloatCoder(), *[float(0.1 * x) for x in range(-100, 100)])
    self.check_coder(
        coders.FloatCoder(), *[float(2**(0.1 * x)) for x in range(-100, 100)])
    self.check_coder(coders.FloatCoder(), float('-Inf'), float('Inf'))
    self.check_coder(
        coders.TupleCoder((coders.FloatCoder(), coders.FloatCoder())), (0, 1),
        (-100, 100), (0.5, 0.25))

  def test_singleton_coder(self):
    a = 'anything'
    b = 'something else'
    self.check_coder(coders.SingletonCoder(a), a)
    self.check_coder(coders.SingletonCoder(b), b)
    self.check_coder(
        coders.TupleCoder((coders.SingletonCoder(a), coders.SingletonCoder(b))),
        (a, b))

  def test_interval_window_coder(self):
    self.check_coder(
        coders.IntervalWindowCoder(),
        *[
            window.IntervalWindow(x, y) for x in [-2**52, 0, 2**52]
            for y in range(-100, 100)
        ])
    self.check_coder(
        coders.TupleCoder((coders.IntervalWindowCoder(), )),
        (window.IntervalWindow(0, 10), ))

  def test_timestamp_coder(self):
    self.check_coder(
        coders.TimestampCoder(),
        *[timestamp.Timestamp(micros=x) for x in (-1000, 0, 1000)])
    self.check_coder(
        coders.TimestampCoder(),
        timestamp.Timestamp(micros=-1234567000),
        timestamp.Timestamp(micros=1234567000))
    self.check_coder(
        coders.TimestampCoder(),
        timestamp.Timestamp(micros=-1234567890123456000),
        timestamp.Timestamp(micros=1234567890123456000))
    self.check_coder(
        coders.TupleCoder((coders.TimestampCoder(), coders.BytesCoder())),
        (timestamp.Timestamp.of(27), b'abc'))

  def test_timer_coder(self):
    self.check_coder(
        coders._TimerCoder(coders.StrUtf8Coder(), coders.GlobalWindowCoder()),
        *[
            userstate.Timer(
                user_key="key",
                dynamic_timer_tag="tag",
                windows=(GlobalWindow(), ),
                clear_bit=True,
                fire_timestamp=None,
                hold_timestamp=None,
                paneinfo=None),
            userstate.Timer(
                user_key="key",
                dynamic_timer_tag="tag",
                windows=(GlobalWindow(), ),
                clear_bit=False,
                fire_timestamp=timestamp.Timestamp.of(123),
                hold_timestamp=timestamp.Timestamp.of(456),
                paneinfo=windowed_value.PANE_INFO_UNKNOWN)
        ])

  def test_tuple_coder(self):
    kv_coder = coders.TupleCoder((coders.VarIntCoder(), coders.BytesCoder()))
    # Verify cloud object representation
    self.assertEqual({
        '@type': 'kind:pair',
        'is_pair_like': True,
        'component_encodings': [
            coders.VarIntCoder().as_cloud_object(),
            coders.BytesCoder().as_cloud_object()
        ],
    },
                     kv_coder.as_cloud_object())
    # Test binary representation
    self.assertEqual(b'\x04abc', kv_coder.encode((4, b'abc')))
    # Test unnested
    self.check_coder(kv_coder, (1, b'a'), (-2, b'a' * 100), (300, b'abc\0' * 5))
    # Test nested
    self.check_coder(
        coders.TupleCoder((
            coders.TupleCoder((coders.PickleCoder(), coders.VarIntCoder())),
            coders.StrUtf8Coder(),
            coders.BooleanCoder())), ((1, 2), 'a', True),
        ((-2, 5), u'a\u0101' * 100, False), ((300, 1), 'abc\0' * 5, True))

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
    self.assertEqual({
        '@type': 'kind:stream',
        'is_stream_like': True,
        'component_encodings': [coders.VarIntCoder().as_cloud_object()]
    },
                     iterable_coder.as_cloud_object())
    # Test unnested
    self.check_coder(iterable_coder, [1], [-1, 0, 100])
    # Test nested
    self.check_coder(
        coders.TupleCoder(
            (coders.VarIntCoder(), coders.IterableCoder(coders.VarIntCoder()))),
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
    self.assertCountEqual(
        list(iter_generator(count)),
        iterable_coder.decode(iterable_coder.encode(iter_generator(count))))

  def test_list_coder(self):
    list_coder = coders.ListCoder(coders.VarIntCoder())
    # Test unnested
    self.check_coder(list_coder, [1], [-1, 0, 100])
    # Test nested
    self.check_coder(
        coders.TupleCoder((coders.VarIntCoder(), list_coder)), (1, [1, 2, 3]))

  def test_windowedvalue_coder_paneinfo(self):
    coder = coders.WindowedValueCoder(
        coders.VarIntCoder(), coders.GlobalWindowCoder())
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
            False, False, windowed_value.PaneInfoTiming.LATE, 0, 123),
    ]

    test_values = [
        windowed_value.WindowedValue(123, 234, (GlobalWindow(), ), p)
        for p in test_paneinfo_values
    ]

    # Test unnested.
    self.check_coder(
        coder,
        windowed_value.WindowedValue(
            123, 234, (GlobalWindow(), ), windowed_value.PANE_INFO_UNKNOWN))
    for value in test_values:
      self.check_coder(coder, value)

    # Test nested.
    for value1 in test_values:
      for value2 in test_values:
        self.check_coder(coders.TupleCoder((coder, coder)), (value1, value2))

  def test_windowed_value_coder(self):
    coder = coders.WindowedValueCoder(
        coders.VarIntCoder(), coders.GlobalWindowCoder())
    # Verify cloud object representation
    self.assertEqual({
        '@type': 'kind:windowed_value',
        'is_wrapper': True,
        'component_encodings': [
            coders.VarIntCoder().as_cloud_object(),
            coders.GlobalWindowCoder().as_cloud_object(),
        ],
    },
                     coder.as_cloud_object())
    # Test binary representation
    self.assertEqual(
        b'\x7f\xdf;dZ\x1c\xac\t\x00\x00\x00\x01\x0f\x01',
        coder.encode(window.GlobalWindows.windowed_value(1)))

    # Test decoding large timestamp
    self.assertEqual(
        coder.decode(b'\x7f\xdf;dZ\x1c\xac\x08\x00\x00\x00\x01\x0f\x00'),
        windowed_value.create(0, MIN_TIMESTAMP.micros, (GlobalWindow(), )))

    # Test unnested
    self.check_coder(
        coders.WindowedValueCoder(coders.VarIntCoder()),
        windowed_value.WindowedValue(3, -100, ()),
        windowed_value.WindowedValue(-1, 100, (1, 2, 3)))

    # Test Global Window
    self.check_coder(
        coders.WindowedValueCoder(
            coders.VarIntCoder(), coders.GlobalWindowCoder()),
        window.GlobalWindows.windowed_value(1))

    # Test nested
    self.check_coder(
        coders.TupleCoder((
            coders.WindowedValueCoder(coders.FloatCoder()),
            coders.WindowedValueCoder(coders.StrUtf8Coder()))),
        (
            windowed_value.WindowedValue(1.5, 0, ()),
            windowed_value.WindowedValue("abc", 10, ('window', ))))

  def test_param_windowed_value_coder(self):
    from apache_beam.transforms.window import IntervalWindow
    from apache_beam.utils.windowed_value import PaneInfo
    wv = windowed_value.create(
        b'',
        # Milliseconds to microseconds
        1000 * 1000,
        (IntervalWindow(11, 21), ),
        PaneInfo(True, False, 1, 2, 3))
    windowed_value_coder = coders.WindowedValueCoder(
        coders.BytesCoder(), coders.IntervalWindowCoder())
    payload = windowed_value_coder.encode(wv)
    coder = coders.ParamWindowedValueCoder(
        payload, [coders.VarIntCoder(), coders.IntervalWindowCoder()])

    # Test binary representation
    self.assertEqual(
        b'\x01', coder.encode(window.GlobalWindows.windowed_value(1)))

    # Test unnested
    self.check_coder(
        coders.ParamWindowedValueCoder(
            payload, [coders.VarIntCoder(), coders.IntervalWindowCoder()]),
        windowed_value.WindowedValue(
            3,
            1, (window.IntervalWindow(11, 21), ),
            PaneInfo(True, False, 1, 2, 3)),
        windowed_value.WindowedValue(
            1,
            1, (window.IntervalWindow(11, 21), ),
            PaneInfo(True, False, 1, 2, 3)))

    # Test nested
    self.check_coder(
        coders.TupleCoder((
            coders.ParamWindowedValueCoder(
                payload, [coders.FloatCoder(), coders.IntervalWindowCoder()]),
            coders.ParamWindowedValueCoder(
                payload,
                [coders.StrUtf8Coder(), coders.IntervalWindowCoder()]))),
        (
            windowed_value.WindowedValue(
                1.5,
                1, (window.IntervalWindow(11, 21), ),
                PaneInfo(True, False, 1, 2, 3)),
            windowed_value.WindowedValue(
                "abc",
                1, (window.IntervalWindow(11, 21), ),
                PaneInfo(True, False, 1, 2, 3))))

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
    self.check_coder(
        coders.TupleCoder((proto_coder, coders.BytesCoder())), (ma, b'a'),
        (mb, b'b'))

  def test_global_window_coder(self):
    coder = coders.GlobalWindowCoder()
    value = window.GlobalWindow()
    # Verify cloud object representation
    self.assertEqual({'@type': 'kind:global_window'}, coder.as_cloud_object())
    # Test binary representation
    self.assertEqual(b'', coder.encode(value))
    self.assertEqual(value, coder.decode(b''))
    # Test unnested
    self.check_coder(coder, value)
    # Test nested
    self.check_coder(coders.TupleCoder((coder, coder)), (value, value))

  def test_length_prefix_coder(self):
    coder = coders.LengthPrefixCoder(coders.BytesCoder())
    # Verify cloud object representation
    self.assertEqual({
        '@type': 'kind:length_prefix',
        'component_encodings': [coders.BytesCoder().as_cloud_object()]
    },
                     coder.as_cloud_object())
    # Test binary representation
    self.assertEqual(b'\x00', coder.encode(b''))
    self.assertEqual(b'\x01a', coder.encode(b'a'))
    self.assertEqual(b'\x02bc', coder.encode(b'bc'))
    self.assertEqual(b'\xff\x7f' + b'z' * 16383, coder.encode(b'z' * 16383))
    # Test unnested
    self.check_coder(coder, b'', b'a', b'bc', b'def')
    # Test nested
    self.check_coder(
        coders.TupleCoder((coder, coder)), (b'', b'a'), (b'bc', b'def'))

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

  def test_state_backed_iterable_coder(self):
    # pylint: disable=global-variable-undefined
    # required for pickling by reference
    global state
    state = {}

    def iterable_state_write(values, element_coder_impl):
      token = b'state_token_%d' % len(state)
      state[token] = [element_coder_impl.encode(e) for e in values]
      return token

    def iterable_state_read(token, element_coder_impl):
      return [element_coder_impl.decode(s) for s in state[token]]

    coder = coders.StateBackedIterableCoder(
        coders.VarIntCoder(),
        read_state=iterable_state_read,
        write_state=iterable_state_write,
        write_state_threshold=1)
    # Note: do not use check_coder
    # see https://github.com/cloudpipe/cloudpickle/issues/452
    self._observe(coder)
    self.assertEqual([1, 2, 3], coder.decode(coder.encode([1, 2, 3])))
    # Ensure that state was actually used.
    self.assertNotEqual(state, {})
    tupleCoder = coders.TupleCoder((coder, coder))
    self._observe(tupleCoder)
    self.assertEqual(([1], [2, 3]),
                     tupleCoder.decode(tupleCoder.encode(([1], [2, 3]))))

  def test_nullable_coder(self):
    self.check_coder(coders.NullableCoder(coders.VarIntCoder()), None, 2 * 64)

  def test_map_coder(self):
    values = [
        {1: "one", 300: "three hundred"}, # force yapf to be nice
        {},
        {i: str(i) for i in range(5000)}
    ]
    map_coder = coders.MapCoder(coders.VarIntCoder(), coders.StrUtf8Coder())
    self.check_coder(map_coder, *values)
    self.check_coder(map_coder.as_deterministic_coder("label"), *values)

  def test_sharded_key_coder(self):
    key_and_coders = [(b'', b'\x00', coders.BytesCoder()),
                      (b'key', b'\x03key', coders.BytesCoder()),
                      ('key', b'\03\x6b\x65\x79', coders.StrUtf8Coder()),
                      (('k', 1),
                       b'\x01\x6b\x01',
                       coders.TupleCoder(
                           (coders.StrUtf8Coder(), coders.VarIntCoder())))]

    for key, bytes_repr, key_coder in key_and_coders:
      coder = coders.ShardedKeyCoder(key_coder)
      # Verify cloud object representation
      self.assertEqual({
          '@type': 'kind:sharded_key',
          'component_encodings': [key_coder.as_cloud_object()]
      },
                       coder.as_cloud_object())

      # Test str repr
      self.assertEqual('%s' % coder, 'ShardedKeyCoder[%s]' % key_coder)

      self.assertEqual(b'\x00' + bytes_repr, coder.encode(ShardedKey(key, b'')))
      self.assertEqual(
          b'\x03123' + bytes_repr, coder.encode(ShardedKey(key, b'123')))

      # Test unnested
      self.check_coder(coder, ShardedKey(key, b''))
      self.check_coder(coder, ShardedKey(key, b'123'))

      # Test type hints
      self.assertTrue(
          isinstance(
              coder.to_type_hint(), sharded_key_type.ShardedKeyTypeConstraint))
      key_type = coder.to_type_hint().key_type
      if isinstance(key_type, typehints.TupleConstraint):
        self.assertEqual(key_type.tuple_types, (type(key[0]), type(key[1])))
      else:
        self.assertEqual(key_type, type(key))
      self.assertEqual(
          coders.ShardedKeyCoder.from_type_hint(
              coder.to_type_hint(), typecoders.CoderRegistry()),
          coder)

      for other_key, _, other_key_coder in key_and_coders:
        other_coder = coders.ShardedKeyCoder(other_key_coder)
        # Test nested
        self.check_coder(
            coders.TupleCoder((coder, other_coder)),
            (ShardedKey(key, b''), ShardedKey(other_key, b'')))
        self.check_coder(
            coders.TupleCoder((coder, other_coder)),
            (ShardedKey(key, b'123'), ShardedKey(other_key, b'')))

  def test_timestamp_prefixing_window_coder(self):
    self.check_coder(
        coders.TimestampPrefixingWindowCoder(coders.IntervalWindowCoder()),
        *[
            window.IntervalWindow(x, y) for x in [-2**52, 0, 2**52]
            for y in range(-100, 100)
        ])
    self.check_coder(
        coders.TupleCoder((
            coders.TimestampPrefixingWindowCoder(
                coders.IntervalWindowCoder()), )),
        (window.IntervalWindow(0, 10), ))

  def test_decimal_coder(self):
    test_coder = coders.DecimalCoder()

    test_values = [
        Decimal("-10.5"),
        Decimal("-1"),
        Decimal(),
        Decimal("1"),
        Decimal("13.258"),
    ]

    test_encodings = ("AZc", "AP8", "AAA", "AAE", "AzPK")

    self.check_coder(test_coder, *test_values)

    for idx, value in enumerate(test_values):
      self.assertEqual(
          test_encodings[idx],
          base64.b64encode(test_coder.encode(value)).decode().rstrip("="))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
