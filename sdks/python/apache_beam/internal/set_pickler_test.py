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

"""Unit tests for best-effort deterministic pickling of sets."""

import enum
import unittest

from apache_beam.internal.set_pickler import sort_if_possible


class A:
  def __init__(self, x, y):
    self.x = x
    self.y = y


class B:
  pass


class C:
  pass


class SortIfPossibleTest(unittest.TestCase):
  def test_order_by_type(self):
    a = A(1, 2)
    self.assertEqual(
        sort_if_possible([123, "abc", True, a]),
        # A, bool, int, str
        [a, True, 123, "abc"],
    )

  def test_sorts_ints(self):
    self.assertEqual(sort_if_possible({5, 2, 3, 1, 4}), [1, 2, 3, 4, 5])

  def test_sorts_booleans(self):
    self.assertEqual(sort_if_possible({False, True}), [False, True])

  def test_sorts_floats(self):
    self.assertEqual(sort_if_possible({-18.0, 3.14, 2.71}), [-18.0, 2.71, 3.14])

  def test_sorts_strings(self):
    self.assertEqual(
        sort_if_possible({"when", "in", "the", "course", "of"}),
        ["course", "in", "of", "the", "when"],
    )

  def test_sorts_bytes(self):
    self.assertEqual(
        sort_if_possible({b"when", b"in", b"the", b"course", b"of"}),
        [b"course", b"in", b"of", b"the", b"when"],
    )

  def test_sorts_bytearrays(self):
    f = bytearray
    self.assertEqual(
        sort_if_possible(
            [f(b"when"), f(b"in"), f(b"the"), f(b"course"), f(b"of")]),
        [f(b"course"), f(b"in"), f(b"of"), f(b"the"), f(b"when")],
    )

  def test_sort_tuples_by_length(self):
    self.assertEqual(
        sort_if_possible({(1, 1, 1), (1, 1), (1, )}), [(1, ), (1, 1),
                                                       (1, 1, 1)])

  def test_sort_tuples_by_element_values(self):
    self.assertEqual(
        sort_if_possible({(0, 0), (1, 1), (0, 1), (1, 0)}),
        [(0, 0), (0, 1), (1, 0), (1, 1)],
    )

  def test_sort_nested_tuples(self):
    self.assertEqual(
        sort_if_possible({(1, (4, )), (1, (1, )), (1, (3, )), (1, (2, ))}),
        [(1, (1, )), (1, (2, )), (1, (3, )), (1, (4, ))],
    )

  def test_sort_lists_by_length(self):
    self.assertEqual(
        sort_if_possible([[1, 1, 1], [1, 1], [
            1,
        ]]), [[
            1,
        ], [1, 1], [1, 1, 1]])

  def test_sort_lists_by_element_values(self):
    self.assertEqual(
        sort_if_possible([[0, 0], [1, 1], [0, 1], [1, 0]]),
        [[0, 0], [0, 1], [1, 0], [1, 1]],
    )

  def test_sort_frozenset_like_sorted_tuple(self):
    self.assertEqual(
        sort_if_possible(
            {frozenset([1, 2, 3]), frozenset([1]), frozenset([1, 2, 4])}),
        [frozenset([1]), frozenset([1, 2, 3]), frozenset([1, 2, 4])],
    )

  def test_sort_set_like_sorted_tuple(self):
    self.assertEqual(
        sort_if_possible([set([1, 2, 3]), set([1]), set([1, 2, 4])]),
        [set([1]), set([1, 2, 3]), set([1, 2, 4])],
    )

  def test_order_objects_by_class_name(self):
    a = A(1, 2)
    b = B()
    c = C()
    self.assertEqual(sort_if_possible({b, c, a}), [a, b, c])

  def test_order_objects_by_number_of_fields(self):
    o1 = C()
    o2 = C()
    setattr(o2, "f1", 1)
    o3 = C()
    setattr(o3, "f1", 1)
    setattr(o3, "f2", 2)

    self.assertEqual(sort_if_possible({o3, o2, o1}), [o1, o2, o3])

  def test_order_objects_by_field_name(self):
    o1 = C()
    setattr(o1, "aaa", 1)
    o2 = C()
    setattr(o2, "bbb", 1)
    o3 = C()
    setattr(o3, "ccc", 1)

    self.assertEqual(sort_if_possible({o3, o2, o1}), [o1, o2, o3])

  def test_order_objects_by_field_value(self):
    a1_1 = A(1, 1)
    a1_2 = A(1, 2)
    a2_1 = A(2, 1)
    a2_2 = A(2, 2)

    self.assertEqual(
        sort_if_possible({a2_1, a1_1, a2_2, a1_2}), [a1_1, a1_2, a2_1, a2_2])

  def test_cyclic_data(self):
    def create_tuple_with_cycles():
      o = C()
      t = (o, )
      setattr(t[0], "t", t)
      return t

    t1 = create_tuple_with_cycles()
    t2 = create_tuple_with_cycles()
    t3 = create_tuple_with_cycles()

    actual = {hash(t) for t in sort_if_possible({t1, t2, t3})}
    expected = {hash(t1), hash(t2), hash(t3)}
    self.assertEqual(actual, expected)

  def test_order_dict_by_length(self):
    self.assertEqual(
        sort_if_possible([{
            'a': 1, 'b': 2
        }, {
            'a': 1
        }, {
            'a': 1, 'b': 2, 'c': 3
        }]), [{
            'a': 1
        }, {
            'a': 1, 'b': 2
        }, {
            'a': 1, 'b': 2, 'c': 3
        }])

  def test_order_dict_by_key(self):
    self.assertEqual(
        sort_if_possible([{
            'b': 1
        }, {
            'a': 1
        }, {
            'c': 1
        }]), [{
            'a': 1
        }, {
            'b': 1
        }, {
            'c': 1
        }])

  def test_order_dict_by_value(self):
    self.assertEqual(
        sort_if_possible([{
            'a': 2
        }, {
            'a': 1
        }, {
            'a': 3
        }]), [{
            'a': 1
        }, {
            'a': 2
        }, {
            'a': 3
        }])

  def test_dict_keys_do_not_have_lt(self):
    self.assertEqual(
        sort_if_possible([{(1, 1): 1}, {(1, ): 1}, {(1, 1, 1): 1}]),
        [{(1, ): 1}, {(1, 1): 1}, {(1, 1, 1): 1}])

  def test_dict_values_do_not_have_lt(self):
    self.assertEqual(
        sort_if_possible([{
            'a': (1, 1)
        }, {
            'a': (1, )
        }, {
            'a': (1, 1, 1)
        }]), [{
            'a': (1, )
        }, {
            'a': (1, 1)
        }, {
            'a': (1, 1, 1)
        }])

  def test_order_enums_by_name(self):
    class CardinalDirection(enum.Enum):
      NORTH = 1
      EAST = 2
      SOUTH = 3
      WEST = 4

    self.assertEqual(
        sort_if_possible({
            CardinalDirection.NORTH,
            CardinalDirection.SOUTH,
            CardinalDirection.EAST,
            CardinalDirection.WEST,
        }),
        [
            CardinalDirection.EAST,
            CardinalDirection.NORTH,
            CardinalDirection.SOUTH,
            CardinalDirection.WEST,
        ])

  def test_enum_with_many_values(self):
    MyEnum = enum.Enum('MyEnum', ' '.join(f'N{i}' for i in range(10000)))

    self.assertEqual(
        sort_if_possible({
            MyEnum.N789,
            MyEnum.N123,
            MyEnum.N456,
        }), [
            MyEnum.N123,
            MyEnum.N456,
            MyEnum.N789,
        ])


if __name__ == "__main__":
  unittest.main()
