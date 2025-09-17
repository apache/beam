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

import copy
import dataclasses
import logging
import os
import unittest
from typing import Optional

import pytest
from parameterized import parameterized

from apache_beam.internal.cloudpickle import cloudpickle
from apache_beam.ml.anomaly.specifiable import _FALLBACK_SUBSPACE
from apache_beam.ml.anomaly.specifiable import _KNOWN_SPECIFIABLE
from apache_beam.ml.anomaly.specifiable import Spec
from apache_beam.ml.anomaly.specifiable import Specifiable
from apache_beam.ml.anomaly.specifiable import specifiable


class TestSpecifiable(unittest.TestCase):
  def setUp(self) -> None:
    self.saved_specifiable = copy.deepcopy(_KNOWN_SPECIFIABLE)

  def tearDown(self) -> None:
    _KNOWN_SPECIFIABLE.clear()
    _KNOWN_SPECIFIABLE.update(self.saved_specifiable)

  def test_decorator_in_function_form(self):
    class A():
      pass

    class B():
      pass

    # class is not decorated and thus not registered
    self.assertNotIn("A", _KNOWN_SPECIFIABLE[_FALLBACK_SUBSPACE])

    # apply the decorator function to an existing class
    A = specifiable(A)
    self.assertEqual(A.spec_type(), "A")
    self.assertTrue(isinstance(A(), Specifiable))
    self.assertIn("A", _KNOWN_SPECIFIABLE[_FALLBACK_SUBSPACE])
    self.assertEqual(_KNOWN_SPECIFIABLE[_FALLBACK_SUBSPACE]["A"], A)

    # Re-registering spec_type with the same class is allowed
    A = specifiable(A)

    # Raise an error when re-registering spec_type with a different class
    self.assertRaises(ValueError, specifiable(spec_type='A'), B)

    # Applying the decorator function to an existing class with a different
    # spec_type will have no effect.
    A = specifiable(spec_type="A_DUP")(A)
    self.assertEqual(A.spec_type(), "A")

  def test_decorator_in_syntactic_sugar_form(self):
    # call decorator without parameters
    @specifiable
    class B():
      pass

    self.assertTrue(isinstance(B(), Specifiable))
    self.assertIn("B", _KNOWN_SPECIFIABLE[_FALLBACK_SUBSPACE])
    self.assertEqual(_KNOWN_SPECIFIABLE[_FALLBACK_SUBSPACE]["B"], B)

    # call decorator with parameters
    @specifiable(spec_type="C_TYPE")
    class C():
      pass

    self.assertTrue(isinstance(C(), Specifiable))
    self.assertIn("C_TYPE", _KNOWN_SPECIFIABLE[_FALLBACK_SUBSPACE])
    self.assertEqual(_KNOWN_SPECIFIABLE[_FALLBACK_SUBSPACE]["C_TYPE"], C)

  def test_init_params_in_specifiable(self):
    @specifiable
    class ParentWithInitParams():
      def __init__(self, arg_1, arg_2=2, arg_3="3", **kwargs):
        pass

    parent = ParentWithInitParams(10, arg_3="30", arg_4=40)
    assert isinstance(parent, Specifiable)
    self.assertEqual(
        parent.init_kwargs, {
            'arg_1': 10, 'arg_3': '30', 'arg_4': 40
        })

    # inheritance of a Specifiable subclass
    @specifiable
    class ChildWithInitParams(ParentWithInitParams):
      def __init__(self, new_arg_1, new_arg_2=200, new_arg_3="300", **kwargs):
        super().__init__(**kwargs)

    child = ChildWithInitParams(
        1000, arg_1=11, arg_2=20, new_arg_2=2000, arg_4=4000)
    assert isinstance(child, Specifiable)
    self.assertEqual(
        child.init_kwargs,
        {
            'new_arg_1': 1000,
            'arg_1': 11,
            'arg_2': 20,
            'new_arg_2': 2000,
            'arg_4': 4000
        })

    # composite of Specifiable subclasses
    @specifiable
    class CompositeWithInitParams():
      def __init__(
          self,
          my_parent: Optional[ParentWithInitParams] = None,
          my_child: Optional[ChildWithInitParams] = None):
        pass

    composite = CompositeWithInitParams(parent, child)
    assert isinstance(composite, Specifiable)
    self.assertEqual(
        composite.init_kwargs, {
            'my_parent': parent, 'my_child': child
        })

  def test_from_spec_on_unknown_spec_type(self):
    self.assertRaises(ValueError, Specifiable.from_spec, Spec(type="unknown"))

  # To test from_spec and to_spec with/without just_in_time_init.
  @parameterized.expand([(False, False), (True, False), (False, True),
                         (True, True)])
  def test_from_spec_and_to_spec(self, on_demand_init, just_in_time_init):
    @specifiable(
        on_demand_init=on_demand_init, just_in_time_init=just_in_time_init)
    @dataclasses.dataclass
    class Product():
      name: str
      price: float

    @specifiable(
        on_demand_init=on_demand_init, just_in_time_init=just_in_time_init)
    class Entry():
      def __init__(self, product: Product, quantity: int = 1):
        self._product = product
        self._quantity = quantity

      def __eq__(self, value) -> bool:
        return isinstance(value, Entry) and \
          self._product == value._product and \
          self._quantity == value._quantity

    @specifiable(
        on_demand_init=on_demand_init, just_in_time_init=just_in_time_init)
    @dataclasses.dataclass
    class ShoppingCart():
      user_id: str
      entries: list[Entry]

    orange = Product("orange", 1.0)

    expected_orange_spec = Spec(
        "Product", config={
            'name': 'orange', 'price': 1.0
        })
    assert isinstance(orange, Specifiable)
    self.assertEqual(orange.to_spec(), expected_orange_spec)

    entry_1 = Entry(product=orange)

    expected_entry_spec_1 = Spec(
        "Entry", config={
            'product': expected_orange_spec,
        })

    assert isinstance(entry_1, Specifiable)
    self.assertEqual(entry_1.to_spec(), expected_entry_spec_1)

    banana = Product("banana", 0.5)
    expected_banana_spec = Spec(
        "Product", config={
            'name': 'banana', 'price': 0.5
        })
    entry_2 = Entry(product=banana, quantity=5)
    expected_entry_spec_2 = Spec(
        "Entry", config={
            'product': expected_banana_spec, 'quantity': 5
        })

    shopping_cart = ShoppingCart(user_id="test", entries=[entry_1, entry_2])
    expected_shopping_cart_spec = Spec(
        "ShoppingCart",
        config={
            "user_id": "test",
            "entries": [expected_entry_spec_1, expected_entry_spec_2]
        })

    assert isinstance(shopping_cart, Specifiable)
    self.assertEqual(shopping_cart.to_spec(), expected_shopping_cart_spec)
    if on_demand_init and not just_in_time_init:
      orange.run_original_init()
      banana.run_original_init()
      entry_1.run_original_init()
      entry_2.run_original_init()
      shopping_cart.run_original_init()

    self.assertEqual(Specifiable.from_spec(expected_orange_spec), orange)
    self.assertEqual(Specifiable.from_spec(expected_entry_spec_1), entry_1)
    self.assertEqual(
        Specifiable.from_spec(expected_shopping_cart_spec), shopping_cart)


class TestInitCallCount(unittest.TestCase):
  def setUp(self) -> None:
    self.saved_specifiable = copy.deepcopy(_KNOWN_SPECIFIABLE)

  def tearDown(self) -> None:
    _KNOWN_SPECIFIABLE.clear()
    _KNOWN_SPECIFIABLE.update(self.saved_specifiable)

  def test_on_demand_init(self):
    @specifiable(on_demand_init=True, just_in_time_init=False)
    class FooOnDemand():
      counter = 0

      def __init__(self, arg):
        self.my_arg = arg * 10
        FooOnDemand.counter += 1  # increment it when __init__ is called

    foo = FooOnDemand(123)
    self.assertEqual(FooOnDemand.counter, 0)
    self.assertIn("init_kwargs", foo.__dict__)
    self.assertEqual(foo.__dict__["init_kwargs"], {"arg": 123})

    self.assertNotIn("my_arg", foo.__dict__)
    self.assertRaises(AttributeError, getattr, foo, "my_arg")
    self.assertRaises(AttributeError, lambda: foo.my_arg)
    self.assertRaises(AttributeError, getattr, foo, "unknown_arg")
    self.assertRaises(AttributeError, lambda: foo.unknown_arg)
    self.assertEqual(FooOnDemand.counter, 0)

    # __init__ is called when _run_init=True is used
    foo_2 = FooOnDemand(456, _run_init=True)
    self.assertEqual(FooOnDemand.counter, 1)
    self.assertIn("init_kwargs", foo_2.__dict__)
    self.assertEqual(foo_2.__dict__["init_kwargs"], {"arg": 456})

    self.assertIn("my_arg", foo_2.__dict__)
    self.assertEqual(foo_2.my_arg, 4560)
    self.assertEqual(FooOnDemand.counter, 1)

  def test_just_in_time_init(self):
    @specifiable(on_demand_init=False, just_in_time_init=True)
    class FooJustInTime():
      counter = 0

      def __init__(self, arg):
        self.my_arg = arg * 10
        FooJustInTime.counter += 1  # increment it when __init__ is called

    foo = FooJustInTime(321)
    self.assertEqual(FooJustInTime.counter, 0)
    self.assertIn("init_kwargs", foo.__dict__)
    self.assertEqual(foo.__dict__["init_kwargs"], {"arg": 321})

    # __init__ hasn't been called yet
    self.assertNotIn("my_arg", foo.__dict__)
    self.assertEqual(FooJustInTime.counter, 0)

    # __init__ is called when trying to access a class attribute
    self.assertEqual(foo.my_arg, 3210)
    self.assertEqual(FooJustInTime.counter, 1)
    self.assertRaises(AttributeError, lambda: foo.unknown_arg)
    self.assertEqual(FooJustInTime.counter, 1)

  def test_on_demand_and_just_in_time_init(self):
    @specifiable(on_demand_init=True, just_in_time_init=True)
    class FooOnDemandAndJustInTime():
      counter = 0

      def __init__(self, arg):
        self.my_arg = arg * 10
        FooOnDemandAndJustInTime.counter += 1

    foo = FooOnDemandAndJustInTime(987)
    self.assertEqual(FooOnDemandAndJustInTime.counter, 0)
    self.assertIn("init_kwargs", foo.__dict__)
    self.assertEqual(foo.__dict__["init_kwargs"], {"arg": 987})
    self.assertNotIn("my_arg", foo.__dict__)

    self.assertEqual(FooOnDemandAndJustInTime.counter, 0)
    # __init__ is called when trying to access a class attribute
    self.assertEqual(foo.my_arg, 9870)
    self.assertEqual(FooOnDemandAndJustInTime.counter, 1)

    # __init__ is called when _run_init=True is used
    foo_2 = FooOnDemandAndJustInTime(789, _run_init=True)
    self.assertEqual(FooOnDemandAndJustInTime.counter, 2)
    self.assertIn("init_kwargs", foo_2.__dict__)
    self.assertEqual(foo_2.__dict__["init_kwargs"], {"arg": 789})

    self.assertEqual(FooOnDemandAndJustInTime.counter, 2)
    # __init__ is NOT called after it is initialized
    self.assertEqual(foo_2.my_arg, 7890)
    self.assertEqual(FooOnDemandAndJustInTime.counter, 2)

  @specifiable(on_demand_init=True, just_in_time_init=True)
  class FooForPickle():
    counter = 0

    def __init__(self, arg):
      self.my_arg = arg * 10
      type(self).counter += 1

  @pytest.mark.uses_dill
  def test_on_dill_pickle(self):
    pytest.importorskip("dill")

    FooForPickle = TestInitCallCount.FooForPickle

    import dill
    FooForPickle.counter = 0
    foo = FooForPickle(456)
    self.assertEqual(FooForPickle.counter, 0)
    new_foo = dill.loads(dill.dumps(foo))
    self.assertEqual(FooForPickle.counter, 0)
    self.assertEqual(new_foo.__dict__, foo.__dict__)
    self.assertEqual(foo.my_arg, 4560)
    self.assertEqual(FooForPickle.counter, 1)
    new_foo_2 = dill.loads(dill.dumps(foo))
    self.assertEqual(FooForPickle.counter, 1)
    self.assertEqual(new_foo_2.__dict__, foo.__dict__)

  def test_on_pickle(self):
    FooForPickle = TestInitCallCount.FooForPickle

    # Note that pickle does not support classes/functions nested in a function.
    import pickle
    FooForPickle.counter = 0
    foo = FooForPickle(456)
    self.assertEqual(FooForPickle.counter, 0)
    new_foo = pickle.loads(pickle.dumps(foo))
    self.assertEqual(FooForPickle.counter, 0)
    self.assertEqual(new_foo.__dict__, foo.__dict__)
    self.assertEqual(foo.my_arg, 4560)
    self.assertEqual(FooForPickle.counter, 1)
    new_foo_2 = pickle.loads(pickle.dumps(foo))
    self.assertEqual(FooForPickle.counter, 1)
    self.assertEqual(new_foo_2.__dict__, foo.__dict__)

    FooForPickle.counter = 0
    foo = FooForPickle(456)
    self.assertEqual(FooForPickle.counter, 0)
    new_foo = cloudpickle.loads(cloudpickle.dumps(foo))
    self.assertEqual(FooForPickle.counter, 0)
    self.assertEqual(new_foo.__dict__, foo.__dict__)
    self.assertEqual(foo.my_arg, 4560)
    self.assertEqual(FooForPickle.counter, 1)
    new_foo_2 = cloudpickle.loads(cloudpickle.dumps(foo))
    self.assertEqual(FooForPickle.counter, 1)
    self.assertEqual(new_foo_2.__dict__, foo.__dict__)


@specifiable
class Parent():
  counter = 0
  parent_class_var = 1000

  def __init__(self, p):
    self.parent_inst_var = p * 10
    Parent.counter += 1


@specifiable
class Child_1(Parent):
  counter = 0
  child_class_var = 2001

  def __init__(self, c):
    super().__init__(c)
    self.child_inst_var = c + 1
    Child_1.counter += 1


@specifiable
class Child_2(Parent):
  counter = 0
  child_class_var = 2001

  def __init__(self, c):
    self.child_inst_var = c + 1
    super().__init__(c)
    Child_2.counter += 1


@specifiable
class Child_Error_1(Parent):
  counter = 0
  child_class_var = 2001

  def __init__(self, c):
    # read an instance var in child that doesn't exist
    self.child_inst_var += 1
    super().__init__(c)
    Child_2.counter += 1


@specifiable
class Child_Error_2(Parent):
  counter = 0
  child_class_var = 2001

  def __init__(self, c):
    # read an instance var in parent without calling parent's __init__.
    self.parent_inst_var += 1
    Child_2.counter += 1


class TestNestedSpecifiable(unittest.TestCase):
  def setUp(self) -> None:
    self.saved_specifiable = copy.deepcopy(_KNOWN_SPECIFIABLE)

  def tearDown(self) -> None:
    _KNOWN_SPECIFIABLE.clear()
    _KNOWN_SPECIFIABLE.update(self.saved_specifiable)

  @parameterized.expand([[Child_1, 0], [Child_2, 0], [Child_1, 1], [Child_2, 1],
                         [Child_1, 2], [Child_2, 2]])
  def test_nested_specifiable(self, Child, mode):
    Parent.counter = 0
    Child.counter = 0
    child = Child(5)

    self.assertEqual(Parent.counter, 0)
    self.assertEqual(Child.counter, 0)

    # accessing class vars won't trigger __init__
    self.assertEqual(child.parent_class_var, 1000)
    self.assertEqual(child.child_class_var, 2001)
    self.assertEqual(Parent.counter, 0)
    self.assertEqual(Child.counter, 0)

    # accessing instance var will trigger __init__
    if mode == 0:
      self.assertEqual(child.parent_inst_var, 50)
    elif mode == 1:
      self.assertEqual(child.child_inst_var, 6)
    else:
      self.assertRaises(AttributeError, lambda: child.unknown_var)

    self.assertEqual(Parent.counter, 1)
    self.assertEqual(Child.counter, 1)

    # after initialization, it won't trigger __init__ again
    self.assertEqual(child.parent_inst_var, 50)
    self.assertEqual(child.child_inst_var, 6)
    self.assertRaises(AttributeError, lambda: child.unknown_var)

    self.assertEqual(Parent.counter, 1)
    self.assertEqual(Child.counter, 1)

  def test_error_in_child(self):
    Parent.counter = 0
    child_1 = Child_Error_1(5)

    self.assertEqual(child_1.child_class_var, 2001)

    # error during child initialization
    self.assertRaises(AttributeError, lambda: child_1.child_inst_var)
    self.assertEqual(Parent.counter, 0)
    self.assertEqual(Child_1.counter, 0)

    child_2 = Child_Error_2(5)
    self.assertEqual(child_2.child_class_var, 2001)

    # error during child initialization
    self.assertRaises(AttributeError, lambda: child_2.parent_inst_var)
    self.assertEqual(Parent.counter, 0)
    self.assertEqual(Child_2.counter, 0)


def my_normal_func(x, y):
  return x + y


@specifiable
class Wrapper():
  def __init__(self, func=None, cls=None, **kwargs):
    self._func = func
    if cls is not None:
      self._cls = cls(**kwargs)

  def run_func(self, x, y):
    return self._func(x, y)

  def run_func_in_class(self, x, y):
    return self._cls.apply(x, y)


class TestFunctionAsArgument(unittest.TestCase):
  def setUp(self) -> None:
    self.saved_specifiable = copy.deepcopy(_KNOWN_SPECIFIABLE)

  def tearDown(self) -> None:
    _KNOWN_SPECIFIABLE.clear()
    _KNOWN_SPECIFIABLE.update(self.saved_specifiable)

  def test_normal_function(self):
    w = Wrapper(my_normal_func)

    self.assertEqual(w.run_func(1, 2), 3)

    w_spec = w.to_spec()
    self.assertEqual(
        w_spec,
        Spec(
            type='Wrapper',
            config={'func': Spec(type="my_normal_func", config=None)}))

    w_2 = Specifiable.from_spec(w_spec)
    self.assertEqual(w_2.run_func(2, 3), 5)

  def test_lambda_function(self):
    my_lambda_func = lambda x, y: x - y

    w = Wrapper(my_lambda_func)

    self.assertEqual(w.run_func(3, 2), 1)

    w_spec = w.to_spec()
    self.assertEqual(
        w_spec,
        Spec(
            type='Wrapper',
            config={
                'func': Spec(
                    type=
                    f"<lambda at {os.path.basename(__file__)}:{my_lambda_func.__code__.co_firstlineno}>",  # pylint: disable=line-too-long
                    config=None)
            }))

    w_2 = Specifiable.from_spec(w_spec)
    self.assertEqual(w_2.run_func(5, 3), 2)


class TestClassAsArgument(unittest.TestCase):
  def setUp(self) -> None:
    self.saved_specifiable = copy.deepcopy(_KNOWN_SPECIFIABLE)

  def tearDown(self) -> None:
    _KNOWN_SPECIFIABLE.clear()
    _KNOWN_SPECIFIABLE.update(self.saved_specifiable)

  def test_normal_class(self):
    class InnerClass():
      def __init__(self, multiplier):
        self._multiplier = multiplier

      def apply(self, x, y):
        return x * y * self._multiplier

    w = Wrapper(cls=InnerClass, multiplier=10)
    self.assertEqual(w.run_func_in_class(2, 3), 60)

    w_spec = w.to_spec()
    self.assertEqual(
        w_spec,
        Spec(
            type='Wrapper',
            config={
                'cls': Spec(type='InnerClass', config=None), 'multiplier': 10
            }))

    w_2 = Specifiable.from_spec(w_spec)
    self.assertEqual(w_2.run_func_in_class(5, 3), 150)


class TestUncommonUsages(unittest.TestCase):
  def test_double_specifiable(self):
    @specifiable
    @specifiable
    class ZZ():
      def __init__(self, a):
        self.a = a

    assert issubclass(ZZ, Specifiable)
    c = ZZ("b")
    c.run_original_init()
    self.assertEqual(c.a, "b")

  def test_unspecifiable(self):
    class YY():
      def __init__(self, x):
        self.x = x
        assert False

    YY = specifiable(YY)
    assert issubclass(YY, Specifiable)
    y = YY(1)
    # __init__ is called (with assertion error raised) when attribute is first
    # accessed
    self.assertRaises(AssertionError, lambda: y.x)

    # unspecifiable YY
    YY.unspecifiable()
    # __init__ is called immediately
    self.assertRaises(AssertionError, YY, 1)
    self.assertFalse(hasattr(YY, 'run_original_init'))
    self.assertFalse(hasattr(YY, 'spec_type'))
    self.assertFalse(hasattr(YY, 'to_spec'))
    self.assertFalse(hasattr(YY, 'from_spec'))
    self.assertFalse(hasattr(YY, 'unspecifiable'))

    # make YY specifiable again
    YY = specifiable(YY)
    assert issubclass(YY, Specifiable)
    y = YY(1)
    self.assertRaises(AssertionError, lambda: y.x)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
