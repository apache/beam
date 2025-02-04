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

import dataclasses
import logging
from parameterized import parameterized
from typing import List
from typing import Optional
import unittest

from apache_beam.ml.anomaly.specifiable import Spec
from apache_beam.ml.anomaly.specifiable import Specifiable
from apache_beam.ml.anomaly.specifiable import specifiable
from apache_beam.ml.anomaly.specifiable import KNOWN_SPECIFIABLE


class TestSpecifiable(unittest.TestCase):
  def test_register_specifiable(self):
    class MyClass():
      pass

    # class is not decorated/registered
    self.assertRaises(AttributeError, lambda: MyClass().to_spec())  # type: ignore

    self.assertNotIn("MyKey", KNOWN_SPECIFIABLE)

    MyClass = specifiable(key="MyKey")(MyClass)

    self.assertIn("MyKey", KNOWN_SPECIFIABLE)
    self.assertEqual(KNOWN_SPECIFIABLE["MyKey"], MyClass)

    # By default, an error is raised if the key is duplicated
    self.assertRaises(ValueError, specifiable(key="MyKey"), MyClass)

    # But it is ok if a different key is used for the same class
    _ = specifiable(key="MyOtherKey")(MyClass)
    self.assertIn("MyOtherKey", KNOWN_SPECIFIABLE)

    # Or, use a parameter to suppress the error
    specifiable(key="MyKey", error_if_exists=False)(MyClass)

  def test_decorator_key(self):
    # use decorator without parameter
    @specifiable
    class MySecondClass():
      pass

    self.assertIn("MySecondClass", KNOWN_SPECIFIABLE)
    self.assertEqual(KNOWN_SPECIFIABLE["MySecondClass"], MySecondClass)
    self.assertTrue(isinstance(MySecondClass(), Specifiable))

    # use decorator with key parameter
    @specifiable(key="MyThirdKey")
    class MyThirdClass():
      pass

    self.assertIn("MyThirdKey", KNOWN_SPECIFIABLE)
    self.assertEqual(KNOWN_SPECIFIABLE["MyThirdKey"], MyThirdClass)

  def test_init_params_in_specifiable(self):
    @specifiable
    class MyClassWithInitParams():
      def __init__(self, arg_1, arg_2=2, arg_3="3", **kwargs):
        pass

    a = MyClassWithInitParams(10, arg_3="30", arg_4=40)
    assert isinstance(a, Specifiable)
    self.assertEqual(a._init_params, {'arg_1': 10, 'arg_3': '30', 'arg_4': 40})

    # inheritance of specifiable
    @specifiable
    class MyDerivedClassWithInitParams(MyClassWithInitParams):
      def __init__(self, new_arg_1, new_arg_2=200, new_arg_3="300", **kwargs):
        super().__init__(**kwargs)

    b = MyDerivedClassWithInitParams(
        1000, arg_1=11, arg_2=20, new_arg_2=2000, arg_4=4000)
    assert isinstance(b, Specifiable)
    self.assertEqual(
        b._init_params,
        {
            'new_arg_1': 1000,
            'arg_1': 11,
            'arg_2': 20,
            'new_arg_2': 2000,
            'arg_4': 4000
        })

    # composite of specifiable
    @specifiable
    class MyCompositeClassWithInitParams():
      def __init__(self, my_class: Optional[MyClassWithInitParams] = None):
        pass

    c = MyCompositeClassWithInitParams(a)
    assert isinstance(c, Specifiable)
    self.assertEqual(c._init_params, {'my_class': a})

  def test_from_and_to_specifiable(self):
    @specifiable(on_demand_init=False, just_in_time_init=False)
    @dataclasses.dataclass
    class Product():
      name: str
      price: float

    @specifiable(
        key="shopping_entry", on_demand_init=False, just_in_time_init=False)
    class Entry():
      def __init__(self, product: Product, quantity: int = 1):
        self._product = product
        self._quantity = quantity

      def __eq__(self, value: 'Entry') -> bool:
        return self._product == value._product and \
          self._quantity == value._quantity

    @specifiable(
        key="shopping_cart", on_demand_init=False, just_in_time_init=False)
    @dataclasses.dataclass
    class ShoppingCart():
      user_id: str
      entries: List[Entry]

    orange = Product("orange", 1.0)

    expected_orange_spec = Spec(
        "Product", config={
            'name': 'orange', 'price': 1.0
        })
    assert isinstance(orange, Specifiable)
    self.assertEqual(orange.to_spec(), expected_orange_spec)
    self.assertEqual(Specifiable.from_spec(expected_orange_spec), orange)

    entry_1 = Entry(product=orange)

    expected_entry_spec_1 = Spec(
        "shopping_entry", config={
            'product': expected_orange_spec,
        })

    assert isinstance(entry_1, Specifiable)
    self.assertEqual(entry_1.to_spec(), expected_entry_spec_1)
    self.assertEqual(Specifiable.from_spec(expected_entry_spec_1), entry_1)

    banana = Product("banana", 0.5)
    expected_banana_spec = Spec(
        "Product", config={
            'name': 'banana', 'price': 0.5
        })
    entry_2 = Entry(product=banana, quantity=5)
    expected_entry_spec_2 = Spec(
        "shopping_entry",
        config={
            'product': expected_banana_spec, 'quantity': 5
        })

    shopping_cart = ShoppingCart(user_id="test", entries=[entry_1, entry_2])
    expected_shopping_cart_spec = Spec(
        "shopping_cart",
        config={
            "user_id": "test",
            "entries": [expected_entry_spec_1, expected_entry_spec_2]
        })

    assert isinstance(shopping_cart, Specifiable)
    self.assertEqual(shopping_cart.to_spec(), expected_shopping_cart_spec)
    self.assertEqual(
        Specifiable.from_spec(expected_shopping_cart_spec), shopping_cart)

  def test_on_demand_init(self):
    @specifiable(on_demand_init=True, just_in_time_init=False)
    class FooOnDemand():
      counter = 0

      def __init__(self, arg):
        self.my_arg = arg * 10
        FooOnDemand.counter += 1

    foo = FooOnDemand(123)
    self.assertEqual(FooOnDemand.counter, 0)
    self.assertIn("_init_params", foo.__dict__)
    self.assertEqual(foo.__dict__["_init_params"], {"arg": 123})

    self.assertNotIn("my_arg", foo.__dict__)
    self.assertRaises(AttributeError, getattr, foo, "my_arg")
    self.assertRaises(AttributeError, lambda: foo.my_arg)
    self.assertRaises(AttributeError, getattr, foo, "unknown_arg")
    self.assertRaises(AttributeError, lambda: foo.unknown_arg)  # type: ignore
    self.assertEqual(FooOnDemand.counter, 0)

    foo_2 = FooOnDemand(456, _run_init=True)  # type: ignore
    self.assertEqual(FooOnDemand.counter, 1)
    self.assertIn("_init_params", foo_2.__dict__)
    self.assertEqual(foo_2.__dict__["_init_params"], {"arg": 456})

    self.assertIn("my_arg", foo_2.__dict__)
    self.assertEqual(foo_2.my_arg, 4560)
    self.assertEqual(FooOnDemand.counter, 1)

  def test_just_in_time_init(self):
    @specifiable(on_demand_init=False, just_in_time_init=True)
    class FooJustInTime():
      counter = 0

      def __init__(self, arg):
        self.my_arg = arg * 10
        FooJustInTime.counter += 1

    foo = FooJustInTime(321)
    self.assertEqual(FooJustInTime.counter, 0)
    self.assertIn("_init_params", foo.__dict__)
    self.assertEqual(foo.__dict__["_init_params"], {"arg": 321})

    self.assertNotIn("my_arg", foo.__dict__)  # __init__ hasn't been called
    self.assertEqual(FooJustInTime.counter, 0)

    # __init__ is called when trying to accessing an attribute
    self.assertEqual(foo.my_arg, 3210)
    self.assertEqual(FooJustInTime.counter, 1)
    self.assertRaises(AttributeError, lambda: foo.unknown_arg)  # type: ignore
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
    self.assertIn("_init_params", foo.__dict__)
    self.assertEqual(foo.__dict__["_init_params"], {"arg": 987})
    self.assertNotIn("my_arg", foo.__dict__)

    self.assertEqual(FooOnDemandAndJustInTime.counter, 0)
    # __init__ is called
    self.assertEqual(foo.my_arg, 9870)
    self.assertEqual(FooOnDemandAndJustInTime.counter, 1)

    # __init__ is called
    foo_2 = FooOnDemandAndJustInTime(789, _run_init=True)  # type: ignore
    self.assertEqual(FooOnDemandAndJustInTime.counter, 2)
    self.assertIn("_init_params", foo_2.__dict__)
    self.assertEqual(foo_2.__dict__["_init_params"], {"arg": 789})

    self.assertEqual(FooOnDemandAndJustInTime.counter, 2)
    # __init__ is NOT called
    self.assertEqual(foo_2.my_arg, 7890)
    self.assertEqual(FooOnDemandAndJustInTime.counter, 2)

  @specifiable(on_demand_init=True, just_in_time_init=True)
  class FooForPickle():
    counter = 0

    def __init__(self, arg):
      self.my_arg = arg * 10
      type(self).counter += 1

  def test_on_pickle(self):
    FooForPickle = TestSpecifiable.FooForPickle

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

    import cloudpickle
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
    self.child_inst_var += 1  # type: ignore
    super().__init__(c)
    Child_2.counter += 1


@specifiable
class Child_Error_2(Parent):
  counter = 0
  child_class_var = 2001

  def __init__(self, c):
    self.parent_inst_var += 1  # type: ignore
    Child_2.counter += 1


class TestNestedSpecifiable(unittest.TestCase):
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
    self.assertRaises(AttributeError, lambda: child_1.child_inst_var)  # type: ignore
    self.assertEqual(Parent.counter, 0)
    self.assertEqual(Child_1.counter, 0)

    child_2 = Child_Error_2(5)
    self.assertEqual(child_2.child_class_var, 2001)

    # error during child initialization
    self.assertRaises(AttributeError, lambda: child_2.parent_inst_var)  # type: ignore
    self.assertEqual(Parent.counter, 0)
    self.assertEqual(Child_2.counter, 0)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
