"""Module for testing code path generation with classes."""


class AddLocalVariable:
  def my_method():
    a = 1
    b = lambda: 2
    new_local_variable = 3
    return b


class RemoveLocalVariable:
  def my_method():
    b = lambda: 2
    return b


class AddLambdaVariable:
  def my_method():
    a = 1
    b = lambda: 2
    c = lambda: 3
    return b


class RemoveLambdaVariable:
  def my_method():
    b = lambda: 2
    return b


class ClassWithNestedFunction:
  def my_method():
    def nested_function():
      c = 3
      return c
    a = 1
    b = lambda: 2
    return b


class ClassWithNestedFunction2:
  def my_method():
    a = 1
    b = lambda: 2
    def nested_function():
      c = 3
      return c
    return b


class ClassWithTwoMethods:
  def another_method():
    a = 1
    b = lambda: 2
    return b

  def my_method():
    a = 1
    b = lambda: 2
    return b


class RemoveMethod:
  def my_method():
    a = 1

    b = lambda: 2
    return b
