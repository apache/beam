"""Module for testing code path generation with classes."""


class AddLocalVariable:
  def my_method():
    a = 1
    b = lambda: 2
    return b


class RemoveLocalVariable:
  def my_method():
    a = 1
    b = lambda: 2
    return b


class AddLambdaVariable:
  def my_method():
    a = 1
    b = lambda: 2
    return b


class RemoveLambdaVariable:
  def my_method():
    a = lambda: 1
    b = lambda: 2
    return b


class ClassWithNestedFunction:
  def my_method():
    a = 1
    b = lambda: 2
    return b


class ClassWithNestedFunction2:
  def my_method():
    a = 1
    b = lambda: 2
    return b


class ClassWithTwoMethods:
  def my_method():
    a = 1
    b = lambda: 2
    return b


class RemoveMethod:
  def another_method():
    a = 1
    b = lambda: 2
    return b

  def my_method():
    a = 1
    b = lambda: 2
    return b
