"""Module for testing code path generation with functions."""


def my_function():
  def nested_function():
    c = 3
    return c
  a = 1
  b = lambda: 2
  return b
