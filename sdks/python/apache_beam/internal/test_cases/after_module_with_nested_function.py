"""Module for testing code path generation with functions."""


def my_function():
  a = 1
  b = lambda: 2
  def nested_function():
    c = 3
    return c
  return b
  
