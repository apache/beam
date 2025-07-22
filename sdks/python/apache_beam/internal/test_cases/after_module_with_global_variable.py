"""Module for testing code path generation with a global variable."""

GLOBAL_VARIABLE = lambda: 3


def my_function():
  a = 1
  b = lambda: 2
  return b
