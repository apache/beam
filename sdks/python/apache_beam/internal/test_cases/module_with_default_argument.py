"""Module for testing code path generation with functions."""


def function_with_lambda_default_argument(fn=lambda x: 1):
  return fn
