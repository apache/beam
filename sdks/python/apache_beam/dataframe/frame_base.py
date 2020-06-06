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

from __future__ import absolute_import

import inspect
from typing import Dict

import pandas as pd

from apache_beam.dataframe import expressions


class DeferredFrame(object):

  _pandas_type_map = {}  # type: Dict[type, type]

  def __init__(self, expr):
    self._expr = expr

  @classmethod
  def _register_for(cls, pandas_type):
    def wrapper(deferred_type):
      cls._pandas_type_map[pandas_type] = deferred_type
      return deferred_type

    return wrapper

  @classmethod
  def wrap(cls, expr):
    return cls._pandas_type_map[type(expr.proxy())](expr)

  def _elementwise(self, func, name=None, other_args=(), inplace=False):
    return _elementwise_function(func, name, inplace=inplace)(self, *other_args)

  @property
  def dtypes(self):
    return self._expr.proxy().dtypes


def name_and_func(method):
  if isinstance(method, str):
    return method, lambda df, *args, **kwargs: getattr(df, method)(*args, **
                                                                   kwargs)
  else:
    return method.__name__, method


def _elementwise_method(func, name=None, restrictions=None, inplace=False):
  if name is None:
    name, func = name_and_func(func)
  if restrictions is None:
    restrictions = {}
  return _elementwise_function(func, name, restrictions)


def _elementwise_function(func, name=None, restrictions=None, inplace=False):
  if name is None:
    name = func.__name__
  if restrictions is None:
    restrictions = {}

  def wrapper(*args, **kwargs):
    for key, values in restrictions.items():
      if key in kwargs:
        value = kwargs[key]
      else:
        try:
          # pylint: disable=deprecated-method
          ix = inspect.getargspec(func).args.index(key)
        except ValueError:
          # TODO: fix for delegation?
          continue
        if len(args) <= ix:
          continue
        value = args[ix]
      if not isinstance(values, list):
        values = [values]
      if value not in values:
        raise NotImplementedError(
            '%s=%s not supported for %s' % (key, value, name))
    deferred_arg_indices = []
    deferred_arg_exprs = []
    constant_args = [None] * len(args)
    for ix, arg in enumerate(args):
      if isinstance(arg, DeferredFrame):
        deferred_arg_indices.append(ix)
        deferred_arg_exprs.append(arg._expr)
      elif isinstance(arg, pd.core.generic.NDFrame):
        deferred_arg_indices.append(ix)
        deferred_arg_exprs.append(expressions.ConstantExpression(arg, arg[0:0]))
      else:
        constant_args[ix] = arg

    if inplace:
      actual_func = copy_and_mutate(func)
    else:
      actual_func = func

    def apply(*actual_args):
      full_args = list(constant_args)
      for ix, arg in zip(deferred_arg_indices, actual_args):
        full_args[ix] = arg
      return actual_func(*full_args, **kwargs)

    result_expr = expressions.elementwise_expression(
        name, apply, deferred_arg_exprs)
    if inplace:
      args[0]._expr = result_expr
      return args[0]

    else:
      return DeferredFrame.wrap(result_expr)

  return wrapper


def copy_and_mutate(func):
  def wrapper(self, *args, **kwargs):
    copy = self.copy()
    func(copy, *args, **kwargs)
    return copy

  return wrapper


class WontImplementError(NotImplementedError):
  """An subclass of NotImplementedError to raise indicating that implementing
  the given method is infeasible.

  Raising this error will also prevent this doctests from being validated
  when run with the beam dataframe validation doctest runner.
  """
  pass
