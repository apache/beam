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

import functools
from inspect import getfullargspec
from inspect import unwrap
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import pandas as pd

from apache_beam.dataframe import expressions
from apache_beam.dataframe import partitionings


class DeferredBase(object):

  _pandas_type_map = {}  # type: Dict[Union[type, None], type]

  def __init__(self, expr):
    self._expr = expr

  @classmethod
  def _register_for(cls, pandas_type):
    def wrapper(deferred_type):
      cls._pandas_type_map[pandas_type] = deferred_type
      return deferred_type

    return wrapper

  @classmethod
  def wrap(cls, expr, split_tuples=True):
    proxy_type = type(expr.proxy())
    if proxy_type is tuple and split_tuples:

      def get(ix):
        return expressions.ComputedExpression(
            # yapf: disable
            'get_%d' % ix,
            lambda t: t[ix],
            [expr],
            requires_partition_by=partitionings.Nothing(),
            preserves_partition_by=partitionings.Singleton())

      return tuple([cls.wrap(get(ix)) for ix in range(len(expr.proxy()))])
    elif proxy_type in cls._pandas_type_map:
      wrapper_type = cls._pandas_type_map[proxy_type]
    else:
      if expr.requires_partition_by() != partitionings.Singleton():
        raise ValueError(
            'Scalar expression %s of type %s partitoned by non-singleton %s' %
            (expr, proxy_type, expr.requires_partition_by()))
      wrapper_type = _DeferredScalar
    return wrapper_type(expr)

  def _elementwise(self, func, name=None, other_args=(), inplace=False):
    return _elementwise_function(func, name, inplace=inplace)(self, *other_args)

  def __reduce__(self):
    return UnusableUnpickledDeferredBase, (str(self), )


class UnusableUnpickledDeferredBase(object):
  """Placeholder object used to break the transitive pickling chain in case a
  DeferredBase accidentially gets pickled (e.g. as part of globals).

  Trying to use this object after unpickling is a bug and will result in an
  error.
  """
  def __init__(self, name):
    self._name = name

  def __repr__(self):
    return 'UnusablePickledDeferredBase(%r)' % self.name


class DeferredFrame(DeferredBase):
  @property
  def dtypes(self):
    return self._expr.proxy().dtypes


class _DeferredScalar(DeferredBase):
  def apply(self, func, name=None, args=()):
    if name is None:
      name = func.__name__
    with expressions.allow_non_parallel_operations(
        all(isinstance(arg, _DeferredScalar) for arg in args) or None):
      return DeferredFrame.wrap(
          expressions.ComputedExpression(
              name,
              func, [self._expr] + [arg._expr for arg in args],
              requires_partition_by=partitionings.Singleton()))

  def __repr__(self):
    return f"DeferredScalar[type={type(self._expr.proxy())}]"

  def __bool__(self):
    # TODO(BEAM-11951): Link to documentation
    raise TypeError(
        "Testing the truth value of a deferred scalar is not "
        "allowed. It's not possible to branch on the result of "
        "deferred operations.")


DeferredBase._pandas_type_map[None] = _DeferredScalar


def name_and_func(method):
  if isinstance(method, str):
    return method, lambda df, *args, **kwargs: getattr(df, method)(*args, **
                                                                   kwargs)
  else:
    return method.__name__, method


def _elementwise_method(func, name=None, restrictions=None, inplace=False):
  return _proxy_method(
      func,
      name,
      restrictions,
      inplace,
      requires_partition_by=partitionings.Nothing(),
      preserves_partition_by=partitionings.Singleton())


def _proxy_method(
    func,
    name=None,
    restrictions=None,
    inplace=False,
    requires_partition_by=partitionings.Singleton(),
    preserves_partition_by=partitionings.Nothing()):
  if name is None:
    name, func = name_and_func(func)
  if restrictions is None:
    restrictions = {}
  return _proxy_function(
      func,
      name,
      restrictions,
      inplace,
      requires_partition_by,
      preserves_partition_by)


def _elementwise_function(func, name=None, restrictions=None, inplace=False):
  return _proxy_function(
      func,
      name,
      restrictions,
      inplace,
      requires_partition_by=partitionings.Nothing(),
      preserves_partition_by=partitionings.Singleton())


def _proxy_function(
      func,  # type: Union[Callable, str]
      name=None,  # type: Optional[str]
      restrictions=None,  # type: Optional[Dict[str, Union[Any, List[Any], Callable[[Any], bool]]]]
      inplace=False,  # type: bool
      requires_partition_by=partitionings.Singleton(),  # type: partitionings.Partitioning
      preserves_partition_by=partitionings.Nothing(),  # type: partitionings.Partitioning
):

  if name is None:
    if isinstance(func, str):
      name = func
    else:
      name = func.__name__
  if restrictions is None:
    restrictions = {}

  def wrapper(*args, **kwargs):
    for key, values in restrictions.items():
      if key in kwargs:
        value = kwargs[key]
      else:
        try:
          ix = getfullargspec(func).args.index(key)
        except ValueError:
          # TODO: fix for delegation?
          continue
        if len(args) <= ix:
          continue
        value = args[ix]
      if callable(values):
        check = values
      elif isinstance(values, list):
        check = lambda x, values=values: x in values
      else:
        check = lambda x, value=value: x == value

      if not check(value):
        raise NotImplementedError(
            '%s=%s not supported for %s' % (key, value, name))
    deferred_arg_indices = []
    deferred_arg_exprs = []
    constant_args = [None] * len(args)
    from apache_beam.dataframe.frames import _DeferredIndex
    for ix, arg in enumerate(args):
      if isinstance(arg, DeferredBase):
        deferred_arg_indices.append(ix)
        deferred_arg_exprs.append(arg._expr)
      elif isinstance(arg, _DeferredIndex):
        # TODO(robertwb): Consider letting indices pass through as indices.
        # This would require updating the partitioning code, as indices don't
        # have indices.
        deferred_arg_indices.append(ix)
        deferred_arg_exprs.append(
            expressions.ComputedExpression(
                'index_as_series',
                lambda ix: ix.index.to_series(),  # yapf break
                [arg._frame._expr],
                preserves_partition_by=partitionings.Singleton(),
                requires_partition_by=partitionings.Nothing()))
      elif isinstance(arg, pd.core.generic.NDFrame):
        deferred_arg_indices.append(ix)
        deferred_arg_exprs.append(expressions.ConstantExpression(arg, arg[0:0]))
      else:
        constant_args[ix] = arg

    deferred_kwarg_keys = []
    deferred_kwarg_exprs = []
    constant_kwargs = {key: None for key in kwargs}
    for key, arg in kwargs.items():
      if isinstance(arg, DeferredBase):
        deferred_kwarg_keys.append(key)
        deferred_kwarg_exprs.append(arg._expr)
      elif isinstance(arg, pd.core.generic.NDFrame):
        deferred_kwarg_keys.append(key)
        deferred_kwarg_exprs.append(
            expressions.ConstantExpression(arg, arg[0:0]))
      else:
        constant_kwargs[key] = arg

    deferred_exprs = deferred_arg_exprs + deferred_kwarg_exprs

    if inplace:
      actual_func = copy_and_mutate(func)
    else:
      actual_func = func

    def apply(*actual_args):
      actual_args, actual_kwargs = (actual_args[:len(deferred_arg_exprs)],
                                    actual_args[len(deferred_arg_exprs):])

      full_args = list(constant_args)
      for ix, arg in zip(deferred_arg_indices, actual_args):
        full_args[ix] = arg

      full_kwargs = dict(constant_kwargs)
      for key, arg in zip(deferred_kwarg_keys, actual_kwargs):
        full_kwargs[key] = arg

      return actual_func(*full_args, **full_kwargs)

    if (not requires_partition_by.is_subpartitioning_of(partitionings.Index())
        and sum(isinstance(arg.proxy(), pd.core.generic.NDFrame)
                for arg in deferred_exprs) > 1):
      # Implicit join on index if there is more than one indexed input.
      actual_requires_partition_by = partitionings.Index()
    else:
      actual_requires_partition_by = requires_partition_by

    result_expr = expressions.ComputedExpression(
        name,
        apply,
        deferred_exprs,
        requires_partition_by=actual_requires_partition_by,
        preserves_partition_by=preserves_partition_by)
    if inplace:
      args[0]._expr = result_expr

    else:
      return DeferredFrame.wrap(result_expr)

  return wrapper


def _agg_method(func):
  def wrapper(self, *args, **kwargs):
    return self.agg(func, *args, **kwargs)

  return wrapper


def wont_implement_method(msg):
  def wrapper(*args, **kwargs):
    raise WontImplementError(msg)

  return wrapper


def not_implemented_method(op, jira='BEAM-9547'):
  def wrapper(*args, **kwargs):
    raise NotImplementedError("'%s' is not yet supported (%s)" % (op, jira))

  return wrapper


def copy_and_mutate(func):
  def wrapper(self, *args, **kwargs):
    copy = self.copy()
    func(copy, *args, **kwargs)
    return copy

  return wrapper


def maybe_inplace(func):
  @functools.wraps(func)
  def wrapper(self, inplace=False, **kwargs):
    result = func(self, **kwargs)
    if inplace:
      self._expr = result._expr
    else:
      return result

  return wrapper


def args_to_kwargs(base_type):
  def wrap(func):
    arg_names = getfullargspec(unwrap(getattr(base_type, func.__name__))).args

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
      for name, value in zip(arg_names, args):
        if name in kwargs:
          raise TypeError(
              "%s() got multiple values for argument '%s'" %
              (func.__name__, name))
        kwargs[name] = value
      return func(**kwargs)

    return wrapper

  return wrap


def populate_defaults(base_type):
  def wrap(func):
    base_argspec = getfullargspec(unwrap(getattr(base_type, func.__name__)))
    if not base_argspec.defaults:
      return func

    arg_to_default = dict(
        zip(
            base_argspec.args[-len(base_argspec.defaults):],
            base_argspec.defaults))

    unwrapped_func = unwrap(func)
    # args that do not have defaults in func, but do have defaults in base
    func_argspec = getfullargspec(unwrapped_func)
    num_non_defaults = len(func_argspec.args) - len(func_argspec.defaults or ())
    defaults_to_populate = set(
        func_argspec.args[:num_non_defaults]).intersection(
            arg_to_default.keys())

    @functools.wraps(func)
    def wrapper(**kwargs):
      for name in defaults_to_populate:
        if name not in kwargs:
          kwargs[name] = arg_to_default[name]
      return func(**kwargs)

    return wrapper

  return wrap


class WontImplementError(NotImplementedError):
  """An subclass of NotImplementedError to raise indicating that implementing
  the given method is infeasible.

  Raising this error will also prevent this doctests from being validated
  when run with the beam dataframe validation doctest runner.
  """
  pass
