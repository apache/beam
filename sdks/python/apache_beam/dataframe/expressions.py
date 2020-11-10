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

import contextlib
import threading
from typing import Any
from typing import Callable
from typing import Iterable
from typing import Optional
from typing import TypeVar

from apache_beam.dataframe import partitionings


class Session(object):
  """A session represents a mapping of expressions to concrete values.

  The bindings typically include required placeholders, but may be any
  intermediate expression as well.
  """
  def __init__(self, bindings=None):
    self._bindings = dict(bindings or {})

  def evaluate(self, expr):  # type: (Expression) -> Any
    if expr not in self._bindings:
      self._bindings[expr] = expr.evaluate_at(self)
    return self._bindings[expr]

  def lookup(self, expr):  #  type: (Expression) -> Any
    return self._bindings[expr]


class PartitioningSession(Session):
  """An extension of Session that enforces actual partitioning of inputs.

  When evaluating an expression, inputs are partitioned according to its
  `requires_partition_by` specifications, the expression is evaluated on each
  partition separately, and the final result concatinated, as if this were
  actually executed in a parallel manner.

  For testing only.
  """
  def evaluate(self, expr):
    import pandas as pd
    import collections

    def is_scalar(expr):
      return not isinstance(expr.proxy(), pd.core.generic.NDFrame)

    if expr not in self._bindings:
      if is_scalar(expr) or not expr.args():
        result = super(PartitioningSession, self).evaluate(expr)
      else:
        scaler_args = [arg for arg in expr.args() if is_scalar(arg)]
        parts = collections.defaultdict(
            lambda: Session({arg: self.evaluate(arg)
                             for arg in scaler_args}))
        for arg in expr.args():
          if not is_scalar(arg):
            input = self.evaluate(arg)
            for key, part in expr.requires_partition_by().test_partition_fn(
                input):
              parts[key]._bindings[arg] = part
        if not parts:
          parts[None]  # Create at least one entry.

        results = []
        for session in parts.values():
          if any(len(session.lookup(arg)) for arg in expr.args()
                 if not is_scalar(arg)):
            results.append(session.evaluate(expr))
        if results:
          result = pd.concat(results)
        else:
          # Choose any single session.
          result = next(iter(parts.values())).evaluate(expr)
        self._bindings[expr] = result
    return self._bindings[expr]


# The return type of an Expression
T = TypeVar('T')


class Expression(object):
  """An expression is an operation bound to a set of arguments.

  An expression represents a deferred tree of operations, which can be
  evaluated at a specific bindings of root expressions to values.
  """
  def __init__(
      self,
      name,  # type: str
      proxy,  # type: T
      _id=None  # type: Optional[str]
  ):
    self._name = name
    self._proxy = proxy
    # Store for preservation through pickling.
    self._id = _id or '%s_%s_%s' % (name, type(proxy).__name__, id(self))

  def proxy(self):  # type: () -> T
    return self._proxy

  def __hash__(self):
    return hash(self._id)

  def __eq__(self, other):
    return self._id == other._id

  def __ne__(self, other):
    return not self == other

  def __repr__(self):
    return '%s[%s]' % (self.__class__.__name__, self._id)

  def placeholders(self):
    """Returns all the placeholders that self depends on."""
    raise NotImplementedError(type(self))

  def evaluate_at(self, session):  # type: (Session) -> T
    """Returns the result of self with the bindings given in session."""
    raise NotImplementedError(type(self))

  def requires_partition_by(self):  # type: () -> partitionings.Partitioning
    """Returns the partitioning, if any, require to evaluate this expression.

    Returns partitioning.Nothing() to require no partitioning is required.
    """
    raise NotImplementedError(type(self))

  def preserves_partition_by(self):  # type: () -> partitionings.Partitioning
    """Returns the partitioning, if any, preserved by this expression.

    This gives an upper bound on the partitioning of its ouput.  The actual
    partitioning of the output may be less strict (e.g. if the input was
    less partitioned).
    """
    raise NotImplementedError(type(self))


class PlaceholderExpression(Expression):
  """An expression whose value must be explicitly bound in the session."""
  def __init__(
      self,  # type: PlaceholderExpression
      proxy, # type: T
      reference=None,  # type: Any
  ):
    """Initialize a placeholder expression.

    Args:
      proxy: A proxy object with the type expected to be bound to this
        expression. Used for type checking at pipeline construction time.
    """
    super(PlaceholderExpression, self).__init__('placeholder', proxy)
    self._reference = reference

  def placeholders(self):
    return frozenset([self])

  def args(self):
    return ()

  def evaluate_at(self, session):
    return session.lookup(self)

  def requires_partition_by(self):
    return partitionings.Nothing()

  def preserves_partition_by(self):
    return partitionings.Nothing()


class ConstantExpression(Expression):
  """An expression whose value is known at pipeline construction time."""
  def __init__(
      self,  # type: ConstantExpression
      value,  # type: T
      proxy=None  # type: Optional[T]
  ):
    """Initialize a constant expression.

    Args:
      value: The constant value to be produced by this expression.
      proxy: (Optional) a proxy object with same type as `value` to use for
        rapid type checking at pipeline construction time. If not provided,
        `value` will be used directly.
    """
    if proxy is None:
      proxy = value
    super(ConstantExpression, self).__init__('constant', proxy)
    self._value = value

  def placeholders(self):
    return frozenset()

  def args(self):
    return ()

  def evaluate_at(self, session):
    return self._value

  def requires_partition_by(self):
    return partitionings.Nothing()

  def preserves_partition_by(self):
    return partitionings.Nothing()


class ComputedExpression(Expression):
  """An expression whose value must be computed at pipeline execution time."""
  def __init__(
      self,  # type: ComputedExpression
      name,  # type: str
      func,  # type: Callable[...,T]
      args,  # type: Iterable[Expression]
      proxy=None,  # type: Optional[T]
      _id=None,  # type: Optional[str]
      requires_partition_by=partitionings.Index(),  # type: partitionings.Partitioning
      preserves_partition_by=partitionings.Nothing(),  # type: partitionings.Partitioning
  ):
    """Initialize a computed expression.

    Args:
      name: The name of this expression.
      func: The function that will be used to compute the value of this
        expression. Should accept arguments of the types returned when
        evaluating the `args` expressions.
      args: The list of expressions that will be used to produce inputs to
        `func`.
      proxy: (Optional) a proxy object with same type as the objects that this
        ComputedExpression will produce at execution time. If not provided, a
        proxy will be generated using `func` and the proxies of `args`.
      _id: (Optional) a string to uniquely identify this expression.
      requires_partition_by: The required (common) partitioning of the args.
      preserves_partition_by: The level of partitioning preserved.
    """
    if (not _get_allow_non_parallel() and
        requires_partition_by == partitionings.Singleton()):
      raise NonParallelOperation(
          "Using non-parallel form of %s "
          "outside of allow_non_parallel_operations block." % name)
    args = tuple(args)
    if proxy is None:
      proxy = func(*(arg.proxy() for arg in args))
    super(ComputedExpression, self).__init__(name, proxy, _id)
    self._func = func
    self._args = args
    self._requires_partition_by = requires_partition_by
    self._preserves_partition_by = preserves_partition_by

  def placeholders(self):
    return frozenset.union(
        frozenset(), *[arg.placeholders() for arg in self.args()])

  def args(self):
    return self._args

  def evaluate_at(self, session):
    return self._func(*(session.evaluate(arg) for arg in self._args))

  def requires_partition_by(self):
    return self._requires_partition_by

  def preserves_partition_by(self):
    return self._preserves_partition_by


def elementwise_expression(name, func, args):
  return ComputedExpression(
      name,
      func,
      args,
      requires_partition_by=partitionings.Nothing(),
      preserves_partition_by=partitionings.Singleton())


_ALLOW_NON_PARALLEL = threading.local()
_ALLOW_NON_PARALLEL.value = False


def _get_allow_non_parallel():
  return _ALLOW_NON_PARALLEL.value


@contextlib.contextmanager
def allow_non_parallel_operations(allow=True):
  if allow is None:
    yield
  else:
    old_value, _ALLOW_NON_PARALLEL.value = _ALLOW_NON_PARALLEL.value, allow
    yield
    _ALLOW_NON_PARALLEL.value = old_value


class NonParallelOperation(Exception):
  pass
