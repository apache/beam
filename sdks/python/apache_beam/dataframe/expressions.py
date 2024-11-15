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

import contextlib
import random
import threading
from collections.abc import Callable
from collections.abc import Iterable
from typing import Any
from typing import Generic
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

  Each expression is evaluated multiple times for various supported
  partitionings determined by its `requires_partition_by` specification. For
  each tested partitioning, the input is partitioned and the expression is
  evaluated on each partition separately, as if this were actually executed in
  a parallel manner.

  For each input partitioning, the results are verified to be partitioned
  appropriately according to the expression's `preserves_partition_by`
  specification.

  For testing only.
  """
  def evaluate(self, expr):
    import pandas as pd
    import collections

    def is_scalar(expr):
      return not isinstance(expr.proxy(), pd.core.generic.NDFrame)

    if expr not in self._bindings:
      if is_scalar(expr) or not expr.args():
        result = super().evaluate(expr)
      else:
        scaler_args = [arg for arg in expr.args() if is_scalar(arg)]

        def evaluate_with(input_partitioning):
          parts = collections.defaultdict(
              lambda: Session({arg: self.evaluate(arg)
                               for arg in scaler_args}))
          for arg in expr.args():
            if not is_scalar(arg):
              input = self.evaluate(arg)
              for key, part in input_partitioning.test_partition_fn(input):
                parts[key]._bindings[arg] = part
          if not parts:
            parts[None]  # Create at least one entry.

          results = []
          for session in parts.values():
            if any(len(session.lookup(arg)) for arg in expr.args()
                   if not is_scalar(arg)):
              results.append(session.evaluate(expr))

          expected_output_partitioning = output_partitioning(
              expr, input_partitioning)

          if not expected_output_partitioning.check(results):
            raise AssertionError(
                f"""Expression does not preserve partitioning!
                Expression: {expr}
                Requires: {expr.requires_partition_by()}
                Preserves: {expr.preserves_partition_by()}
                Input partitioning: {input_partitioning}
                Expected output partitioning: {expected_output_partitioning}
                """)

          if results:
            return pd.concat(results)
          else:
            # Choose any single session.
            return next(iter(parts.values())).evaluate(expr)

        # Store random state so it can be re-used for each execution, in case
        # the expression is part of a test that relies on the random seed.
        random_state = random.getstate()

        result = None
        # Run with all supported partitionings s.t. the smallest subpartitioning
        # is used last. This way the final result is computed with the most
        # challenging partitioning. Avoids heisenbugs where sometimes the result
        # is computed trivially with Singleton partitioning and passes.
        for input_partitioning in sorted(set([expr.requires_partition_by(),
                                              partitionings.Arbitrary(),
                                              partitionings.JoinIndex(),
                                              partitionings.Index(),
                                              partitionings.Singleton()])):
          if not expr.requires_partition_by().is_subpartitioning_of(
              input_partitioning):
            continue

          random.setstate(random_state)

          result = evaluate_with(input_partitioning)

        assert result is not None
        self._bindings[expr] = result
    return self._bindings[expr]


# The return type of an Expression
T = TypeVar('T')


def output_partitioning(expr, input_partitioning):
  """ Return the expected output partitioning for `expr` when it's input is
  partitioned by `input_partitioning`.

  For internal use only; No backward compatibility guarantees """
  assert expr.requires_partition_by().is_subpartitioning_of(input_partitioning)

  if expr.preserves_partition_by().is_subpartitioning_of(input_partitioning):
    return min(input_partitioning, expr.preserves_partition_by())
  else:
    return partitionings.Arbitrary()


class Expression(Generic[T]):
  """An expression is an operation bound to a set of arguments.

  An expression represents a deferred tree of operations, which can be
  evaluated at a specific bindings of root expressions to values.

  requires_partition_by indicates the upper bound of a set of partitionings that
  are acceptable inputs to this expression. The expression should be able to
  produce the correct result when given input(s) partitioned by its
  requires_partition_by attribute, or by any partitoning that is _not_
  a subpartitioning of it.

  preserves_partition_by indicates the upper bound of a set of partitionings
  that can be preserved by this expression. When the input(s) to this expression
  are partitioned by preserves_partition_by, or by any partitioning that is
  _not_ a subpartitioning of it, this expression should produce output(s)
  partitioned by the same partitioning.

  However, if the partitioning of an expression's input is a subpartitioning of
  the partitioning that it preserves, the output is presumed to have no
  particular partitioning (i.e. Arbitrary()).

  For example, let's look at an "element-wise operation", that has no
  partitioning requirement, and preserves any partitioning given to it::

    requires_partition_by = Arbitrary() -----------------------------+
                                                                     |
             +-----------+-------------+---------- ... ----+---------|
             |           |             |                   |         |
        Singleton() < Index([i]) < Index([i, j]) < ... < Index() < Arbitrary()
             |           |             |                   |         |
             +-----------+-------------+---------- ... ----+---------|
                                                                     |
    preserves_partition_by = Arbitrary() ----------------------------+

  As a more interesting example, consider this expression, which requires Index
  partitioning, and preserves just Singleton partitioning::

    requires_partition_by = Index() -----------------------+
                                                           |
             +-----------+-------------+---------- ... ----|
             |           |             |                   |
        Singleton() < Index([i]) < Index([i, j]) < ... < Index() < Arbitrary()
             |
             |
    preserves_partition_by = Singleton()

  Note that any non-Arbitrary partitioning is an acceptable input for this
  expression. However, unless the inputs are Singleton-partitioned, the
  expression makes no guarantees about the partitioning of the output.
  """
  def __init__(self, name: str, proxy: T, _id: Optional[str] = None):
    self._name = name
    self._proxy = proxy
    # Store for preservation through pickling.
    self._id = _id or '%s_%s_%s' % (name, type(proxy).__name__, id(self))

  def proxy(self) -> T:
    return self._proxy

  def __hash__(self):
    return hash(self._id)

  def __eq__(self, other):
    return self._id == other._id

  def __repr__(self):
    return '%s[%s]' % (self.__class__.__name__, self._id)

  def placeholders(self):
    """Returns all the placeholders that self depends on."""
    raise NotImplementedError(type(self))

  def evaluate_at(self, session: Session) -> T:
    """Returns the result of self with the bindings given in session."""
    raise NotImplementedError(type(self))

  def requires_partition_by(self) -> partitionings.Partitioning:
    """Returns the partitioning, if any, require to evaluate this expression.

    Returns partitioning.Arbitrary() to require no partitioning is required.
    """
    raise NotImplementedError(type(self))

  def preserves_partition_by(self) -> partitionings.Partitioning:
    """Returns the partitioning, if any, preserved by this expression.

    This gives an upper bound on the partitioning of its ouput.  The actual
    partitioning of the output may be less strict (e.g. if the input was
    less partitioned).
    """
    raise NotImplementedError(type(self))


class PlaceholderExpression(Expression):
  """An expression whose value must be explicitly bound in the session."""
  def __init__(
      self,
      proxy: T,
      reference: Any = None,
  ):
    """Initialize a placeholder expression.

    Args:
      proxy: A proxy object with the type expected to be bound to this
        expression. Used for type checking at pipeline construction time.
    """
    super().__init__('placeholder', proxy)
    self._reference = reference

  def placeholders(self):
    return frozenset([self])

  def args(self):
    return ()

  def evaluate_at(self, session):
    return session.lookup(self)

  def requires_partition_by(self):
    return partitionings.Arbitrary()

  def preserves_partition_by(self):
    return partitionings.Index()


class ConstantExpression(Expression):
  """An expression whose value is known at pipeline construction time."""
  def __init__(self, value: T, proxy: Optional[T] = None):
    """Initialize a constant expression.

    Args:
      value: The constant value to be produced by this expression.
      proxy: (Optional) a proxy object with same type as `value` to use for
        rapid type checking at pipeline construction time. If not provided,
        `value` will be used directly.
    """
    if proxy is None:
      proxy = value
    super().__init__('constant', proxy)
    self._value = value

  def placeholders(self):
    return frozenset()

  def args(self):
    return ()

  def evaluate_at(self, session):
    return self._value

  def requires_partition_by(self):
    return partitionings.Arbitrary()

  def preserves_partition_by(self):
    return partitionings.Arbitrary()


class ComputedExpression(Expression):
  """An expression whose value must be computed at pipeline execution time."""
  def __init__(
      self,
      name: str,
      func: Callable[..., T],
      args: Iterable[Expression],
      proxy: Optional[T] = None,
      _id: Optional[str] = None,
      requires_partition_by: partitionings.Partitioning = partitionings.Index(),
      preserves_partition_by: partitionings.Partitioning = partitionings.
      Singleton(),
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
        isinstance(requires_partition_by, partitionings.Singleton)):
      reason = requires_partition_by.reason or (
          f"Encountered non-parallelizable form of {name!r}.")

      raise NonParallelOperation(
          f"{reason}\n"
          "Consider using an allow_non_parallel_operations block if you're "
          "sure you want to do this. See "
          "https://s.apache.org/dataframe-non-parallel-operations for more "
          "information.")
    args = tuple(args)
    if proxy is None:
      proxy = func(*(arg.proxy() for arg in args))
    super().__init__(name, proxy, _id)
    self._func = func
    self._args = args
    self._requires_partition_by = requires_partition_by
    self._preserves_partition_by = preserves_partition_by

  def placeholders(self):
    if not hasattr(self, '_placeholders'):
      self._placeholders = frozenset.union(
          frozenset(), *[arg.placeholders() for arg in self.args()])
    return self._placeholders

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
      requires_partition_by=partitionings.Arbitrary(),
      preserves_partition_by=partitionings.Arbitrary())


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
    try:
      yield
    finally:
      _ALLOW_NON_PARALLEL.value = old_value


class NonParallelOperation(Exception):
  def __init__(self, msg):
    super().__init__(self, msg)
    self.msg = msg
