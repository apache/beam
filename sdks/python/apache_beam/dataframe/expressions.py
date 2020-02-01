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


class Session(object):
  """A session represents a mapping of expressions to concrete values.

  The bindings typically include required placeholders, but may be any
  intermediate expression as well.
  """
  def __init__(self, bindings={}):
    self._bindings = dict(bindings)

  def evaluate(self, expr):
    if expr not in self._bindings:
      self._bindings[expr] = expr.evaluate_at(self)
    return self._bindings[expr]

  def lookup(self, expr):
    return self._bindings[expr]


class Expression(object):
  """An expression is an operation bound to a set of arguments.

  An expression represents a deferred tree of operations, which can be
  evaluated at a specific bindings of root expressions to values.
  """
  def __init__(self, name, proxy=None, _id=None):
    self._name = name
    self._proxy = proxy
    # Store for preservation through pickling.
    self._id = _id or '%s_%s' % (name, id(self))

  def proxy(self):
    return self._proxy

  def __hash__(self):
    return hash(self._id)

  def __eq__(self):
    return self._id == other._id

  def __ne__(self, other):
    return not self == other

  def evaluate_at(self, session):
    """Returns the result of self with the bindings given in session."""
    raise NotImplementedError(type(self))

  def requires_partition_by_index(self):
    """Whether this expression requires its argument(s) to be partitioned
    by index."""
    # TODO: It might be necessary to support partitioning by part of the index,
    # for some args, which would require returning more than a boolean here.
    raise NotImplementedError(type(self))

  def preserves_partition_by_index(self):
    """Whether the result of this expression will be partitioned by index
    whenever all of its inputs are partitioned by index."""
    raise NotImplementedError(type(self))


class PlaceholderExpression(Expression):
  """An expression whose value must be explicitly bound in the session."""
  def __init__(self, proxy):
    super(PlaceholderExpression, self).__init__('placeholder', proxy)

  def args(self):
    return ()

  def evaluate_at(self, session):
    return session.lookup(self)

  def requires_partition_by_index(self):
    return False

  def preserves_partition_by_index(self):
    return False


class ConstantExpression(Expression):
  """An expression whose value is known at compile time."""
  def __init__(self, value, proxy=None):
    if proxy is None:
      proxy = value
    super(ConstantExpression, self).__init__('constant', proxy)
    self._value = value

  def args(self):
    return ()

  def evaluate_at(self, session):
    return self._value

  def requires_partition_by_index(self):
    return False

  def preserves_partition_by_index(self):
    return False


class ComputedExpression(Expression):
  def __init__(
      self,  # type: ComputedExpression
      name,  # type: str
      func,  # type: callable
      args,  # type: Iterable[Expression]
      proxy=None,  # type: Optional[Any]
      _id=None,  # type: Optional[str]
      requires_partition_by_index=True,  # type: bool
      preserves_partition_by_index=False,  # type: bool
  ):
    args = tuple(args)
    if proxy is None:
      proxy = func(*(arg.proxy() for arg in args))
    super(ComputedExpression, self).__init__(name, proxy, _id)
    self._func = func
    self._args = args
    self._requires_partition_by_index = requires_partition_by_index
    self._preserves_partition_by_index = preserves_partition_by_index

  def args(self):
    return self._args

  def evaluate_at(self, session):
    return self._func(*(arg.evaluate_at(session) for arg in self._args))

  def requires_partition_by_index(self):
    return self._requires_partition_by_index

  def preserves_partition_by_index(self):
    return self._preserves_partition_by_index


def elementwise_expression(name, func, args):
  return ComputedExpression(
      name,
      func,
      args,
      requires_partition_by_index=False,
      preserves_partition_by_index=True)
