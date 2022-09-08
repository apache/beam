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
#

"""Syntax & semantics for type-hinting custom-functions/PTransforms in the SDK.

This module defines type-hinting objects and the corresponding syntax for
type-hinting function arguments, function return types, or PTransform object
themselves. TypeHint's defined in the module can be used to implement either
static or run-time type-checking in regular Python code.

Type-hints are defined by 'indexing' a type-parameter into a defined
CompositeTypeHint instance:

  * 'List[int]'.

Valid type-hints are partitioned into two categories: simple, and composite.

Simple type hints are type hints based on a subset of Python primitive types:
int, bool, float, str, object, None, and bytes. No other primitive types are
allowed.

Composite type-hints are reserved for hinting the types of container-like
Python objects such as 'list'. Composite type-hints can be parameterized by an
inner simple or composite type-hint, using the 'indexing' syntax. In order to
avoid conflicting with the namespace of the built-in container types, when
specifying this category of type-hints, the first letter should be capitalized.
The following composite type-hints are permitted. NOTE: 'T' can be any of the
type-hints listed or a simple Python type:

  * Any
  * Union[T, T, T]
  * Optional[T]
  * Tuple[T, T]
  * Tuple[T, ...]
  * List[T]
  * KV[T, T]
  * Dict[T, T]
  * Set[T]
  * Iterable[T]
  * Iterator[T]
  * Generator[T]

Type-hints can be nested, allowing one to define type-hints for complex types:

  * 'List[Tuple[int, int, str]]

In addition, type-hints can be used to implement run-time type-checking via the
'type_check' method on each TypeConstraint.

"""

# pytype: skip-file

import copy
import logging
import typing
from collections import abc

__all__ = [
    'Any',
    'Union',
    'Optional',
    'Tuple',
    'List',
    'KV',
    'Dict',
    'Set',
    'FrozenSet',
    'Iterable',
    'Iterator',
    'Generator',
    'WindowedValue',
    'TypeVariable',
]

# A set of the built-in Python types we don't support, guiding the users
# to templated (upper-case) versions instead.
DISALLOWED_PRIMITIVE_TYPES = (list, set, frozenset, tuple, dict)

_LOGGER = logging.getLogger(__name__)


class SimpleTypeHintError(TypeError):
  pass


class CompositeTypeHintError(TypeError):
  pass


class GetitemConstructor(type):
  """A metaclass that makes Cls[arg] an alias for Cls(arg)."""
  def __getitem__(cls, arg):
    return cls(arg)


class TypeConstraint(object):
  """The base-class for all created type-constraints defined below.

  A :class:`TypeConstraint` is the result of parameterizing a
  :class:`CompositeTypeHint` with with one of the allowed Python types or
  another :class:`CompositeTypeHint`. It binds and enforces a specific
  version of a generalized TypeHint.
  """
  def _consistent_with_check_(self, sub):
    """Returns whether sub is consistent with self.

    Has the same relationship to is_consistent_with() as
    __subclasscheck__ does for issubclass().

    Not meant to be called directly; call is_consistent_with(sub, self)
    instead.

    Implementation may assume that maybe_sub_type is not Any
    and has been normalized.
    """
    raise NotImplementedError

  def type_check(self, instance):
    """Determines if the type of 'instance' satisfies this type constraint.

    Args:
      instance: An instance of a Python object.

    Raises:
      :class:`TypeError`: The passed **instance** doesn't satisfy
        this :class:`TypeConstraint`. Subclasses of
        :class:`TypeConstraint` are free to raise any of the subclasses of
        :class:`TypeError` defined above, depending on
        the manner of the type hint error.

    All :class:`TypeConstraint` sub-classes must define this method in other
    for the class object to be created.
    """
    raise NotImplementedError

  def match_type_variables(self, unused_concrete_type):
    return {}

  def bind_type_variables(self, unused_bindings):
    return self

  def _inner_types(self):
    """Iterates over the inner types of the composite type."""
    return []

  def visit(self, visitor, visitor_arg):
    """Visitor method to visit all inner types of a composite type.

    Args:
      visitor: A callable invoked for all nodes in the type tree comprising
        a composite type. The visitor will be called with the node visited
        and the visitor argument specified here.
      visitor_arg: Visitor callback second argument.
    """
    visitor(self, visitor_arg)
    for t in self._inner_types():
      if isinstance(t, TypeConstraint):
        t.visit(visitor, visitor_arg)
      else:
        visitor(t, visitor_arg)


def visit_inner_types(type_constraint, visitor, visitor_arg):
  """Visitor pattern to visit all inner types of a type constraint.

  Args:
    type_constraint: A type constraint or a type.
    visitor: A callable invoked for all nodes in the type tree comprising a
      composite type. The visitor will be called with the node visited and the
      visitor argument specified here.
    visitor_arg: Visitor callback second argument.

  Note:
    Raise and capture a StopIteration to terminate the visit, e.g.

    ```
    def visitor(type_constraint, visitor_arg):
      if ...:
        raise StopIteration

    try:
      visit_inner_types(type_constraint, visitor, visitor_arg)
    except StopIteration:
      pass
    ```
  """
  if isinstance(type_constraint, TypeConstraint):
    return type_constraint.visit(visitor, visitor_arg)
  return visitor(type_constraint, visitor_arg)


def match_type_variables(type_constraint, concrete_type):
  if isinstance(type_constraint, TypeConstraint):
    return type_constraint.match_type_variables(concrete_type)
  return {}


def bind_type_variables(type_constraint, bindings):
  if isinstance(type_constraint, TypeConstraint):
    return type_constraint.bind_type_variables(bindings)
  return type_constraint


class IndexableTypeConstraint(TypeConstraint):
  """An internal common base-class for all type constraints with indexing.
  E.G. SequenceTypeConstraint + Tuple's of fixed size.
  """
  def _constraint_for_index(self, idx):
    """Returns the type at the given index. This is used to allow type inference
    to determine the correct type for a specific index. On lists this will also
    be the same, however for tuples the value will depend on the position. This
    was added as part of the futurize changes since more of the expressions now
    index into tuples."""
    raise NotImplementedError


class SequenceTypeConstraint(IndexableTypeConstraint):
  """A common base-class for all sequence related type-constraint classes.

  A sequence is defined as an arbitrary length homogeneous container type. Type
  hints which fall under this category include: List[T], Set[T], Iterable[T],
  and Tuple[T, ...].

  Sub-classes may need to override '_consistent_with_check_' if a particular
  sequence requires special handling with respect to type compatibility.

  Attributes:
    inner_type: The type which every element in the sequence should be an
      instance of.
  """
  def __init__(self, inner_type, sequence_type):
    self.inner_type = normalize(inner_type)
    self._sequence_type = sequence_type

  def __eq__(self, other):
    return (
        isinstance(other, SequenceTypeConstraint) and
        type(self) == type(other) and self.inner_type == other.inner_type)

  def __hash__(self):
    return hash(self.inner_type) ^ 13 * hash(type(self))

  def _inner_types(self):
    yield self.inner_type

  def _constraint_for_index(self, idx):
    """Returns the type at the given index."""
    return self.inner_type

  def _consistent_with_check_(self, sub):
    return (
        isinstance(sub, self.__class__) and
        is_consistent_with(sub.inner_type, self.inner_type))

  def type_check(self, sequence_instance):
    if not isinstance(sequence_instance, self._sequence_type):
      raise CompositeTypeHintError(
          "%s type-constraint violated. Valid object instance "
          "must be of type '%s'. Instead, an instance of '%s' "
          "was received." % (
              self._sequence_type.__name__.title(),
              self._sequence_type.__name__.lower(),
              sequence_instance.__class__.__name__))

    for index, elem in enumerate(sequence_instance):
      try:
        check_constraint(self.inner_type, elem)
      except SimpleTypeHintError:
        raise CompositeTypeHintError(
            '%s hint type-constraint violated. The type of element #%s in '
            'the passed %s is incorrect. Expected an instance of type %s, '
            'instead received an instance of type %s.' % (
                repr(self),
                index,
                repr(self._sequence_type),
                repr(self.inner_type),
                elem.__class__.__name__))
      except CompositeTypeHintError as e:
        raise CompositeTypeHintError(
            '%s hint type-constraint violated. The type of element #%s in '
            'the passed %s is incorrect: %s' %
            (repr(self), index, self._sequence_type.__name__, e))

  def match_type_variables(self, concrete_type):
    if isinstance(concrete_type, SequenceTypeConstraint):
      return match_type_variables(self.inner_type, concrete_type.inner_type)
    return {}

  def bind_type_variables(self, bindings):
    bound_inner_type = bind_type_variables(self.inner_type, bindings)
    if bound_inner_type == self.inner_type:
      return self
    bound_self = copy.copy(self)
    bound_self.inner_type = bound_inner_type
    return bound_self


class CompositeTypeHint(object):
  """The base-class for all created type-hint classes defined below.

  CompositeTypeHint's serve primarily as TypeConstraint factories. They are
  only required to define a single method: '__getitem__' which should return a
  parameterized TypeConstraint, that can be used to enforce static or run-time
  type-checking.

  '__getitem__' is used as a factory function in order to provide a familiar
  API for defining type-hints. The ultimate result is that one will be able to
  use: CompositeTypeHint[type_parameter] to create a type-hint object that
  behaves like any other Python object. This allows one to create
  'type-aliases' by assigning the returned type-hints to a variable.

    * Example: 'Coordinates = List[Tuple[int, int]]'
  """
  def __getitem___(self, py_type):
    """Given a type creates a TypeConstraint instance parameterized by the type.

    This function serves as a factory function which creates TypeConstraint
    instances. Additionally, implementations by sub-classes should perform any
    sanity checking of the passed types in this method in order to rule-out
    disallowed behavior. Such as, attempting to create a TypeConstraint whose
    parameterized type is actually an object instance.

    Args:
      py_type: An instance of a Python type or TypeConstraint.

    Returns: An instance of a custom TypeConstraint for this CompositeTypeHint.

    Raises:
      TypeError: If the passed type violates any contraints for this particular
        TypeHint.
    """
    raise NotImplementedError


def is_typing_generic(type_param):
  """Determines if an object is a subscripted typing.Generic type, such as
  PCollection[int].

  Such objects are considered valid type parameters.

  Always returns false for Python versions below 3.7.
  """
  if hasattr(typing, '_GenericAlias'):
    return isinstance(type_param, typing._GenericAlias)
  return False


def validate_composite_type_param(type_param, error_msg_prefix):
  """Determines if an object is a valid type parameter to a
  :class:`CompositeTypeHint`.

  Implements sanity checking to disallow things like::

    List[1, 2, 3] or Dict[5].

  Args:
    type_param: An object instance.
    error_msg_prefix (:class:`str`): A string prefix used to format an error
      message in the case of an exception.

  Raises:
    TypeError: If the passed **type_param** is not a valid type
      parameter for a :class:`CompositeTypeHint`.
  """
  # Must either be a TypeConstraint instance or a basic Python type.
  possible_classes = [type, TypeConstraint]
  is_not_type_constraint = (
      not is_typing_generic(type_param) and
      not isinstance(type_param, tuple(possible_classes)) and
      type_param is not None and
      getattr(type_param, '__module__', None) != 'typing')
  is_forbidden_type = (
      isinstance(type_param, type) and type_param in DISALLOWED_PRIMITIVE_TYPES)

  if is_not_type_constraint or is_forbidden_type:
    raise TypeError(
        '%s must be a non-sequence, a type, or a TypeConstraint. %s'
        ' is an instance of %s.' %
        (error_msg_prefix, type_param, type_param.__class__.__name__))


def check_constraint(type_constraint, object_instance):
  """Determine if the passed type instance satisfies the TypeConstraint.

  When examining a candidate type for constraint satisfaction in
  'type_check', all CompositeTypeHint's eventually call this function. This
  function may end up being called recursively if the hinted type of a
  CompositeTypeHint is another CompositeTypeHint.

  Args:
    type_constraint: An instance of a TypeConstraint or a built-in Python type.
    object_instance: An object instance.

  Raises:
    SimpleTypeHintError: If 'type_constraint' is a one of the allowed primitive
      Python types and 'object_instance' isn't an instance of this type.
    CompositeTypeHintError: If 'type_constraint' is a TypeConstraint object and
      'object_instance' does not satisfy its constraint.
  """
  if type_constraint is None and object_instance is None:
    return
  elif isinstance(type_constraint, TypeConstraint):
    type_constraint.type_check(object_instance)
  elif type_constraint is None:
    # TODO(robertwb): Fix uses of None for Any.
    pass
  elif not isinstance(type_constraint, type):
    raise RuntimeError("bad type: %s" % (type_constraint, ))
  elif not isinstance(object_instance, type_constraint):
    raise SimpleTypeHintError


class AnyTypeConstraint(TypeConstraint):
  """An Any type-hint.

  Any is intended to be used as a "don't care" when hinting the types of
  function arguments or return types. All other TypeConstraint's are equivalent
  to 'Any', and its 'type_check' method is a no-op.
  """
  def __eq__(self, other):
    return type(self) == type(other)

  def __repr__(self):
    return 'Any'

  def __hash__(self):
    # TODO(https://github.com/apache/beam/issues/18633): Fix
    # typehints.TypeVariable issues with __hash__.
    return hash(id(self))

  def type_check(self, instance):
    pass


class TypeVariable(AnyTypeConstraint):
  def __init__(self, name, use_name_in_eq=True):
    self.name = name
    self.use_name_in_eq = use_name_in_eq

  def __eq__(self, other):
    # The "other" may be an Ellipsis object
    # so we have to check if it has use_name_in_eq first
    if self.use_name_in_eq and (hasattr(other, 'use_name_in_eq') and
                                other.use_name_in_eq):
      return type(self) == type(other) and self.name == other.name

    return type(self) == type(other)

  def __hash__(self):
    # TODO(https://github.com/apache/beam/issues/18633): Fix
    # typehints.TypeVariable issues with __hash__.
    return hash(id(self))

  def __repr__(self):
    return 'TypeVariable[%s]' % self.name

  def match_type_variables(self, concrete_type):
    return {self: concrete_type}

  def bind_type_variables(self, bindings):
    return bindings.get(
        self,
        # Star matches all type variables.
        bindings.get('*', self))


class UnionHint(CompositeTypeHint):
  """A Union type-hint. Union[X, Y] accepts instances of type X OR type Y.

  Duplicate type parameters are ignored. Additonally, Nested Union hints will
  be flattened out. For example:

    * Union[Union[str, int], bool] -> Union[str, int, bool]

  A candidate type instance satisfies a UnionConstraint if it is an
  instance of any of the parameterized 'union_types' for a Union.

  Union[X] is disallowed, and all type parameters will be sanity checked to
  ensure compatibility with nested type-hints.

  When comparing two Union hints, ordering is enforced before comparison.

    * Union[int, str] == Union[str, int]
  """
  class UnionConstraint(TypeConstraint):
    def __init__(self, union_types):
      self.union_types = set(normalize(t) for t in union_types)

    def __eq__(self, other):
      return (
          isinstance(other, UnionHint.UnionConstraint) and
          self.union_types == other.union_types)

    def __hash__(self):
      return 1 + sum(hash(t) for t in self.union_types)

    def __repr__(self):
      # Sorting the type name strings simplifies unit tests.
      return 'Union[%s]' % (
          ', '.join(sorted(repr(t) for t in self.union_types)))

    def inner_types(self):
      for t in self.union_types:
        yield t

    def contains_type(self, maybe_type):
      return maybe_type in self.union_types

    def _consistent_with_check_(self, sub):
      if isinstance(sub, UnionConstraint):
        # A union type is compatible if every possible type is compatible.
        # E.g. Union[A, B, C] > Union[A, B].
        return all(is_consistent_with(elem, self) for elem in sub.union_types)
      # Other must be compatible with at least one of this union's subtypes.
      # E.g. Union[A, B, C] > T if T > A or T > B or T > C.
      return any(is_consistent_with(sub, elem) for elem in self.union_types)

    def type_check(self, instance):
      error_msg = ''
      for t in self.union_types:
        try:
          check_constraint(t, instance)
          return
        except TypeError as e:
          error_msg = str(e)
          continue

      raise CompositeTypeHintError(
          '%s type-constraint violated. Expected an instance of one of: %s, '
          'received %s instead.%s' % (
              repr(self),
              tuple(sorted(repr(t) for t in self.union_types)),
              instance.__class__.__name__,
              error_msg))

    def match_type_variables(self, concrete_type):
      sub_bindings = [
          match_type_variables(t, concrete_type) for t in self.union_types
          if is_consistent_with(concrete_type, t)
      ]
      if sub_bindings:
        return {
            var: Union[(sub[var] for sub in sub_bindings)]
            for var in set.intersection(
                *[set(sub.keys()) for sub in sub_bindings])
        }
      else:
        return {}

    def bind_type_variables(self, bindings):
      return Union[(bind_type_variables(t, bindings) for t in self.union_types)]

  def __getitem__(self, type_params):
    if not isinstance(type_params, (abc.Iterable, set)):
      raise TypeError('Cannot create Union without a sequence of types.')

    # Flatten nested Union's and duplicated repeated type hints.
    params = set()
    dict_union = None
    for t in type_params:
      validate_composite_type_param(
          t, error_msg_prefix='All parameters to a Union hint')

      if isinstance(t, self.UnionConstraint):
        params |= t.union_types
      elif isinstance(t, DictConstraint):
        if dict_union is None:
          dict_union = t
        else:
          dict_union.key_type = Union[dict_union.key_type, t.key_type]
          dict_union.value_type = Union[dict_union.value_type, t.value_type]
      else:
        params.add(t)

    if dict_union is not None:
      params.add(dict_union)

    if Any in params:
      return Any
    elif len(params) == 1:
      return next(iter(params))
    return self.UnionConstraint(params)


UnionConstraint = UnionHint.UnionConstraint


class OptionalHint(UnionHint):
  """An Option type-hint. Optional[X] accepts instances of X or None.

  The Optional[X] factory function proxies to Union[X, type(None)]
  """
  def __getitem__(self, py_type):
    # A single type must have been passed.
    if isinstance(py_type, abc.Sequence):
      raise TypeError(
          'An Option type-hint only accepts a single type '
          'parameter.')

    return Union[py_type, type(None)]


def is_nullable(typehint):
  return (
      isinstance(typehint, UnionConstraint) and
      typehint.contains_type(type(None)) and
      len(list(typehint.inner_types())) == 2)


def get_concrete_type_from_nullable(typehint):
  if is_nullable(typehint):
    for inner_type in typehint.inner_types():
      if not type(None) == inner_type:
        return inner_type
  else:
    raise TypeError('Typehint is not of nullable type', typehint)


class TupleHint(CompositeTypeHint):
  """A Tuple type-hint.

  Tuple can accept 1 or more type-hint parameters.

  Tuple[X, Y] represents a tuple of *exactly* two elements, with the first
  being of type 'X' and the second an instance of type 'Y'.

    * (1, 2) satisfies Tuple[int, int]

  Additionally, one is able to type-hint an arbitary length, homogeneous tuple
  by passing the Ellipsis (...) object as the second parameter.

  As an example, Tuple[str, ...] indicates a tuple of any length with each
  element being an instance of 'str'.
  """
  class TupleSequenceConstraint(SequenceTypeConstraint):
    def __init__(self, type_param):
      super().__init__(type_param, tuple)

    def __repr__(self):
      return 'Tuple[%s, ...]' % repr(self.inner_type)

    def _consistent_with_check_(self, sub):
      if isinstance(sub, TupleConstraint):
        # E.g. Tuple[A, B] < Tuple[C, ...] iff A < C and B < C.
        return all(
            is_consistent_with(elem, self.inner_type)
            for elem in sub.tuple_types)
      return super()._consistent_with_check_(sub)

  class TupleConstraint(IndexableTypeConstraint):
    def __init__(self, type_params):
      self.tuple_types = tuple(normalize(t) for t in type_params)

    def __eq__(self, other):
      return (
          isinstance(other, TupleHint.TupleConstraint) and
          self.tuple_types == other.tuple_types)

    def __hash__(self):
      return hash(self.tuple_types)

    def __repr__(self):
      return 'Tuple[%s]' % (', '.join(repr(t) for t in self.tuple_types))

    def _inner_types(self):
      for t in self.tuple_types:
        yield t

    def _constraint_for_index(self, idx):
      """Returns the type at the given index."""
      return self.tuple_types[idx]

    def _consistent_with_check_(self, sub):
      return (
          isinstance(sub, self.__class__) and
          len(sub.tuple_types) == len(self.tuple_types) and all(
              is_consistent_with(sub_elem, elem) for sub_elem,
              elem in zip(sub.tuple_types, self.tuple_types)))

    def type_check(self, tuple_instance):
      if not isinstance(tuple_instance, tuple):
        raise CompositeTypeHintError(
            "Tuple type constraint violated. Valid object instance must be of "
            "type 'tuple'. Instead, an instance of '%s' was received." %
            tuple_instance.__class__.__name__)

      if len(tuple_instance) != len(self.tuple_types):
        raise CompositeTypeHintError(
            'Passed object instance is of the proper type, but differs in '
            'length from the hinted type. Expected a tuple of length %s, '
            'received a tuple of length %s.' %
            (len(self.tuple_types), len(tuple_instance)))

      for type_pos, (expected, actual) in enumerate(zip(self.tuple_types,
                                                        tuple_instance)):
        try:
          check_constraint(expected, actual)
          continue
        except SimpleTypeHintError:
          raise CompositeTypeHintError(
              '%s hint type-constraint violated. The type of element #%s in '
              'the passed tuple is incorrect. Expected an instance of '
              'type %s, instead received an instance of type %s.' %
              (repr(self), type_pos, repr(expected), actual.__class__.__name__))
        except CompositeTypeHintError as e:
          raise CompositeTypeHintError(
              '%s hint type-constraint violated. The type of element #%s in '
              'the passed tuple is incorrect. %s' % (repr(self), type_pos, e))

    def match_type_variables(self, concrete_type):
      bindings = {}
      if isinstance(concrete_type, TupleConstraint):
        for a, b in zip(self.tuple_types, concrete_type.tuple_types):
          bindings.update(match_type_variables(a, b))
      return bindings

    def bind_type_variables(self, bindings):
      bound_tuple_types = tuple(
          bind_type_variables(t, bindings) for t in self.tuple_types)
      if bound_tuple_types == self.tuple_types:
        return self
      return Tuple[bound_tuple_types]

  def __getitem__(self, type_params):
    ellipsis = False

    if not isinstance(type_params, abc.Iterable):
      # Special case for hinting tuples with arity-1.
      type_params = (type_params, )

    if type_params and type_params[-1] == Ellipsis:
      if len(type_params) != 2:
        raise TypeError(
            'Ellipsis can only be used to type-hint an arbitrary '
            'length tuple of containing a single type: '
            'Tuple[A, ...].')
      # Tuple[A, ...] indicates an arbitary length homogeneous tuple.
      type_params = type_params[:1]
      ellipsis = True

    for t in type_params:
      validate_composite_type_param(
          t, error_msg_prefix='All parameters to a Tuple hint')

    if ellipsis:
      return self.TupleSequenceConstraint(type_params[0])
    return self.TupleConstraint(type_params)


TupleConstraint = TupleHint.TupleConstraint
TupleSequenceConstraint = TupleHint.TupleSequenceConstraint


class ListHint(CompositeTypeHint):
  """A List type-hint.

  List[X] represents an instance of a list populated by a single homogeneous
  type. The parameterized type 'X' can either be a built-in Python type or an
  instance of another TypeConstraint.

    * ['1', '2', '3'] satisfies List[str]
  """
  class ListConstraint(SequenceTypeConstraint):
    def __init__(self, list_type):
      super().__init__(list_type, list)

    def __repr__(self):
      return 'List[%s]' % repr(self.inner_type)

  def __getitem__(self, t):
    validate_composite_type_param(t, error_msg_prefix='Parameter to List hint')

    return self.ListConstraint(t)


ListConstraint = ListHint.ListConstraint


class KVHint(CompositeTypeHint):
  """A KV type-hint, represents a Key-Value pair of a particular type.

  Internally, KV[X, Y] proxies to Tuple[X, Y]. A KV type-hint accepts only
  accepts exactly two type-parameters. The first represents the required
  key-type and the second the required value-type.
  """
  def __getitem__(self, type_params):
    if not isinstance(type_params, tuple):
      raise TypeError(
          'Parameter to KV type-hint must be a tuple of types: '
          'KV[.., ..].')

    if len(type_params) != 2:
      raise TypeError(
          'Length of parameters to a KV type-hint must be exactly 2. Passed '
          'parameters: %s, have a length of %s.' %
          (type_params, len(type_params)))

    return Tuple[type_params]


def key_value_types(kv):
  """Returns the key and value type of a KV type-hint.

  Args:
    kv: An instance of a TypeConstraint sub-class.
  Returns:
    A tuple: (key_type, value_type) if the passed type-hint is an instance of a
    KV type-hint, and (Any, Any) otherwise.
  """
  if isinstance(kv, TupleHint.TupleConstraint):
    return kv.tuple_types
  return Any, Any


class DictHint(CompositeTypeHint):
  """A Dict type-hint.

  Dict[K, V] Represents a dictionary where all keys are of a particular type
  and all values are of another (possible the same) type.
  """
  class DictConstraint(TypeConstraint):
    def __init__(self, key_type, value_type):
      self.key_type = normalize(key_type)
      self.value_type = normalize(value_type)

    def __repr__(self):
      return 'Dict[%s, %s]' % (repr(self.key_type), repr(self.value_type))

    def __eq__(self, other):
      return (
          type(self) == type(other) and self.key_type == other.key_type and
          self.value_type == other.value_type)

    def __hash__(self):
      return hash((type(self), self.key_type, self.value_type))

    def _inner_types(self):
      yield self.key_type
      yield self.value_type

    def _consistent_with_check_(self, sub):
      return (
          isinstance(sub, self.__class__) and
          is_consistent_with(sub.key_type, self.key_type) and
          is_consistent_with(sub.value_type, self.value_type))

    def _raise_hint_exception_or_inner_exception(
        self, is_key, incorrect_instance, inner_error_message=''):
      incorrect_type = 'values' if not is_key else 'keys'
      hinted_type = self.value_type if not is_key else self.key_type
      if inner_error_message:
        raise CompositeTypeHintError(
            '%s hint %s-type constraint violated. All %s should be of type '
            '%s. Instead: %s' % (
                repr(self),
                incorrect_type[:-1],
                incorrect_type,
                repr(hinted_type),
                inner_error_message))
      else:
        raise CompositeTypeHintError(
            '%s hint %s-type constraint violated. All %s should be of '
            'type %s. Instead, %s is of type %s.' % (
                repr(self),
                incorrect_type[:-1],
                incorrect_type,
                repr(hinted_type),
                incorrect_instance,
                incorrect_instance.__class__.__name__))

    def type_check(self, dict_instance):
      if not isinstance(dict_instance, dict):
        raise CompositeTypeHintError(
            'Dict type-constraint violated. All passed instances must be of '
            'type dict. %s is of type %s.' %
            (dict_instance, dict_instance.__class__.__name__))

      for key, value in dict_instance.items():
        try:
          check_constraint(self.key_type, key)
        except CompositeTypeHintError as e:
          self._raise_hint_exception_or_inner_exception(True, key, str(e))
        except SimpleTypeHintError:
          self._raise_hint_exception_or_inner_exception(True, key)

        try:
          check_constraint(self.value_type, value)
        except CompositeTypeHintError as e:
          self._raise_hint_exception_or_inner_exception(False, value, str(e))
        except SimpleTypeHintError:
          self._raise_hint_exception_or_inner_exception(False, value)

    def match_type_variables(self, concrete_type):
      if isinstance(concrete_type, DictConstraint):
        bindings = {}
        bindings.update(
            match_type_variables(self.key_type, concrete_type.key_type))
        bindings.update(
            match_type_variables(self.value_type, concrete_type.value_type))
        return bindings
      return {}

    def bind_type_variables(self, bindings):
      bound_key_type = bind_type_variables(self.key_type, bindings)
      bound_value_type = bind_type_variables(self.value_type, bindings)
      if (bound_key_type, self.key_type) == (bound_value_type, self.value_type):
        return self
      return Dict[bound_key_type, bound_value_type]

  def __getitem__(self, type_params):
    # Type param must be a (k, v) pair.
    if not isinstance(type_params, tuple):
      raise TypeError(
          'Parameter to Dict type-hint must be a tuple of types: '
          'Dict[.., ..].')

    if len(type_params) != 2:
      raise TypeError(
          'Length of parameters to a Dict type-hint must be exactly 2. Passed '
          'parameters: %s, have a length of %s.' %
          (type_params, len(type_params)))

    key_type, value_type = type_params

    validate_composite_type_param(
        key_type, error_msg_prefix='Key-type parameter to a Dict hint')
    validate_composite_type_param(
        value_type, error_msg_prefix='Value-type parameter to a Dict hint')

    return self.DictConstraint(key_type, value_type)


DictConstraint = DictHint.DictConstraint


class SetHint(CompositeTypeHint):
  """A Set type-hint.


  Set[X] defines a type-hint for a set of homogeneous types. 'X' may be either a
  built-in Python type or a another nested TypeConstraint.
  """
  class SetTypeConstraint(SequenceTypeConstraint):
    def __init__(self, type_param):
      super().__init__(type_param, set)

    def __repr__(self):
      return 'Set[%s]' % repr(self.inner_type)

  def __getitem__(self, type_param):
    validate_composite_type_param(
        type_param, error_msg_prefix='Parameter to a Set hint')

    return self.SetTypeConstraint(type_param)


SetTypeConstraint = SetHint.SetTypeConstraint


class FrozenSetHint(CompositeTypeHint):
  """A FrozenSet type-hint.

  FrozenSet[X] defines a type-hint for a set of homogeneous types. 'X' may be
  either a  built-in Python type or a another nested TypeConstraint.

  This is a mirror copy of SetHint - consider refactoring common functionality.
  """
  class FrozenSetTypeConstraint(SequenceTypeConstraint):
    def __init__(self, type_param):
      super(FrozenSetHint.FrozenSetTypeConstraint,
            self).__init__(type_param, frozenset)

    def __repr__(self):
      return 'FrozenSet[%s]' % repr(self.inner_type)

  def __getitem__(self, type_param):
    validate_composite_type_param(
        type_param, error_msg_prefix='Parameter to a FrozenSet hint')

    return self.FrozenSetTypeConstraint(type_param)


FrozenSetTypeConstraint = FrozenSetHint.FrozenSetTypeConstraint


class IterableHint(CompositeTypeHint):
  """An Iterable type-hint.

  Iterable[X] defines a type-hint for an object implementing an '__iter__'
  method which yields objects which are all of the same type.
  """
  class IterableTypeConstraint(SequenceTypeConstraint):
    def __init__(self, iter_type):
      super(IterableHint.IterableTypeConstraint,
            self).__init__(iter_type, abc.Iterable)

    def __repr__(self):
      return 'Iterable[%s]' % repr(self.inner_type)

    def _consistent_with_check_(self, sub):
      if isinstance(sub, SequenceTypeConstraint):
        return is_consistent_with(sub.inner_type, self.inner_type)
      elif isinstance(sub, TupleConstraint):
        if not sub.tuple_types:
          # The empty tuple is consistent with Iterator[T] for any T.
          return True
        # Each element in the hetrogenious tuple must be consistent with
        # the iterator type.
        # E.g. Tuple[A, B] < Iterable[C] if A < C and B < C.
        return all(
            is_consistent_with(elem, self.inner_type)
            for elem in sub.tuple_types)
      return False

  def __getitem__(self, type_param):
    validate_composite_type_param(
        type_param, error_msg_prefix='Parameter to an Iterable hint')

    return self.IterableTypeConstraint(type_param)


IterableTypeConstraint = IterableHint.IterableTypeConstraint


class IteratorHint(CompositeTypeHint):
  """An Iterator type-hint.

  Iterator[X] defines a type-hint for an object implementing both '__iter__'
  and a 'next' method which yields objects which are all of the same type. Type
  checking a type-hint of this type is deferred in order to avoid depleting the
  underlying lazily generated sequence. See decorators.interleave_type_check for
  further information.
  """
  class IteratorTypeConstraint(TypeConstraint):
    def __init__(self, t):
      self.yielded_type = normalize(t)

    def __repr__(self):
      return 'Iterator[%s]' % repr(self.yielded_type)

    def __eq__(self, other):
      return (
          type(self) == type(other) and self.yielded_type == other.yielded_type)

    def __hash__(self):
      return hash(self.yielded_type)

    def _inner_types(self):
      yield self.yielded_type

    def _consistent_with_check_(self, sub):
      return (
          isinstance(sub, self.__class__) and
          is_consistent_with(sub.yielded_type, self.yielded_type))

    def type_check(self, instance):
      # Special case for lazy types, we only need to enforce the underlying
      # type. This avoid having to compute the entirety of the generator/iter.
      try:
        check_constraint(self.yielded_type, instance)
        return
      except CompositeTypeHintError as e:
        raise CompositeTypeHintError(
            '%s hint type-constraint violated: %s' % (repr(self), str(e)))
      except SimpleTypeHintError:
        raise CompositeTypeHintError(
            '%s hint type-constraint violated. Expected a iterator of type %s. '
            'Instead received a iterator of type %s.' %
            (repr(self), repr(self.yielded_type), instance.__class__.__name__))

  def __getitem__(self, type_param):
    validate_composite_type_param(
        type_param, error_msg_prefix='Parameter to an Iterator hint')

    return self.IteratorTypeConstraint(type_param)


IteratorTypeConstraint = IteratorHint.IteratorTypeConstraint


class WindowedTypeConstraint(TypeConstraint, metaclass=GetitemConstructor):
  """A type constraint for WindowedValue objects.

  Mostly for internal use.

  Attributes:
    inner_type: The type which the element should be an instance of.
  """
  def __init__(self, inner_type):
    self.inner_type = normalize(inner_type)

  def __eq__(self, other):
    return (
        isinstance(other, WindowedTypeConstraint) and
        self.inner_type == other.inner_type)

  def __hash__(self):
    return hash(self.inner_type) ^ 13 * hash(type(self))

  def _inner_types(self):
    yield self.inner_type

  def _consistent_with_check_(self, sub):
    return (
        isinstance(sub, self.__class__) and
        is_consistent_with(sub.inner_type, self.inner_type))

  def type_check(self, instance):
    from apache_beam.transforms import window
    if not isinstance(instance, window.WindowedValue):
      raise CompositeTypeHintError(
          "Window type-constraint violated. Valid object instance "
          "must be of type 'WindowedValue'. Instead, an instance of '%s' "
          "was received." % (instance.__class__.__name__))

    try:
      check_constraint(self.inner_type, instance.value)
    except (CompositeTypeHintError, SimpleTypeHintError):
      raise CompositeTypeHintError(
          '%s hint type-constraint violated. The type of element in '
          'is incorrect. Expected an instance of type %s, '
          'instead received an instance of type %s.' % (
              repr(self),
              repr(self.inner_type),
              instance.value.__class__.__name__))


class GeneratorHint(IteratorHint):
  """A Generator type hint.

  Subscriptor is in the form [yield_type, send_type, return_type], however
  only yield_type is supported. The 2 others are expected to be None.
  """
  def __getitem__(self, type_params):
    if isinstance(type_params, tuple) and len(type_params) == 3:
      yield_type, send_type, return_type = type_params
      if send_type is not type(None):
        _LOGGER.warning('Ignoring send_type hint: %s' % send_type)
      if return_type is not type(None):
        _LOGGER.warning('Ignoring return_type hint: %s' % return_type)
    else:
      yield_type = type_params
    return self.IteratorTypeConstraint(yield_type)


# Create the actual instances for all defined type-hints above.
Any = AnyTypeConstraint()
Union = UnionHint()
Optional = OptionalHint()
Tuple = TupleHint()
List = ListHint()
KV = KVHint()
Dict = DictHint()
Set = SetHint()
FrozenSet = FrozenSetHint()
Iterable = IterableHint()
Iterator = IteratorHint()
Generator = GeneratorHint()
WindowedValue = WindowedTypeConstraint

# There is a circular dependency between defining this mapping
# and using it in normalize().  Initialize it here and populate
# it below.
_KNOWN_PRIMITIVE_TYPES = {}  # type: typing.Dict[type, typing.Any]


def normalize(x, none_as_type=False):
  # None is inconsistantly used for Any, unknown, or NoneType.
  if none_as_type and x is None:
    return type(None)
  elif x in _KNOWN_PRIMITIVE_TYPES:
    return _KNOWN_PRIMITIVE_TYPES[x]
  elif getattr(x, '__module__', None) == 'typing':
    # Avoid circular imports
    from apache_beam.typehints import native_type_compatibility
    beam_type = native_type_compatibility.convert_to_beam_type(x)
    if beam_type != x:
      # We were able to do the conversion.
      return beam_type
    else:
      # It might be a compatible type we don't understand.
      return Any
  return x


_KNOWN_PRIMITIVE_TYPES.update({
    dict: Dict[Any, Any],
    list: List[Any],
    tuple: Tuple[Any, ...],
    set: Set[Any],
    frozenset: FrozenSet[Any],
})


def is_consistent_with(sub, base):
  """Checks whether sub a is consistent with base.

  This is according to the terminology of PEP 483/484.  This relationship is
  neither symmetric nor transitive, but a good mnemonic to keep in mind is that
  is_consistent_with(a, b) is roughly equivalent to the issubclass(a, b)
  relation, but also handles the special Any type as well as type
  parameterization.
  """
  if sub == base:
    # Common special case.
    return True
  if isinstance(sub, AnyTypeConstraint) or isinstance(base, AnyTypeConstraint):
    return True
  sub = normalize(sub, none_as_type=True)
  base = normalize(base, none_as_type=True)
  if isinstance(sub, UnionConstraint):
    return all(is_consistent_with(c, base) for c in sub.union_types)
  elif isinstance(base, TypeConstraint):
    return base._consistent_with_check_(sub)
  elif isinstance(sub, TypeConstraint):
    # Nothing but object lives above any type constraints.
    return base == object
  return issubclass(sub, base)


def get_yielded_type(type_hint):
  """Obtains the type of elements yielded by an iterable.

  Note that "iterable" here means: can be iterated over in a for loop, excluding
  strings and dicts.

  Args:
    type_hint: (TypeConstraint) The iterable in question. Must be normalize()-d.

  Returns:
    Yielded type of the iterable.

  Raises:
    ValueError if not iterable.
  """
  if isinstance(type_hint, AnyTypeConstraint):
    return type_hint
  if is_consistent_with(type_hint, Iterator[Any]):
    return type_hint.yielded_type
  if is_consistent_with(type_hint, Tuple[Any, ...]):
    if isinstance(type_hint, TupleConstraint):
      return Union[type_hint.tuple_types]
    else:  # TupleSequenceConstraint
      return type_hint.inner_type
  if is_consistent_with(type_hint, Iterable[Any]):
    return type_hint.inner_type
  raise ValueError('%s is not iterable' % type_hint)


def coerce_to_kv_type(element_type, label=None, side_input_producer=None):
  """Attempts to coerce element_type to a compatible kv type.

  Raises an error on failure.
  """
  if side_input_producer:
    consumer = 'side-input of %r (producer: %r)' % (label, side_input_producer)
  else:
    consumer = '%r' % label

  # If element_type is not specified, then treat it as `Any`.
  if not element_type:
    return KV[Any, Any]
  elif isinstance(element_type, TupleHint.TupleConstraint):
    if len(element_type.tuple_types) == 2:
      return element_type
    else:
      raise ValueError(
          "Tuple input to %s must have two components. "
          "Found %s." % (consumer, element_type))
  elif isinstance(element_type, AnyTypeConstraint):
    # `Any` type needs to be replaced with a KV[Any, Any] to
    # satisfy the KV form.
    return KV[Any, Any]
  elif isinstance(element_type, UnionConstraint):
    union_types = [coerce_to_kv_type(t) for t in element_type.union_types]
    return KV[Union[tuple(t.tuple_types[0] for t in union_types)],
              Union[tuple(t.tuple_types[1] for t in union_types)]]
  else:
    # TODO: Possibly handle other valid types.
    raise ValueError(
        "Input to %s must be compatible with KV[Any, Any]. "
        "Found %s." % (consumer, element_type))
