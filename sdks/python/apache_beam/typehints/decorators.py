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

"""Type hinting decorators allowing static or runtime type-checking for the SDK.

This module defines decorators which utilize the type-hints defined in
'type_hints.py' to allow annotation of the types of function arguments and
return values.

Type-hints for functions are annotated using two separate decorators. One is for
type-hinting the types of function arguments, the other for type-hinting the
function return value. Type-hints can either be specified in the form of
positional arguments::

  @with_input_types(int, int)
  def add(a, b):
    return a + b

Keyword arguments::

  @with_input_types(a=int, b=int)
  def add(a, b):
    return a + b

Or even a mix of both::

  @with_input_types(int, b=int)
  def add(a, b):
    return a + b

Example usage for type-hinting arguments only::

  @with_input_types(s=str)
  def to_lower(a):
    return a.lower()

Example usage for type-hinting return values only::

  @with_output_types(Tuple[int, bool])
  def compress_point(ec_point):
    return ec_point.x, ec_point.y < 0

Example usage for type-hinting both arguments and return values::

  @with_input_types(a=int)
  @with_output_types(str)
  def int_to_str(a):
    return str(a)

Type-hinting a function with arguments that unpack tuples are also supported. As
an example, such a function would be defined as::

  def foo((a, b)):
    ...

The valid type-hint for such as function looks like the following::

  @with_input_types(a=int, b=int)
  def foo((a, b)):
    ...

Notice that we hint the type of each unpacked argument independently, rather
than hinting the type of the tuple as a whole (Tuple[int, int]).

Optionally, type-hints can be type-checked at runtime. To toggle this behavior
this module defines two functions: 'enable_run_time_type_checking' and
'disable_run_time_type_checking'. NOTE: for this toggle behavior to work
properly it must appear at the top of the module where all functions are
defined, or before importing a module containing type-hinted functions.
"""

from __future__ import absolute_import

import inspect
import types
from builtins import next
from builtins import object
from builtins import zip

from apache_beam.typehints import native_type_compatibility
from apache_beam.typehints import typehints
from apache_beam.typehints.typehints import CompositeTypeHintError
from apache_beam.typehints.typehints import SimpleTypeHintError
from apache_beam.typehints.typehints import check_constraint
from apache_beam.typehints.typehints import validate_composite_type_param

__all__ = [
    'with_input_types',
    'with_output_types',
    'WithTypeHints',
    'TypeCheckError',
]


# This is missing in the builtin types module.  str.upper is arbitrary, any
# method on a C-implemented type will do.
# pylint: disable=invalid-name
_MethodDescriptorType = type(str.upper)
# pylint: enable=invalid-name

try:
  _original_getfullargspec = inspect.getfullargspec
  _use_full_argspec = True
except AttributeError:  # Python 2
  _original_getfullargspec = inspect.getargspec
  _use_full_argspec = False


def getfullargspec(func):
  try:
    return _original_getfullargspec(func)
  except TypeError:
    if isinstance(func, type):
      argspec = getfullargspec(func.__init__)
      del argspec.args[0]
      return argspec
    elif callable(func):
      try:
        return _original_getfullargspec(func.__call__)
      except TypeError:
        # Return an ArgSpec with at least one positional argument,
        # and any number of other (positional or keyword) arguments
        # whose name won't match any real argument.
        # Arguments with the %unknown% prefix will be ignored in the type
        # checking code.
        if _use_full_argspec:
          return inspect.FullArgSpec(
              ['_'], '__unknown__varargs', '__unknown__keywords', (),
              [], {}, {})
        else:  # Python 2
          return inspect.ArgSpec(
              ['_'], '__unknown__varargs', '__unknown__keywords', ())
    else:
      raise


class IOTypeHints(object):
  """Encapsulates all type hint information about a Dataflow construct.

  This should primarily be used via the WithTypeHints mixin class, though
  may also be attached to other objects (such as Python functions).
  """
  __slots__ = ('input_types', 'output_types')

  def __init__(self, input_types=None, output_types=None):
    self.input_types = input_types
    self.output_types = output_types

  def set_input_types(self, *args, **kwargs):
    self.input_types = args, kwargs

  def set_output_types(self, *args, **kwargs):
    self.output_types = args, kwargs

  def simple_output_type(self, context):
    if self.output_types:
      args, kwargs = self.output_types
      if len(args) != 1 or kwargs:
        raise TypeError('Expected simple output type hint for %s' % context)
      return args[0]

  def copy(self):
    return IOTypeHints(self.input_types, self.output_types)

  def with_defaults(self, hints):
    if not hints:
      return self
    elif not self:
      return hints
    return IOTypeHints(self.input_types or hints.input_types,
                       self.output_types or hints.output_types)

  def __bool__(self):
    return bool(self.input_types or self.output_types)

  def __repr__(self):
    return 'IOTypeHints[inputs=%s, outputs=%s]' % (
        self.input_types, self.output_types)


class WithTypeHints(object):
  """A mixin class that provides the ability to set and retrieve type hints.
  """

  def __init__(self, *unused_args, **unused_kwargs):
    self._type_hints = IOTypeHints()

  def _get_or_create_type_hints(self):
    # __init__ may have not been called
    try:
      return self._type_hints
    except AttributeError:
      self._type_hints = IOTypeHints()
      return self._type_hints

  def get_type_hints(self):
    return (self._get_or_create_type_hints()
            .with_defaults(self.default_type_hints())
            .with_defaults(get_type_hints(self.__class__)))

  def default_type_hints(self):
    return None

  def with_input_types(self, *arg_hints, **kwarg_hints):
    self._get_or_create_type_hints().set_input_types(*arg_hints, **kwarg_hints)
    return self

  def with_output_types(self, *arg_hints, **kwarg_hints):
    self._get_or_create_type_hints().set_output_types(*arg_hints, **kwarg_hints)
    return self


class TypeCheckError(Exception):
  pass


def _positional_arg_hints(arg, hints):
  """Returns the type of a (possibly tuple-packed) positional argument.

  E.g. for lambda ((a, b), c): None the single positional argument is (as
  returned by inspect) [[a, b], c] which should have type
  Tuple[Tuple[Int, Any], float] when applied to the type hints
  {a: int, b: Any, c: float}.
  """
  if isinstance(arg, list):
    return typehints.Tuple[[_positional_arg_hints(a, hints) for a in arg]]
  return hints.get(arg, typehints.Any)


def _unpack_positional_arg_hints(arg, hint):
  """Unpacks the given hint according to the nested structure of arg.

  For example, if arg is [[a, b], c] and hint is Tuple[Any, int], than
  this function would return ((Any, Any), int) so it can be used in conjunction
  with inspect.getcallargs.
  """
  if isinstance(arg, list):
    tuple_constraint = typehints.Tuple[[typehints.Any] * len(arg)]
    if not typehints.is_consistent_with(hint, tuple_constraint):
      raise TypeCheckError('Bad tuple arguments for %s: expected %s, got %s' %
                           (arg, tuple_constraint, hint))
    if isinstance(hint, typehints.TupleConstraint):
      return tuple(_unpack_positional_arg_hints(a, t)
                   for a, t in zip(arg, hint.tuple_types))
    return (typehints.Any,) * len(arg)
  return hint


def getcallargs_forhints(func, *typeargs, **typekwargs):
  """Like inspect.getcallargs, but understands that Tuple[] and an Any unpack.
  """
  argspec = getfullargspec(func)
  # Turn Tuple[x, y] into (x, y) so getcallargs can do the proper unpacking.
  packed_typeargs = [_unpack_positional_arg_hints(arg, hint)
                     for (arg, hint) in zip(argspec.args, typeargs)]
  packed_typeargs += list(typeargs[len(packed_typeargs):])

  # Monkeypatch inspect.getfullargspec to allow passing non-function objects.
  # getfullargspec (getargspec on Python 2) are used by inspect.getcallargs.
  # TODO(BEAM-5490): Reimplement getcallargs and stop relying on monkeypatch.
  if _use_full_argspec:
    inspect.getfullargspec = getfullargspec
  else:  # Python 2
    inspect.getargspec = getfullargspec

  try:
    callargs = inspect.getcallargs(func, *packed_typeargs, **typekwargs)
  except TypeError as e:
    raise TypeCheckError(e)
  finally:
    # Revert monkey-patch.
    if _use_full_argspec:
      inspect.getfullargspec = _original_getfullargspec
    else:
      inspect.getargspec = _original_getfullargspec

  if argspec.defaults:
    # Declare any default arguments to be Any.
    for k, var in enumerate(reversed(argspec.args)):
      if k >= len(argspec.defaults):
        break
      if callargs.get(var, None) is argspec.defaults[-k-1]:
        callargs[var] = typehints.Any
  # Patch up varargs and keywords
  if argspec.varargs:
    callargs[argspec.varargs] = typekwargs.get(
        argspec.varargs, typehints.Tuple[typehints.Any, ...])
  if _use_full_argspec:
    varkw = argspec.varkw
  else:  # Python 2
    varkw = argspec.keywords

  if varkw:
    # TODO(robertwb): Consider taking the union of key and value types.
    callargs[varkw] = typekwargs.get(
        varkw, typehints.Dict[typehints.Any, typehints.Any])

  # TODO(BEAM-5878) Support kwonlyargs.

  return callargs


def get_type_hints(fn):
  """Gets the type hint associated with an arbitrary object fn.

  Always returns a valid IOTypeHints object, creating one if necissary.
  """
  # pylint: disable=protected-access
  if not hasattr(fn, '_type_hints'):
    try:
      fn._type_hints = IOTypeHints()
    except (AttributeError, TypeError):
      # Can't add arbitrary attributes to this object,
      # but might have some restrictions anyways...
      hints = IOTypeHints()
      if isinstance(fn, _MethodDescriptorType):
        hints.set_input_types(fn.__objclass__)
      return hints
  return fn._type_hints
  # pylint: enable=protected-access


def with_input_types(*positional_hints, **keyword_hints):
  """A decorator that type-checks defined type-hints with passed func arguments.

  All type-hinted arguments can be specified using positional arguments,
  keyword arguments, or a mix of both. Additionaly, all function arguments must
  be type-hinted in totality if even one parameter is type-hinted.

  Once fully decorated, if the arguments passed to the resulting function
  violate the type-hint constraints defined, a :class:`TypeCheckError`
  detailing the error will be raised.

  To be used as:

  .. testcode::

    from apache_beam.typehints import with_input_types

    @with_input_types(str)
    def upper(s):
      return s.upper()

  Or:

  .. testcode::

    from apache_beam.typehints import with_input_types
    from apache_beam.typehints import List
    from apache_beam.typehints import Tuple

    @with_input_types(ls=List[Tuple[int, int]])
    def increment(ls):
      [(i + 1, j + 1) for (i,j) in ls]

  Args:
    *positional_hints: Positional type-hints having identical order as the
      function's formal arguments. Values for this argument must either be a
      built-in Python type or an instance of a
      :class:`~apache_beam.typehints.typehints.TypeConstraint` created by
      'indexing' a
      :class:`~apache_beam.typehints.typehints.CompositeTypeHint` instance
      with a type parameter.
    **keyword_hints: Keyword arguments mirroring the names of the parameters to
      the decorated functions. The value of each keyword argument must either
      be one of the allowed built-in Python types, a custom class, or an
      instance of a :class:`~apache_beam.typehints.typehints.TypeConstraint`
      created by 'indexing' a
      :class:`~apache_beam.typehints.typehints.CompositeTypeHint` instance
      with a type parameter.

  Raises:
    :class:`~exceptions.ValueError`: If not all function arguments have
      corresponding type-hints specified. Or if the inner wrapper function isn't
      passed a function object.
    :class:`TypeCheckError`: If the any of the passed type-hint
      constraints are not a type or
      :class:`~apache_beam.typehints.typehints.TypeConstraint` instance.

  Returns:
    The original function decorated such that it enforces type-hint constraints
    for all received function arguments.
  """

  converted_positional_hints = (
      native_type_compatibility.convert_to_beam_types(positional_hints))
  converted_keyword_hints = (
      native_type_compatibility.convert_to_beam_types(keyword_hints))
  del positional_hints
  del keyword_hints

  def annotate(f):
    if isinstance(f, types.FunctionType):
      for t in (list(converted_positional_hints) +
                list(converted_keyword_hints.values())):
        validate_composite_type_param(
            t, error_msg_prefix='All type hint arguments')

    get_type_hints(f).set_input_types(*converted_positional_hints,
                                      **converted_keyword_hints)
    return f
  return annotate


def with_output_types(*return_type_hint, **kwargs):
  """A decorator that type-checks defined type-hints for return values(s).

  This decorator will type-check the return value(s) of the decorated function.

  Only a single type-hint is accepted to specify the return type of the return
  value. If the function to be decorated has multiple return values, then one
  should use: ``Tuple[type_1, type_2]`` to annotate the types of the return
  values.

  If the ultimate return value for the function violates the specified type-hint
  a :class:`TypeCheckError` will be raised detailing the type-constraint
  violation.

  This decorator is intended to be used like:

  .. testcode::

    from apache_beam.typehints import with_output_types
    from apache_beam.typehints import Set

    class Coordinate(object):
      def __init__(self, x, y):
        self.x = x
        self.y = y

    @with_output_types(Set[Coordinate])
    def parse_ints(ints):
      return {Coordinate(i, i) for i in ints}

  Or with a simple type-hint:

  .. testcode::

    from apache_beam.typehints import with_output_types

    @with_output_types(bool)
    def negate(p):
      return not p if p else p

  Args:
    *return_type_hint: A type-hint specifying the proper return type of the
      function. This argument should either be a built-in Python type or an
      instance of a :class:`~apache_beam.typehints.typehints.TypeConstraint`
      created by 'indexing' a
      :class:`~apache_beam.typehints.typehints.CompositeTypeHint`.
    **kwargs: Not used.

  Raises:
    :class:`~exceptions.ValueError`: If any kwarg parameters are passed in,
      or the length of **return_type_hint** is greater than ``1``. Or if the
      inner wrapper function isn't passed a function object.
    :class:`TypeCheckError`: If the **return_type_hint** object is
      in invalid type-hint.

  Returns:
    The original function decorated such that it enforces type-hint constraints
    for all return values.
  """
  if kwargs:
    raise ValueError("All arguments for the 'returns' decorator must be "
                     "positional arguments.")

  if len(return_type_hint) != 1:
    raise ValueError("'returns' accepts only a single positional argument. In "
                     "order to specify multiple return types, use the 'Tuple' "
                     "type-hint.")

  return_type_hint = native_type_compatibility.convert_to_beam_type(
      return_type_hint[0])

  validate_composite_type_param(
      return_type_hint,
      error_msg_prefix='All type hint arguments'
  )

  def annotate(f):
    get_type_hints(f).set_output_types(return_type_hint)
    return f

  return annotate


def _check_instance_type(
    type_constraint, instance, var_name=None, verbose=False):
  """A helper function to report type-hint constraint violations.

  Args:
    type_constraint: An instance of a 'TypeConstraint' or a built-in Python
      type.
    instance: The candidate object which will be checked by to satisfy
      'type_constraint'.
    var_name: If 'instance' is an argument, then the actual name for the
      parameter in the original function definition.

  Raises:
    TypeCheckError: If 'instance' fails to meet the type-constraint of
      'type_constraint'.
  """
  hint_type = (
      "argument: '%s'" % var_name if var_name is not None else 'return type')

  try:
    check_constraint(type_constraint, instance)
  except SimpleTypeHintError:
    if verbose:
      verbose_instance = '%s, ' % instance
    else:
      verbose_instance = ''
    raise TypeCheckError('Type-hint for %s violated. Expected an '
                         'instance of %s, instead found %san instance of %s.'
                         % (hint_type, type_constraint,
                            verbose_instance, type(instance)))
  except CompositeTypeHintError as e:
    raise TypeCheckError('Type-hint for %s violated: %s' % (hint_type, e))


def _interleave_type_check(type_constraint, var_name=None):
  """Lazily type-check the type-hint for a lazily generated sequence type.

  This function can be applied as a decorator or called manually in a curried
  manner:
    * @_interleave_type_check(List[int])
      def gen():
        yield 5

    or

     * gen = _interleave_type_check(Tuple[int, int], 'coord_gen')(gen)

  As a result, all type-checking for the passed generator will occur at 'yield'
  time. This way, we avoid having to depleat the generator in order to
  type-check it.

  Args:
    type_constraint: An instance of a TypeConstraint. The output yielded of
      'gen' will be type-checked according to this type constraint.
    var_name: The variable name binded to 'gen' if type-checking a function
      argument. Used solely for templating in error message generation.

  Returns:
    A function which takes a generator as an argument and returns a wrapped
    version of the generator that interleaves type-checking at 'yield'
    iteration. If the generator received is already wrapped, then it is simply
    returned to avoid nested wrapping.
  """
  def wrapper(gen):
    if isinstance(gen, GeneratorWrapper):
      return gen
    return GeneratorWrapper(
        gen,
        lambda x: _check_instance_type(type_constraint, x, var_name)
    )
  return wrapper


class GeneratorWrapper(object):
  """A wrapper around a generator, allows execution of a callback per yield.

  Additionally, wrapping a generator with this class allows one to assign
  arbitary attributes to a generator object just as with a function object.

  Attributes:
    internal_gen: A instance of a generator object. As part of 'step' of the
      generator, the yielded object will be passed to 'interleave_func'.
    interleave_func: A callback accepting a single argument. This function will
      be called with the result of each yielded 'step' in the internal
      generator.
  """

  def __init__(self, gen, interleave_func):
    self.internal_gen = gen
    self.interleave_func = interleave_func

  def __getattr__(self, attr):
    # TODO(laolu): May also want to intercept 'send' in the future if we move to
    # a GeneratorHint with 3 type-params:
    #   * Generator[send_type, return_type, yield_type]
    if attr == '__next__':
      return self.__next__()
    elif attr == '__iter__':
      return self.__iter__()
    return getattr(self.internal_gen, attr)

  def __next__(self):
    next_val = next(self.internal_gen)
    self.interleave_func(next_val)
    return next_val

  next = __next__

  def __iter__(self):
    while True:
      x = next(self.internal_gen)
      self.interleave_func(x)
      yield x
