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

Type-hinting a function with arguments that unpack tuples are also supported
(in Python 2 only). As an example, such a function would be defined as::

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

# pytype: skip-file

from __future__ import absolute_import

import inspect
import itertools
import logging
import sys
import traceback
import types
from builtins import next
from builtins import object
from builtins import zip
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Tuple
from typing import TypeVar
from typing import Union

from apache_beam.typehints import native_type_compatibility
from apache_beam.typehints import typehints
from apache_beam.typehints.native_type_compatibility import convert_to_beam_type
from apache_beam.typehints.typehints import CompositeTypeHintError
from apache_beam.typehints.typehints import SimpleTypeHintError
from apache_beam.typehints.typehints import check_constraint
from apache_beam.typehints.typehints import validate_composite_type_param

try:
  import funcsigs  # Python 2 only.
except ImportError:
  funcsigs = None

__all__ = [
    'disable_type_annotations',
    'no_annotations',
    'with_input_types',
    'with_output_types',
    'WithTypeHints',
    'TypeCheckError',
]

T = TypeVar('T')
WithTypeHintsT = TypeVar('WithTypeHintsT', bound='WithTypeHints')  # pylint: disable=invalid-name

# This is missing in the builtin types module.  str.upper is arbitrary, any
# method on a C-implemented type will do.
# pylint: disable=invalid-name
_MethodDescriptorType = type(str.upper)
# pylint: enable=invalid-name

_ANY_VAR_POSITIONAL = typehints.Tuple[typehints.Any, ...]
_ANY_VAR_KEYWORD = typehints.Dict[typehints.Any, typehints.Any]
_disable_from_callable = False

try:
  _original_getfullargspec = inspect.getfullargspec
  _use_full_argspec = True
except AttributeError:  # Python 2
  _original_getfullargspec = inspect.getargspec  # type: ignore
  _use_full_argspec = False


def getfullargspec(func):
  # Python 3: Use get_signature instead.
  assert sys.version_info < (3, ), 'This method should not be used in Python 3'
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
          return inspect.FullArgSpec(['_'],
                                     '__unknown__varargs',
                                     '__unknown__keywords', (), [], {}, {})
        else:  # Python 2
          return inspect.ArgSpec(['_'],
                                 '__unknown__varargs',
                                 '__unknown__keywords', ())
    else:
      raise


def get_signature(func):
  """Like inspect.signature(), but supports Py2 as well.

  This module uses inspect.signature instead of getfullargspec since in the
  latter: 'the "self" parameter is always reported, even for bound methods'
  https://github.com/python/cpython/blob/44f91c388a6f4da9ed3300df32ca290b8aa104ea/Lib/inspect.py#L1103
  """
  # Fall back on funcsigs if inspect module doesn't have 'signature'; prefer
  # inspect.signature over funcsigs.signature if both are available.
  if hasattr(inspect, 'signature'):
    inspect_ = inspect
  else:
    inspect_ = funcsigs

  try:
    signature = inspect_.signature(func)
  except ValueError:
    # Fall back on a catch-all signature.
    params = [
        inspect_.Parameter('_', inspect_.Parameter.POSITIONAL_OR_KEYWORD),
        inspect_.Parameter(
            '__unknown__varargs', inspect_.Parameter.VAR_POSITIONAL),
        inspect_.Parameter(
            '__unknown__keywords', inspect_.Parameter.VAR_KEYWORD)
    ]

    signature = inspect_.Signature(params)

  # This is a specialization to hint the first argument of certain builtins,
  # such as str.strip.
  if isinstance(func, _MethodDescriptorType):
    params = list(signature.parameters.values())
    if params[0].annotation == params[0].empty:
      params[0] = params[0].replace(annotation=func.__objclass__)
      signature = signature.replace(parameters=params)

  # This is a specialization to hint the return value of type callables.
  if (signature.return_annotation == signature.empty and
      isinstance(func, type)):
    signature = signature.replace(return_annotation=typehints.normalize(func))

  return signature


def no_annotations(fn):
  """Decorator that prevents Beam from using type hint annotations on a
  callable."""
  setattr(fn, '_beam_no_annotations', True)
  return fn


def disable_type_annotations():
  """Prevent Beam from using type hint annotations to determine input and output
  types of transforms.

  This setting applies globally.
  """
  global _disable_from_callable
  _disable_from_callable = True


class IOTypeHints(NamedTuple(
    'IOTypeHints',
    [('input_types', Optional[Tuple[Tuple[Any, ...], Dict[str, Any]]]),
     ('output_types', Optional[Tuple[Tuple[Any, ...], Dict[str, Any]]]),
     ('origin', List[str])])):
  """Encapsulates all type hint information about a Dataflow construct.

  This should primarily be used via the WithTypeHints mixin class, though
  may also be attached to other objects (such as Python functions).

  Attributes:
    input_types: (tuple, dict) List of typing types, and an optional dictionary.
      May be None. The list and dict correspond to args and kwargs.
    output_types: (tuple, dict) List of typing types, and an optional dictionary
      (unused). Only the first element of the list is used. May be None.
    origin: (List[str]) Stack of tracebacks of method calls used to create this
      instance.
  """

  traceback_limit = 5

  @classmethod
  def _make_origin(cls, bases, tb=True, msg=()):
    # type: (List[IOTypeHints], bool, Iterable[str]) -> List[str]
    if msg:
      res = list(msg)
    else:
      res = []
    if tb:
      # Omit this method and the IOTypeHints method that called it.
      num_frames_skip = 2
      tbs = traceback.format_stack(limit=cls.traceback_limit +
                                   num_frames_skip)[:-num_frames_skip]
      # tb is a list of strings in the form of 'File ...\n[code]\n'. Split into
      # single lines and flatten.
      res += list(
          itertools.chain.from_iterable(s.strip().split('\n') for s in tbs))

    bases = [base for base in bases if base.origin]
    if bases:
      res += ['', 'based on:']
      for i, base in enumerate(bases):
        if i > 0:
          res += ['', 'and:']
        res += ['  ' + str(base)]
        res += ['  ' + s for s in base.origin]
    return res

  @classmethod
  def empty(cls):
    # type: () -> IOTypeHints

    """Construct a base IOTypeHints object with no hints."""
    return IOTypeHints(None, None, [])

  @classmethod
  def from_callable(cls, fn):
    # type: (Callable) -> Optional[IOTypeHints]

    """Construct an IOTypeHints object from a callable's signature.

    Supports Python 3 annotations. For partial annotations, sets unknown types
    to Any, _ANY_VAR_POSITIONAL, or _ANY_VAR_KEYWORD.

    Returns:
      A new IOTypeHints or None if no annotations found.
    """
    if _disable_from_callable or getattr(fn, '_beam_no_annotations', False):
      return None
    signature = get_signature(fn)
    if (all(param.annotation == param.empty
            for param in signature.parameters.values()) and
        signature.return_annotation == signature.empty):
      return None
    input_args = []
    input_kwargs = {}
    for param in signature.parameters.values():
      if param.annotation == param.empty:
        if param.kind == param.VAR_POSITIONAL:
          input_args.append(_ANY_VAR_POSITIONAL)
        elif param.kind == param.VAR_KEYWORD:
          input_kwargs[param.name] = _ANY_VAR_KEYWORD
        elif param.kind == param.KEYWORD_ONLY:
          input_kwargs[param.name] = typehints.Any
        else:
          input_args.append(typehints.Any)
      else:
        if param.kind in [param.KEYWORD_ONLY, param.VAR_KEYWORD]:
          input_kwargs[param.name] = convert_to_beam_type(param.annotation)
        else:
          assert param.kind in [param.POSITIONAL_ONLY,
                                param.POSITIONAL_OR_KEYWORD,
                                param.VAR_POSITIONAL], \
              'Unsupported Parameter kind: %s' % param.kind
          input_args.append(convert_to_beam_type(param.annotation))
    output_args = []
    if signature.return_annotation != signature.empty:
      output_args.append(convert_to_beam_type(signature.return_annotation))
    else:
      output_args.append(typehints.Any)

    name = getattr(fn, '__name__', '<unknown>')
    msg = ['from_callable(%s)' % name, '  signature: %s' % signature]
    if hasattr(fn, '__code__'):
      msg.append(
          '  File "%s", line %d' %
          (fn.__code__.co_filename, fn.__code__.co_firstlineno))
    return IOTypeHints(
        input_types=(tuple(input_args), input_kwargs),
        output_types=(tuple(output_args), {}),
        origin=cls._make_origin([], tb=False, msg=msg))

  def with_input_types(self, *args, **kwargs):
    # type: (...) -> IOTypeHints
    return self._replace(
        input_types=(args, kwargs), origin=self._make_origin([self]))

  def with_output_types(self, *args, **kwargs):
    # type: (...) -> IOTypeHints
    return self._replace(
        output_types=(args, kwargs), origin=self._make_origin([self]))

  def simple_output_type(self, context):
    if self._has_output_types():
      args, kwargs = self.output_types
      if len(args) != 1 or kwargs:
        raise TypeError(
            'Expected single output type hint for %s but got: %s' %
            (context, self.output_types))
      return args[0]

  def has_simple_output_type(self):
    """Whether there's a single positional output type."""
    return (
        self.output_types and len(self.output_types[0]) == 1 and
        not self.output_types[1])

  def strip_pcoll(self):
    from apache_beam.pipeline import Pipeline
    from apache_beam.pvalue import PBegin
    from apache_beam.pvalue import PDone

    return self.strip_pcoll_helper(self.input_types,
                                   self._has_input_types,
                                   'input_types',
                                    [Pipeline, PBegin],
                                   'This input type hint will be ignored '
                                   'and not used for type-checking purposes. '
                                   'Typically, input type hints for a '
                                   'PTransform are single (or nested) types '
                                   'wrapped by a PCollection, or PBegin.',
                                   'strip_pcoll_input()').\
                strip_pcoll_helper(self.output_types,
                                   self.has_simple_output_type,
                                   'output_types',
                                   [PDone, None],
                                   'This output type hint will be ignored '
                                   'and not used for type-checking purposes. '
                                   'Typically, output type hints for a '
                                   'PTransform are single (or nested) types '
                                   'wrapped by a PCollection, PDone, or None.',
                                   'strip_pcoll_output()')

  def strip_pcoll_helper(
      self,
      my_type,            # type: any
      has_my_type,        # type: Callable[[], bool]
      my_key,             # type: str
      special_containers,   # type: List[Union[PBegin, PDone, PCollection]]
      error_str,          # type: str
      source_str          # type: str
      ):
    # type: (...) -> IOTypeHints
    from apache_beam.pvalue import PCollection

    if not has_my_type() or not my_type or len(my_type[0]) != 1:
      return self

    my_type = my_type[0][0]

    if isinstance(my_type, typehints.AnyTypeConstraint):
      return self

    special_containers += [PCollection]

    if (my_type not in special_containers and
        getattr(my_type, '__origin__', None) != PCollection):
      logging.warning(error_str + ' Got: %s instead.' % my_type)
      kwarg_dict = {}
      kwarg_dict[my_key] = None
      return self._replace(
          origin=self._make_origin([self], tb=False, msg=[source_str]),
          **kwarg_dict)
    kwarg_dict = {}

    if (getattr(my_type, '__args__', -1) in [-1, None] or
        len(my_type.__args__) == 0):
      # e.g. PCollection (or PBegin/PDone)
      kwarg_dict[my_key] = ((typehints.Any, ), {})
    else:
      # e.g. PCollection[type]
      kwarg_dict[my_key] = ((my_type.__args__[0], ), {})

    return self._replace(
        origin=self._make_origin([self], tb=False, msg=[source_str]),
        **kwarg_dict)

  def strip_iterable(self):
    # type: () -> IOTypeHints

    """Removes outer Iterable (or equivalent) from output type.

    Only affects instances with simple output types, otherwise is a no-op.
    Does not modify self.

    Designed to be used with type hints from callables of ParDo, FlatMap, DoFn.
    Output type may be Optional[T], in which case the result of stripping T is
    used as the output type.
    Output type may be None/NoneType, in which case nothing is done.

    Example: Generator[Tuple(int, int)] becomes Tuple(int, int)

    Returns:
      A copy of this instance with a possibly different output type.

    Raises:
      ValueError if output type is simple and not iterable.
    """
    if self.output_types is None or not self.has_simple_output_type():
      return self
    output_type = self.output_types[0][0]
    if output_type is None or isinstance(output_type, type(None)):
      return self
    # If output_type == Optional[T]: output_type = T.
    if isinstance(output_type, typehints.UnionConstraint):
      types = list(output_type.union_types)
      if len(types) == 2:
        try:
          types.remove(type(None))
          output_type = types[0]
        except ValueError:
          pass

    yielded_type = typehints.get_yielded_type(output_type)
    return self._replace(
        output_types=((yielded_type, ), {}),
        origin=self._make_origin([self], tb=False, msg=['strip_iterable()']))

  def with_defaults(self, hints):
    # type: (Optional[IOTypeHints]) -> IOTypeHints
    if not hints:
      return self
    if not self:
      return hints
    if self._has_input_types():
      input_types = self.input_types
    else:
      input_types = hints.input_types
    if self._has_output_types():
      output_types = self.output_types
    else:
      output_types = hints.output_types
    res = IOTypeHints(
        input_types,
        output_types,
        self._make_origin([self, hints], tb=False, msg=['with_defaults()']))
    if res == self:
      return self  # Don't needlessly increase origin traceback length.
    else:
      return res

  def _has_input_types(self):
    return self.input_types is not None and any(self.input_types)

  def _has_output_types(self):
    return self.output_types is not None and any(self.output_types)

  def __bool__(self):
    return self._has_input_types() or self._has_output_types()

  def __repr__(self):
    return 'IOTypeHints[inputs=%s, outputs=%s]' % (
        self.input_types, self.output_types)

  def debug_str(self):
    return '\n'.join([self.__repr__()] + self.origin)

  def __eq__(self, other):
    def same(a, b):
      if a is None or not any(a):
        return b is None or not any(b)
      else:
        return a == b

    return (
        same(self.input_types, other.input_types) and
        same(self.output_types, other.output_types))

  def __ne__(self, other):
    return not self == other

  def __hash__(self):
    return hash(str(self))

  def __reduce__(self):
    # Don't include "origin" debug information in pickled form.
    return (IOTypeHints, (self.input_types, self.output_types, []))


class WithTypeHints(object):
  """A mixin class that provides the ability to set and retrieve type hints.
  """
  def __init__(self, *unused_args, **unused_kwargs):
    self._type_hints = IOTypeHints.empty()

  def _get_or_create_type_hints(self):
    # type: () -> IOTypeHints
    # __init__ may have not been called
    try:
      # Only return an instance bound to self (see BEAM-8629).
      return self.__dict__['_type_hints']
    except KeyError:
      self._type_hints = IOTypeHints.empty()
      return self._type_hints

  def get_type_hints(self):
    """Gets and/or initializes type hints for this object.

    If type hints have not been set, attempts to initialize type hints in this
    order:
    - Using self.default_type_hints().
    - Using self.__class__ type hints.
    """
    return (
        self._get_or_create_type_hints().with_defaults(
            self.default_type_hints()).with_defaults(
                get_type_hints(self.__class__)))

  def default_type_hints(self):
    return None

  def with_input_types(self, *arg_hints, **kwarg_hints):
    # type: (WithTypeHintsT, *Any, **Any) -> WithTypeHintsT
    arg_hints = native_type_compatibility.convert_to_beam_types(arg_hints)
    kwarg_hints = native_type_compatibility.convert_to_beam_types(kwarg_hints)
    self._type_hints = self._get_or_create_type_hints().with_input_types(
        *arg_hints, **kwarg_hints)
    return self

  def with_output_types(self, *arg_hints, **kwarg_hints):
    # type: (WithTypeHintsT, *Any, **Any) -> WithTypeHintsT
    arg_hints = native_type_compatibility.convert_to_beam_types(arg_hints)
    kwarg_hints = native_type_compatibility.convert_to_beam_types(kwarg_hints)
    self._type_hints = self._get_or_create_type_hints().with_output_types(
        *arg_hints, **kwarg_hints)
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

  For example, if arg is [[a, b], c] and hint is Tuple[Any, int], then
  this function would return ((Any, Any), int) so it can be used in conjunction
  with inspect.getcallargs.
  """
  if isinstance(arg, list):
    tuple_constraint = typehints.Tuple[[typehints.Any] * len(arg)]
    if not typehints.is_consistent_with(hint, tuple_constraint):
      raise TypeCheckError(
          'Bad tuple arguments for %s: expected %s, got %s' %
          (arg, tuple_constraint, hint))
    if isinstance(hint, typehints.TupleConstraint):
      return tuple(
          _unpack_positional_arg_hints(a, t) for a,
          t in zip(arg, hint.tuple_types))
    return (typehints.Any, ) * len(arg)
  return hint


def getcallargs_forhints(func, *typeargs, **typekwargs):
  """Like inspect.getcallargs, with support for declaring default args as Any.

  In Python 2, understands that Tuple[] and an Any unpack.

  Returns:
    (Dict[str, Any]) A dictionary from arguments names to values.
  """
  if sys.version_info < (3, ):
    return getcallargs_forhints_impl_py2(func, typeargs, typekwargs)
  else:
    return getcallargs_forhints_impl_py3(func, typeargs, typekwargs)


def getcallargs_forhints_impl_py2(func, typeargs, typekwargs):
  argspec = getfullargspec(func)
  # Turn Tuple[x, y] into (x, y) so getcallargs can do the proper unpacking.
  packed_typeargs = [
      _unpack_positional_arg_hints(arg, hint)
      for (arg, hint) in zip(argspec.args, typeargs)
  ]
  packed_typeargs += list(typeargs[len(packed_typeargs):])

  # Monkeypatch inspect.getfullargspec to allow passing non-function objects.
  # getfullargspec (getargspec on Python 2) are used by inspect.getcallargs.
  # TODO(BEAM-5490): Reimplement getcallargs and stop relying on monkeypatch.
  inspect.getargspec = getfullargspec
  try:
    callargs = inspect.getcallargs(func, *packed_typeargs, **typekwargs)  # pylint: disable=deprecated-method
  except TypeError as e:
    raise TypeCheckError(e)
  finally:
    # Revert monkey-patch.
    inspect.getargspec = _original_getfullargspec

  if argspec.defaults:
    # Declare any default arguments to be Any.
    for k, var in enumerate(reversed(argspec.args)):
      if k >= len(argspec.defaults):
        break
      if callargs.get(var, None) is argspec.defaults[-k - 1]:
        callargs[var] = typehints.Any
  # Patch up varargs and keywords
  if argspec.varargs:
    # TODO(BEAM-8122): This will always assign _ANY_VAR_POSITIONAL. Should be
    #   "callargs.get(...) or _ANY_VAR_POSITIONAL".
    callargs[argspec.varargs] = typekwargs.get(
        argspec.varargs, _ANY_VAR_POSITIONAL)

  varkw = argspec.keywords
  if varkw:
    # TODO(robertwb): Consider taking the union of key and value types.
    callargs[varkw] = typekwargs.get(varkw, _ANY_VAR_KEYWORD)

  # TODO(BEAM-5878) Support kwonlyargs.

  return callargs


def _normalize_var_positional_hint(hint):
  """Converts a var_positional hint into Tuple[Union[<types>], ...] form.

  Args:
    hint: (tuple) Should be either a tuple of one or more types, or a single
      Tuple[<type>, ...].

  Raises:
    TypeCheckError if hint does not have the right form.
  """
  if not hint or type(hint) != tuple:
    raise TypeCheckError('Unexpected VAR_POSITIONAL value: %s' % hint)

  if len(hint) == 1 and isinstance(hint[0], typehints.TupleSequenceConstraint):
    # Example: tuple(Tuple[Any, ...]) -> Tuple[Any, ...]
    return hint[0]
  else:
    # Example: tuple(int, str) -> Tuple[Union[int, str], ...]
    return typehints.Tuple[typehints.Union[hint], ...]


def _normalize_var_keyword_hint(hint, arg_name):
  """Converts a var_keyword hint into Dict[<key type>, <value type>] form.

  Args:
    hint: (dict) Should either contain a pair (arg_name,
      Dict[<key type>, <value type>]), or one or more possible types for the
      value.
    arg_name: (str) The keyword receiving this hint.

  Raises:
    TypeCheckError if hint does not have the right form.
  """
  if not hint or type(hint) != dict:
    raise TypeCheckError('Unexpected VAR_KEYWORD value: %s' % hint)
  keys = list(hint.keys())
  values = list(hint.values())
  if (len(values) == 1 and keys[0] == arg_name and
      isinstance(values[0], typehints.DictConstraint)):
    # Example: dict(kwargs=Dict[str, Any]) -> Dict[str, Any]
    return values[0]
  else:
    # Example: dict(k1=str, k2=int) -> Dict[str, Union[str,int]]
    return typehints.Dict[str, typehints.Union[values]]


def getcallargs_forhints_impl_py3(func, type_args, type_kwargs):
  """Bind type_args and type_kwargs to func.

  Works like inspect.getcallargs, with some modifications to support type hint
  checks.
  For unbound args, will use annotations and fall back to Any (or variants of
  Any).

  Returns:
    A mapping from parameter name to argument.
  """
  try:
    signature = get_signature(func)
  except ValueError as e:
    logging.warning('Could not get signature for function: %s: %s', func, e)
    return {}
  try:
    bindings = signature.bind(*type_args, **type_kwargs)
  except TypeError as e:
    # Might be raised due to too few or too many arguments.
    raise TypeCheckError(e)
  bound_args = bindings.arguments
  for param in signature.parameters.values():
    if param.name in bound_args:
      # Bound: unpack/convert variadic arguments.
      if param.kind == param.VAR_POSITIONAL:
        bound_args[param.name] = _normalize_var_positional_hint(
            bound_args[param.name])
      elif param.kind == param.VAR_KEYWORD:
        bound_args[param.name] = _normalize_var_keyword_hint(
            bound_args[param.name], param.name)
    else:
      # Unbound: must have a default or be variadic.
      if param.annotation != param.empty:
        bound_args[param.name] = param.annotation
      elif param.kind == param.VAR_POSITIONAL:
        bound_args[param.name] = _ANY_VAR_POSITIONAL
      elif param.kind == param.VAR_KEYWORD:
        bound_args[param.name] = _ANY_VAR_KEYWORD
      elif param.default is not param.empty:
        # Declare unbound parameters with defaults to be Any.
        bound_args[param.name] = typehints.Any
      else:
        # This case should be caught by signature.bind() above.
        raise ValueError('Unexpected unbound parameter: %s' % param.name)

  return dict(bound_args)


def get_type_hints(fn):
  # type: (Any) -> IOTypeHints

  """Gets the type hint associated with an arbitrary object fn.

  Always returns a valid IOTypeHints object, creating one if necessary.
  """
  # pylint: disable=protected-access
  if not hasattr(fn, '_type_hints'):
    try:
      fn._type_hints = IOTypeHints.empty()
    except (AttributeError, TypeError):
      # Can't add arbitrary attributes to this object,
      # but might have some restrictions anyways...
      hints = IOTypeHints.empty()
      # Python 3.7 introduces annotations for _MethodDescriptorTypes.
      if isinstance(fn, _MethodDescriptorType) and sys.version_info < (3, 7):
        hints = hints.with_input_types(fn.__objclass__)  # type: ignore
      return hints
  return fn._type_hints
  # pylint: enable=protected-access


def with_input_types(*positional_hints, **keyword_hints):
  # type: (*Any, **Any) -> Callable[[T], T]

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
    :class:`ValueError`: If not all function arguments have
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

  def annotate_input_types(f):
    if isinstance(f, types.FunctionType):
      for t in (list(converted_positional_hints) +
                list(converted_keyword_hints.values())):
        validate_composite_type_param(
            t, error_msg_prefix='All type hint arguments')

    th = getattr(f, '_type_hints', IOTypeHints.empty()).with_input_types(
        *converted_positional_hints, **converted_keyword_hints)
    f._type_hints = th  # pylint: disable=protected-access
    return f

  return annotate_input_types


def with_output_types(*return_type_hint, **kwargs):
  # type: (*Any, **Any) -> Callable[[T], T]

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
    :class:`ValueError`: If any kwarg parameters are passed in,
      or the length of **return_type_hint** is greater than ``1``. Or if the
      inner wrapper function isn't passed a function object.
    :class:`TypeCheckError`: If the **return_type_hint** object is
      in invalid type-hint.

  Returns:
    The original function decorated such that it enforces type-hint constraints
    for all return values.
  """
  if kwargs:
    raise ValueError(
        "All arguments for the 'returns' decorator must be "
        "positional arguments.")

  if len(return_type_hint) != 1:
    raise ValueError(
        "'returns' accepts only a single positional argument. In "
        "order to specify multiple return types, use the 'Tuple' "
        "type-hint.")

  return_type_hint = native_type_compatibility.convert_to_beam_type(
      return_type_hint[0])

  validate_composite_type_param(
      return_type_hint, error_msg_prefix='All type hint arguments')

  def annotate_output_types(f):
    th = getattr(f, '_type_hints', IOTypeHints.empty())
    f._type_hints = th.with_output_types(return_type_hint)  # pylint: disable=protected-access
    return f

  return annotate_output_types


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
    raise TypeCheckError(
        'Type-hint for %s violated. Expected an '
        'instance of %s, instead found %san instance of %s.' %
        (hint_type, type_constraint, verbose_instance, type(instance)))
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
        gen, lambda x: _check_instance_type(type_constraint, x, var_name))

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
    for x in self.internal_gen:
      self.interleave_func(x)
      yield x
