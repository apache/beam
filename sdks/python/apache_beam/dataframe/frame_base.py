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

import functools
import operator
import re
from inspect import cleandoc
from inspect import getfullargspec
from inspect import isclass
from inspect import ismodule
from inspect import unwrap
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import pandas as pd

from apache_beam.dataframe import expressions
from apache_beam.dataframe import partitionings


class DeferredBase(object):

  _pandas_type_map: Dict[Union[type, None], type] = {}

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
            requires_partition_by=partitionings.Arbitrary(),
            preserves_partition_by=partitionings.Singleton())

      return tuple(cls.wrap(get(ix)) for ix in range(len(expr.proxy())))
    elif proxy_type in cls._pandas_type_map:
      wrapper_type = cls._pandas_type_map[proxy_type]
    else:
      if expr.requires_partition_by() != partitionings.Singleton():
        raise ValueError(
            'Scalar expression %s of type %s partitoned by non-singleton %s' %
            (expr, proxy_type, expr.requires_partition_by()))
      wrapper_type = _DeferredScalar
    return wrapper_type(expr)

  def _elementwise(
      self, func, name=None, other_args=(), other_kwargs=None, inplace=False):
    other_kwargs = other_kwargs or {}
    return _elementwise_function(
        func, name, inplace=inplace)(self, *other_args, **other_kwargs)

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
  pass


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

  def __neg__(self):
    return self.apply(operator.neg)

  def __pos__(self):
    return self.apply(operator.pos)

  def __invert__(self):
    return self.apply(operator.invert)

  def __repr__(self):
    return f"DeferredScalar[type={type(self._expr.proxy())}]"

  def __bool__(self):
    # TODO(BEAM-11951): Link to documentation
    raise TypeError(
        "Testing the truth value of a deferred scalar is not "
        "allowed. It's not possible to branch on the result of "
        "deferred operations.")


def _scalar_binop(op):
  def binop(self, other):
    if not isinstance(other, DeferredBase):
      return self.apply(lambda left: getattr(left, op)(other), name=op)
    elif isinstance(other, _DeferredScalar):
      return self.apply(
          lambda left, right: getattr(left, op)(right), name=op, args=[other])
    else:
      return NotImplemented

  return binop


for op in ['__add__',
           '__sub__',
           '__mul__',
           '__div__',
           '__truediv__',
           '__floordiv__',
           '__mod__',
           '__divmod__',
           '__pow__',
           '__and__',
           '__or__']:
  setattr(_DeferredScalar, op, _scalar_binop(op))

DeferredBase._pandas_type_map[None] = _DeferredScalar


def name_and_func(method: Union[str, Callable]) -> Tuple[str, Callable]:
  """For the given method name or method, return the method name and the method
  itself.

  For internal use only. No backwards compatibility guarantees."""
  if isinstance(method, str):
    method_str = method
    func = lambda df, *args, **kwargs: getattr(df, method_str)(*args, **kwargs)
    return method, func
  else:
    return method.__name__, method


def _elementwise_method(
    func, name=None, restrictions=None, inplace=False, base=None):
  return _proxy_method(
      func,
      name,
      restrictions,
      inplace,
      base,
      requires_partition_by=partitionings.Arbitrary(),
      preserves_partition_by=partitionings.Arbitrary())


def _proxy_method(
    func,
    name=None,
    restrictions=None,
    inplace=False,
    base=None,
    *,
    requires_partition_by: partitionings.Partitioning,
    preserves_partition_by: partitionings.Partitioning,
):
  if name is None:
    name, func = name_and_func(func)
  if base is None:
    raise ValueError("base is required for _proxy_method")
  return _proxy_function(
      func,
      name,
      restrictions,
      inplace,
      base,
      requires_partition_by=requires_partition_by,
      preserves_partition_by=preserves_partition_by)


def _elementwise_function(
    func, name=None, restrictions=None, inplace=False, base=None):
  return _proxy_function(
      func,
      name,
      restrictions,
      inplace,
      base,
      requires_partition_by=partitionings.Arbitrary(),
      preserves_partition_by=partitionings.Arbitrary())


def _proxy_function(
    func: Union[Callable, str],
    name: Optional[str] = None,
    restrictions: Optional[Dict[str, Union[Any, List[Any]]]] = None,
    inplace: bool = False,
    base: Optional[type] = None,
    *,
    requires_partition_by: partitionings.Partitioning,
    preserves_partition_by: partitionings.Partitioning,
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
                requires_partition_by=partitionings.Arbitrary()))
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
      actual_func = _copy_and_mutate(func)
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

    if (requires_partition_by.is_subpartitioning_of(partitionings.Index()) and
        sum(isinstance(arg.proxy(), pd.core.generic.NDFrame)
            for arg in deferred_exprs) > 1):
      # Implicit join on index if there is more than one indexed input.
      actual_requires_partition_by = partitionings.JoinIndex()
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

  wrapper.__name__ = name
  if restrictions:
    wrapper.__doc__ = "\n".join(
        f"Only {kw}={value!r} is supported"
        for (kw, value) in restrictions.items())

  if base is not None:
    return with_docs_from(base)(wrapper)
  else:
    return wrapper


def _prettify_pandas_type(pandas_type):
  if pandas_type in (pd.DataFrame, pd.Series):
    return f'pandas.{pandas_type.__name__}'
  elif isclass(pandas_type):
    return f'{pandas_type.__module__}.{pandas_type.__name__}'
  elif ismodule(pandas_type):
    return pandas_type.__name__
  else:
    raise TypeError(pandas_type)


def wont_implement_method(base_type, name, reason=None, explanation=None):
  """Generate a stub method that raises WontImplementError.

  Note either reason or explanation must be specified. If both are specified,
  explanation is ignored.

  Args:
      base_type: The pandas type of the method that this is trying to replicate.
      name: The name of the method that this is aiming to replicate.
      reason: If specified, use data from the corresponding entry in
           ``_WONT_IMPLEMENT_REASONS`` to generate a helpful exception message
           and docstring for the method.
      explanation: If specified, use this string as an explanation for why
           this operation is not supported when generating an exception message
           and docstring.
  """
  if reason is not None:
    if reason not in _WONT_IMPLEMENT_REASONS:
      raise AssertionError(
          f"reason must be one of {list(_WONT_IMPLEMENT_REASONS.keys())}, "
          f"got {reason!r}")
    reason_data = _WONT_IMPLEMENT_REASONS[reason]
  elif explanation is not None:
    reason_data = {'explanation': explanation}
  else:
    raise ValueError("One of (reason, explanation) must be specified")

  def wrapper(*args, **kwargs):
    raise WontImplementError(
        f"'{name}' is not yet supported {reason_data['explanation']}",
        reason=reason)

  wrapper.__name__ = name
  wrapper.__doc__ = (
      f":meth:`{_prettify_pandas_type(base_type)}.{name}` is not yet supported "
      f"in the Beam DataFrame API {reason_data['explanation']}")

  if 'url' in reason_data:
    wrapper.__doc__ += f"\n\n For more information see {reason_data['url']}."

  return wrapper


def not_implemented_method(op, issue='20318', base_type=None):
  """Generate a stub method for ``op`` that simply raises a NotImplementedError.

  For internal use only. No backwards compatibility guarantees."""
  assert base_type is not None, "base_type must be specified"
  issue_url = f"https://issues.apache.org/jira/{issue}." if issue.startswith(
      "BEAM-") else f"https://github.com/apache/beam/issues/{issue}"

  def wrapper(*args, **kwargs):
    raise NotImplementedError(
        f"{op!r} is not implemented yet. "
        f"If support for {op!r} is important to you, please let the Beam "
        "community know by writing to user@beam.apache.org "
        "(see https://beam.apache.org/community/contact-us/) or commenting on "
        f"{issue_url}")

  wrapper.__name__ = op
  wrapper.__doc__ = (
      f":meth:`{_prettify_pandas_type(base_type)}.{op}` is not implemented yet "
      "in the Beam DataFrame API.\n\n"
      f"If support for {op!r} is important to you, please let the Beam "
      "community know by `writing to user@beam.apache.org "
      "<https://beam.apache.org/community/contact-us/>`_ or commenting on "
      f"`{issue} <{issue_url}>`_.")

  return wrapper


def _copy_and_mutate(func):
  def wrapper(self, *args, **kwargs):
    copy = self.copy()
    func(copy, *args, **kwargs)
    return copy

  return wrapper


def maybe_inplace(func):
  """Handles the inplace= kwarg available in many pandas operations.

  This decorator produces a new function handles the inplace kwarg. When
  `inplace=False`, the new function simply yields the result of `func`
  directly.

  When `inplace=True`, the output of `func` is used to replace this instances
  expression. The result is that any operations applied to this instance after
  the inplace operation will refernce the updated expression.

  For internal use only. No backwards compatibility guarantees."""
  @functools.wraps(func)
  def wrapper(self, inplace=False, **kwargs):
    result = func(self, **kwargs)
    if inplace:
      self._expr = result._expr
    else:
      return result

  return wrapper


def args_to_kwargs(base_type, removed_method=False, removed_args=None):
  """Convert all args to kwargs before calling the decorated function.

  When applied to a function, this decorator creates a new function
  that always calls the wrapped function with *only* keyword arguments. It
  inspects the argspec for the identically-named method on `base_type` to
  determine the name to use for arguments that are converted to keyword
  arguments.

  For internal use only. No backwards compatibility guarantees.

  Args:
      base_type: The pandas type of the method that this is trying to replicate.
      removed_method: Whether this method has been removed in the running
           Pandas version.
      removed_args: If not empty, which arguments have been dropped in the
           running Pandas version.
  """
  def wrap(func):
    if removed_method:
      # Do no processing, let Beam function itself raise the error if called.
      return func

    removed_arg_names = removed_args if removed_args is not None else []

    # We would need to add position only arguments if they ever become a thing
    # in Pandas (as of 2.1 currently they aren't).
    base_arg_spec = getfullargspec(unwrap(getattr(base_type, func.__name__)))
    base_arg_names = base_arg_spec.args
    # Some arguments are keyword only and we still want to check against those.
    all_possible_base_arg_names = base_arg_names + base_arg_spec.kwonlyargs
    beam_arg_names = getfullargspec(func).args

    if not_found := (set(beam_arg_names) - set(all_possible_base_arg_names) -
                     set(removed_arg_names)):
      raise TypeError(
          f"Beam definition of {func.__name__} has arguments that are not found"
          f" in the base version of the function: {not_found}")

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
      if len(args) > len(base_arg_names):
        raise TypeError(f"{func.__name__} got too many positioned arguments.")

      for name, value in zip(base_arg_names, args):
        if name in kwargs:
          raise TypeError(
              "%s() got multiple values for argument '%s'" %
              (func.__name__, name))
        kwargs[name] = value
      # Still have to populate these for the Beam function signature.
      if removed_args:
        for name in removed_args:
          if name not in kwargs:
            kwargs[name] = None
      return func(**kwargs)

    return wrapper

  return wrap


BEAM_SPECIFIC = "Differences from pandas"

SECTION_ORDER = [
    'Parameters',
    'Returns',
    'Raises',
    BEAM_SPECIFIC,
    'See Also',
    'Notes',
    'Examples'
]

EXAMPLES_DISCLAIMER = (
    "**NOTE:** These examples are pulled directly from the pandas "
    "documentation for convenience. Usage of the Beam DataFrame API will look "
    "different because it is a deferred API.")
EXAMPLES_DIFFERENCES = EXAMPLES_DISCLAIMER + (
    " In addition, some arguments shown here may not be supported, see "
    f"**{BEAM_SPECIFIC!r}** for details.")


def with_docs_from(base_type, name=None, removed_method=False):
  """Decorator that updates the documentation from the wrapped function to
  duplicate the documentation from the identically-named method in `base_type`.

  Any docstring on the original function will be included in the new function
  under a "Differences from pandas" heading.

  removed_method used in cases where a method has been removed in a later
  version of Pandas.
  """
  def wrap(func):
    if removed_method:
      func.__doc__ = (
          "This method has been removed in the current version of Pandas.")
      return func

    fn_name = name or func.__name__
    orig_doc = getattr(base_type, fn_name).__doc__
    if orig_doc is None:
      return func

    orig_doc = cleandoc(orig_doc)

    section_splits = re.split(r'^(.*)$\n^-+$\n', orig_doc, flags=re.MULTILINE)
    intro = section_splits[0].strip()
    sections = dict(zip(section_splits[1::2], section_splits[2::2]))

    beam_has_differences = bool(func.__doc__)

    for header, content in sections.items():
      content = content.strip()

      # Replace references to version numbers so its clear they reference
      # *pandas* versions
      content = re.sub(r'([Vv]ersion\s+[\d\.]+)', r'pandas \1', content)

      if header == "Examples":
        content = '\n\n'.join([
            (
                EXAMPLES_DIFFERENCES
                if beam_has_differences else EXAMPLES_DISCLAIMER),
            # Indent the examples under a doctest heading,
            # add skipif option. This makes sure our doctest
            # framework doesn't run these pandas tests.
            (".. doctest::\n"
             "    :skipif: True"),
            re.sub(r"^", "    ", content, flags=re.MULTILINE),
        ])
      else:
        content = content.replace('DataFrame', 'DeferredDataFrame').replace(
            'Series', 'DeferredSeries')
      sections[header] = content

    if beam_has_differences:
      sections[BEAM_SPECIFIC] = cleandoc(func.__doc__)
    else:
      sections[BEAM_SPECIFIC] = (
          "This operation has no known divergences from the "
          "pandas API.")

    def format_section(header):
      return '\n'.join([header, ''.join('-' for _ in header), sections[header]])

    func.__doc__ = '\n\n'.join([intro] + [
        format_section(header) for header in SECTION_ORDER if header in sections
    ])

    return func

  return wrap


def populate_defaults(base_type, removed_method=False, removed_args=None):
  """Populate default values for keyword arguments in decorated function.

  When applied to a function, this decorator creates a new function
  with default values for all keyword arguments, based on the default values
  for the identically-named method on `base_type`.

  For internal use only. No backwards compatibility guarantees.

  Args:
      base_type: The pandas type of the method that this is trying to replicate.
      removed_method: Whether this method has been removed in the running
           Pandas version.
      removed_args: If not empty, which arguments have been dropped in the
           running Pandas version.
  """
  def wrap(func):
    if removed_method:
      return func

    base_argspec = getfullargspec(unwrap(getattr(base_type, func.__name__)))
    if not base_argspec.defaults and not base_argspec.kwonlydefaults:
      return func

    arg_to_default = {}
    if base_argspec.defaults:
      arg_to_default.update(
          zip(
              base_argspec.args[-len(base_argspec.defaults):],
              base_argspec.defaults))

    if base_argspec.kwonlydefaults:
      arg_to_default.update(base_argspec.kwonlydefaults)

    unwrapped_func = unwrap(func)
    # args that do not have defaults in func, but do have defaults in base
    func_argspec = getfullargspec(unwrapped_func)
    num_non_defaults = len(func_argspec.args) - len(func_argspec.defaults or ())
    defaults_to_populate = set(
        func_argspec.args[:num_non_defaults]).intersection(
            arg_to_default.keys())
    if removed_args:
      defaults_to_populate -= set(removed_args)

    # In pandas 2, many methods rely on the default copy=None
    # to mean that copy is the value of copy_on_write. Since
    # copy_on_write will always be true for Beam, just fill it
    # in here. In pandas 1, the default was True anyway.
    if 'copy' in arg_to_default and arg_to_default['copy'] is None:
      arg_to_default['copy'] = True

    @functools.wraps(func)
    def wrapper(**kwargs):
      for name in defaults_to_populate:
        if name not in kwargs:
          kwargs[name] = arg_to_default[name]

      return func(**kwargs)

    return wrapper

  return wrap


_WONT_IMPLEMENT_REASONS = {
    'order-sensitive': {
        'explanation': "because it is sensitive to the order of the data.",
        'url': 'https://s.apache.org/dataframe-order-sensitive-operations',
    },
    'non-deferred-columns': {
        'explanation': (
            "because the columns in the output DataFrame depend "
            "on the data."),
        'url': 'https://s.apache.org/dataframe-non-deferred-columns',
    },
    'non-deferred-result': {
        'explanation': (
            "because it produces an output type that is not "
            "deferred."),
        'url': 'https://s.apache.org/dataframe-non-deferred-result',
    },
    'plotting-tools': {
        'explanation': "because it is a plotting tool.",
        'url': 'https://s.apache.org/dataframe-plotting-tools',
    },
    'event-time-semantics': {
        'explanation': (
            "because implementing it would require integrating with Beam "
            "event-time semantics"),
        'url': 'https://s.apache.org/dataframe-event-time-semantics',
    },
    'deprecated': {
        'explanation': "because it is deprecated in pandas.",
    },
    'experimental': {
        'explanation': "because it is experimental in pandas.",
    },
}


class WontImplementError(NotImplementedError):
  """An subclass of NotImplementedError to raise indicating that implementing
  the given method is not planned.

  Raising this error will also prevent this doctests from being validated
  when run with the beam dataframe validation doctest runner.
  """
  def __init__(self, msg, reason=None):
    if reason is not None:
      if reason not in _WONT_IMPLEMENT_REASONS:
        raise AssertionError(
            f"reason must be one of {list(_WONT_IMPLEMENT_REASONS.keys())}, "
            f"got {reason!r}")

      reason_data = _WONT_IMPLEMENT_REASONS[reason]
      if 'url' in reason_data:
        msg = f"{msg}\nFor more information see {reason_data['url']}."

    super().__init__(msg)
