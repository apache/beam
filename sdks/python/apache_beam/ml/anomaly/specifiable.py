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

"""
A module that provides utilities to turn a class into a Specifiable subclass.
"""

from __future__ import annotations

import collections
import dataclasses
import inspect
import logging
import os
from collections.abc import Callable
from typing import Any
from typing import Optional
from typing import Protocol
from typing import TypeVar
from typing import Union
from typing import overload
from typing import runtime_checkable

from typing_extensions import Self

__all__ = ["Spec", "Specifiable", "specifiable"]

_FALLBACK_SUBSPACE = "*"

_ACCEPTED_SUBSPACES = [
    "EnsembleAnomalyDetector",
    "AnomalyDetector",
    "BaseTracker",
    "ThresholdFn",
    "AggregationFn",
    _FALLBACK_SUBSPACE,
]

#: A nested dictionary for efficient lookup of Specifiable subclasses.
#: Structure: `_KNOWN_SPECIFIABLE[subspace][spec_type]`, where `subspace` is one
#: of the accepted subspaces that the class belongs to and `spec_type` is the
#: class name by default. Users can also specify a different value for
#: `spec_type` when applying the `specifiable` decorator to an existing class.
_KNOWN_SPECIFIABLE = collections.defaultdict(dict)

T = TypeVar('T', bound=type)
BUILTIN_TYPES_IN_SPEC = (int, float, complex, str, bytes, bytearray)


def _class_to_subspace(cls: type) -> str:
  """
  Search the class hierarchy to find the subspace: the closest ancestor class in
  the class's method resolution order (MRO) whose name is found in the accepted
  subspace list. This is usually called when registering a new specifiable
  class.
  """
  if hasattr(cls, "mro"):
    # some classes do not have "mro", such as functions.
    for c in cls.mro():
      if c.__name__ in _ACCEPTED_SUBSPACES:
        return c.__name__

  return _FALLBACK_SUBSPACE


def _spec_type_to_subspace(spec_type: str) -> str:
  """
  Look for the subspace for a spec type. This is usually called to retrieve
  the subspace of a registered specifiable class.
  """
  for subspace in _ACCEPTED_SUBSPACES:
    if spec_type in _KNOWN_SPECIFIABLE[subspace]:
      return subspace

  raise ValueError(f"subspace for {spec_type} not found.")


@dataclasses.dataclass(frozen=True)
class Spec():
  """
  Dataclass for storing specifications of specifiable objects.
  Objects can be initialized using the data in their corresponding spec.
  """
  #: A string indicating the concrete `Specifiable` class
  type: str
  #: An optional dictionary of keyword arguments for the `__init__` method of
  #: the class. If None, when we materialize this Spec, we only return the
  #: class without instantiate any objects from it.
  config: Optional[dict[str, Any]] = dataclasses.field(default_factory=dict)


def _specifiable_from_spec_helper(v, _run_init):
  if isinstance(v, Spec):
    return Specifiable.from_spec(v, _run_init)

  if isinstance(v, list):
    return [_specifiable_from_spec_helper(e, _run_init) for e in v]

  # TODO: support spec treatment for more types
  if not isinstance(v, BUILTIN_TYPES_IN_SPEC):
    logging.warning(
        "Type %s is not a recognized supported type for the "
        "specification. It will be included without conversion.",
        str(type(v)))
  return v


def _specifiable_to_spec_helper(v):
  if isinstance(v, Specifiable):
    return v.to_spec()

  if isinstance(v, list):
    return [_specifiable_to_spec_helper(e) for e in v]

  if inspect.isfunction(v):
    if not hasattr(v, "spec_type"):
      _register(v, inject_spec_type=False)
    return Spec(type=_get_default_spec_type(v), config=None)

  if inspect.isclass(v):
    if not hasattr(v, "spec_type"):
      _register(v, inject_spec_type=False)
    return Spec(type=_get_default_spec_type(v), config=None)

  # TODO: support spec treatment for more types
  if not isinstance(v, BUILTIN_TYPES_IN_SPEC):
    logging.warning(
        "Type %s is not a recognized supported type for the "
        "specification. It will be included without conversion.",
        str(type(v)))
  return v


@runtime_checkable
class Specifiable(Protocol):
  """Protocol that a specifiable class needs to implement."""
  @classmethod
  def spec_type(cls) -> str:
    pass

  @classmethod
  def from_spec(cls,
                spec: Spec,
                _run_init: bool = True) -> Union[Self, type[Self]]:
    """Generate a `Specifiable` subclass object based on a spec.

    Args:
      spec: the specification of a `Specifiable` subclass object
      _run_init: whether to call `__init__` or not for the initial instantiation

    Returns:
      Self: the `Specifiable` subclass object
    """
    if spec.type is None:
      raise ValueError(f"Spec type not found in {spec}")

    subspace = _spec_type_to_subspace(spec.type)
    subclass: type[Self] = _KNOWN_SPECIFIABLE[subspace].get(spec.type, None)

    if subclass is None:
      raise ValueError(f"Unknown spec type '{spec.type}' in {spec}")

    if spec.config is None:
      # when functions or classes are used as arguments, we won't try to
      # create an instance.
      return subclass

    kwargs = {
        k: _specifiable_from_spec_helper(v, _run_init)
        for k, v in spec.config.items()
    }

    if _run_init:
      kwargs["_run_init"] = True
    return subclass(**kwargs)

  def to_spec(self) -> Spec:
    """Generate a spec from a `Specifiable` subclass object.

    Returns:
      Spec: The specification of the instance.
    """
    if getattr(type(self), 'spec_type', None) is None:
      raise ValueError(
          f"'{type(self).__name__}' not registered as Specifiable. "
          f"Decorate ({type(self).__name__}) with @specifiable")

    args = {
        k: _specifiable_to_spec_helper(v)
        for k, v in self.init_kwargs.items()
    }

    return Spec(type=self.spec_type(), config=args)

  def run_original_init(self) -> None:
    """Invoke the original __init__ method with original keyword arguments"""
    pass

  @classmethod
  def unspecifiable(cls) -> None:
    """Resume the class structure prior to specifiable"""
    pass


def _get_default_spec_type(cls):
  spec_type = cls.__name__
  if inspect.isfunction(cls) and cls.__name__ == "<lambda>":
    # for lambda functions, we need to include more information to distinguish
    # among them
    spec_type = '<lambda at %s:%s>' % (
        os.path.basename(cls.__code__.co_filename), cls.__code__.co_firstlineno)

  return spec_type


# Register a `Specifiable` subclass in `KNOWN_SPECIFIABLE`
def _register(cls: type, spec_type=None, inject_spec_type=True) -> None:
  assert spec_type is None or inject_spec_type, \
      "need to inject spec_type to class if spec_type is not None"
  if spec_type is None:
    # Use default spec_type for a class if users do not specify one.
    spec_type = _get_default_spec_type(cls)

  subspace = _class_to_subspace(cls)
  if spec_type in _KNOWN_SPECIFIABLE[subspace]:
    if cls is not _KNOWN_SPECIFIABLE[subspace][spec_type]:
      # only raise exception if we register the same spec type with a different
      # class
      raise ValueError(
          f"{spec_type} is already registered for "
          f"specifiable class {_KNOWN_SPECIFIABLE[subspace][spec_type]}. "
          "Please specify a different spec_type by @specifiable(spec_type=...)."
      )
  else:
    _KNOWN_SPECIFIABLE[subspace][spec_type] = cls

  if inject_spec_type:
    setattr(cls, cls.__name__ + '__spec_type', spec_type)
    # cls.__spec_type = spec_type


# Keep a copy of arguments that are used to call the `__init__` method when the
# object is initialized.
def _get_init_kwargs(inst, init_method, *args, **kwargs):
  params = dict(
      zip(inspect.signature(init_method).parameters.keys(), (None, ) + args))
  del params['self']
  params.update(**kwargs)
  return params


@overload
def specifiable(
    my_cls: None = None,
    /,
    *,
    spec_type: Optional[str] = None,
    on_demand_init: bool = True,
    just_in_time_init: bool = True) -> Callable[[T], T]:
  pass


@overload
def specifiable(
    my_cls: T,
    /,
    *,
    spec_type: Optional[str] = None,
    on_demand_init: bool = True,
    just_in_time_init: bool = True) -> T:
  pass


def specifiable(
    my_cls: Optional[T] = None,
    /,
    *,
    spec_type: Optional[str] = None,
    on_demand_init: bool = True,
    just_in_time_init: bool = True) -> Union[T, Callable[[T], T]]:
  """A decorator that turns a class into a `Specifiable` subclass by
  implementing the `Specifiable` protocol.

  To use the decorator, simply place `@specifiable` before the class
  definition::

    @specifiable
    class Foo():
      ...

  For finer control, the decorator can accept arguments::

    @specifiable(spec_type="My Class", on_demand_init=False)
    class Bar():
      ...

  Args:
    spec_type: The value of the `type` field in the Spec of a `Specifiable`
      subclass. If not provided, the class name is used. This argument is useful
      when registering multiple classes with the same base name; in such cases,
      one can specify `spec_type` to different values to resolve conflict.
    on_demand_init: If True, allow on-demand object initialization. The original
      `__init__` method will be called when `_run_init=True` is passed to the
      object's initialization function.
    just_in_time_init: If True, allow just-in-time object initialization. The
      original `__init__` method will be called when the first time an attribute
      is accessed.
  """
  def _wrapper(cls: T) -> T:
    def new_init(self, *args, **kwargs):
      self._initialized = False
      self._in_init = False

      run_init_request = False
      if "_run_init" in kwargs:
        run_init_request = kwargs["_run_init"]
        del kwargs["_run_init"]

      if 'init_kwargs' not in self.__dict__:
        # If it is a child specifiable (i.e.g init_kwargs not set), we determine
        # whether to skip the original __init__ call based on options:
        # on_demand_init, just_in_time_init and _run_init.
        # Otherwise (i.e. init_kwargs is set), we always call the original
        # __init__ method for ancestor specifiable.
        self.init_kwargs = _get_init_kwargs(
            self, original_init, *args, **kwargs)
        logging.debug("Record init params in %s.new_init", class_name)

        if (on_demand_init and not run_init_request) or \
            (not on_demand_init and just_in_time_init):
          logging.debug("Skip original %s.__init__", class_name)
          return

      logging.debug("Call original %s.__init__ in new_init", class_name)

      original_init(self, *args, **kwargs)
      self._initialized = True

    def run_original_init(self) -> None:
      """Execute the original `__init__` method with its saved arguments.

      For instances of the `Specifiable` class, initialization is deferred
      (lazy initialization). This function forces the execution of the
      original `__init__` method using the arguments captured during
      the object's initial instantiation.
      """
      self._in_init = True
      original_init(self, **self.init_kwargs)
      self._in_init = False
      self._initialized = True

    # __getattr__ is only called when an attribute is not found in the object
    def new_getattr(self, name):
      logging.debug(
          "Trying to access %s.%s, but it is not found.", class_name, name)

      # Fix the infinite loop issue when pickling a Specifiable
      if name in ["_in_init", "__getstate__"] and name not in self.__dict__:
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'")

      # If the attribute is not found during or after initialization, then
      # it is a missing attribute.
      if self._in_init or self._initialized:
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'")

      # Here, we know the object is not initialized, then we will call original
      # init method.
      logging.debug("Call original %s.__init__ in new_getattr", class_name)
      run_original_init(self)

      # __getattribute__ is call for every attribute regardless whether it is
      # present in the object. In this case, we don't cause an infinite loop
      # if the attribute does not exist.
      logging.debug(
          "Call original %s.__getattribute__(%s) in new_getattr",
          class_name,
          name)
      return self.__getattribute__(name)

    def spec_type_func(cls):
      return getattr(cls, spec_type_attr_name)

    def unspecifiable(cls):
      delattr(cls, spec_type_attr_name)
      cls.__init__ = original_init
      if just_in_time_init:
        delattr(cls, '__getattr__')
      delattr(cls, 'spec_type')
      delattr(cls, 'run_original_init')
      delattr(cls, 'to_spec')
      delattr(cls, 'from_spec')
      delattr(cls, 'unspecifiable')

    spec_type_attr_name = cls.__name__ + "__spec_type"

    # the class is registered
    if hasattr(cls, spec_type_attr_name):
      return cls

    # start of the function body of _wrapper
    _register(cls, spec_type)

    class_name = cls.__name__
    original_init = cls.__init__  # type: ignore[misc]
    cls.__init__ = new_init  # type: ignore[misc]
    if just_in_time_init:
      cls.__getattr__ = new_getattr

    cls.spec_type = classmethod(spec_type_func)
    cls.run_original_init = run_original_init
    cls.to_spec = Specifiable.to_spec
    cls.from_spec = Specifiable.from_spec
    cls.unspecifiable = classmethod(unspecifiable)

    return cls
    # end of the function body of _wrapper

  # When this decorator is called with arguments, i.e..
  # "@specifiable(arg1=...,arg2=...)", it is equivalent to assigning
  # specifiable(arg1=..., arg2=...) to a variable, say decor_func, and then
  # calling "@decor_func".
  if my_cls is None:
    return _wrapper

  # When this decorator is called without an argument, i.e. "@specifiable",
  # we return the augmented class.
  return _wrapper(my_cls)
