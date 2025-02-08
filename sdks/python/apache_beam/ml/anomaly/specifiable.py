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
from typing import Any
from typing import ClassVar
from typing import List
from typing import Protocol
from typing import Type
from typing import TypeVar
from typing import runtime_checkable

from typing_extensions import Self

__all__ = ["Spec", "Specifiable", "specifiable"]

_FALLBACK_SUBSPACE = "*"

_ACCEPTED_SUBSPACES = [
    "EnsembleAnomalyDetector",
    "AnomalyDetector",
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

SpecT = TypeVar('SpecT', bound='Specifiable')


def _class_to_subspace(cls: Type) -> str:
  """
  Search the class hierarchy to find the subspace: the closest ancestor class in
  the class's method resolution order (MRO) whose name is found in the accepted
  subspace list. This is usually called when registering a new specifiable
  class.
  """
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

  raise ValueError(f"subspace for {str} not found.")


@dataclasses.dataclass(frozen=True)
class Spec():
  """
  Dataclass for storing specifications of specifiable objects.
  Objects can be initialized using the data in their corresponding spec.
  """
  #: A string indicating the concrete `Specifiable` class
  type: str
  #: A dictionary of keyword arguments for the `__init__` method of the class.
  config: dict[str, Any] = dataclasses.field(default_factory=dict)


@runtime_checkable
class Specifiable(Protocol):
  """Protocol that a specifiable class needs to implement."""
  #: The value of the `type` field in the object's spec for this class.
  spec_type: ClassVar[str]
  #: The raw keyword arguments passed to `__init__` method during object
  #: initialization.
  init_kwargs: dict[str, Any]

  # a boolean to tell whether the original `__init__` method is called
  _initialized: bool
  # a boolean used by new_getattr to tell whether it is in the `__init__` method
  # call
  _in_init: bool

  @staticmethod
  def _from_spec_helper(v, _run_init):
    if isinstance(v, Spec):
      return Specifiable.from_spec(v, _run_init)

    if isinstance(v, List):
      return [Specifiable._from_spec_helper(e, _run_init) for e in v]

    return v

  @classmethod
  def from_spec(cls, spec: Spec, _run_init: bool = True) -> Self:
    """Generate a `Specifiable` subclass object based on a spec."""
    if spec.type is None:
      raise ValueError(f"Spec type not found in {spec}")

    subspace = _spec_type_to_subspace(spec.type)
    subclass: Type[Self] = _KNOWN_SPECIFIABLE[subspace].get(spec.type, None)
    if subclass is None:
      raise ValueError(f"Unknown spec type '{spec.type}' in {spec}")

    kwargs = {
        k: Specifiable._from_spec_helper(v, _run_init)
        for k,
        v in spec.config.items()
    }

    if _run_init:
      kwargs["_run_init"] = True
    return subclass(**kwargs)

  @staticmethod
  def _to_spec_helper(v):
    if isinstance(v, Specifiable):
      return v.to_spec()

    if isinstance(v, List):
      return [Specifiable._to_spec_helper(e) for e in v]

    return v

  def to_spec(self) -> Spec:
    """Generate a spec from a `Specifiable` subclass object."""
    if getattr(type(self), 'spec_type', None) is None:
      raise ValueError(
          f"'{type(self).__name__}' not registered as Specifiable. "
          f"Decorate ({type(self).__name__}) with @specifiable")

    args = {k: self._to_spec_helper(v) for k, v in self.init_kwargs.items()}

    return Spec(type=self.__class__.spec_type, config=args)

  def run_original_init(self) -> None:
    """Invoke the original __init__ method with original keyword arguments"""
    pass


# Register a `Specifiable` subclass in `KNOWN_SPECIFIABLE`
def _register(cls, spec_type=None) -> None:
  if spec_type is None:
    # By default, spec type is the class name. Users can override this with
    # other unique identifier.
    spec_type = cls.__name__

  subspace = _class_to_subspace(cls)
  if spec_type in _KNOWN_SPECIFIABLE[subspace]:
    raise ValueError(
        f"{spec_type} is already registered for "
        f"specifiable class {_KNOWN_SPECIFIABLE[subspace][spec_type]}. "
        "Please specify a different spec_type by @specifiable(spec_type=...).")
  else:
    _KNOWN_SPECIFIABLE[subspace][spec_type] = cls

  cls.spec_type = spec_type


# Keep a copy of arguments that are used to call the `__init__` method when the
# object is initialized.
def _get_init_kwargs(inst, init_method, *args, **kwargs):
  params = dict(
      zip(inspect.signature(init_method).parameters.keys(), (None, ) + args))
  del params['self']
  params.update(**kwargs)
  return params


def specifiable(
    my_cls=None,
    /,
    *,
    spec_type=None,
    on_demand_init=True,
    just_in_time_init=True):
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
  def _wrapper(cls):
    def new_init(self: Specifiable, *args, **kwargs):
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

    def run_original_init(self):
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

    # start of the function body of _wrapper
    _register(cls, spec_type)

    class_name = cls.__name__
    original_init = cls.__init__
    cls.__init__ = new_init
    if just_in_time_init:
      cls.__getattr__ = new_getattr

    cls.run_original_init = run_original_init
    cls.to_spec = Specifiable.to_spec
    cls._to_spec_helper = staticmethod(Specifiable._to_spec_helper)
    cls.from_spec = classmethod(Specifiable.from_spec)
    cls._from_spec_helper = staticmethod(Specifiable._from_spec_helper)
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
