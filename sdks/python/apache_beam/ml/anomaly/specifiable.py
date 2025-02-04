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

from __future__ import annotations

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

ACCEPTED_SPECIFIABLE_SUBSPACES = [
    "EnsembleAnomalyDetector",
    "AnomalyDetector",
    "ThresholdFn",
    "AggregationFn",
    "*"
]
KNOWN_SPECIFIABLE = {"*": {}}

SpecT = TypeVar('SpecT', bound='Specifiable')


def get_subspace(cls):
  subspace = "*"
  for c in cls.mro():
    if c in ACCEPTED_SPECIFIABLE_SUBSPACES:
      subspace = c.__name__
      break
  return subspace


@dataclasses.dataclass(frozen=True)
class Spec():
  type: str
  config: dict[str, Any] = dataclasses.field(default_factory=dict)


@runtime_checkable
class Specifiable(Protocol):
  _key: ClassVar[str]
  _init_params: dict[str, Any]

  @staticmethod
  def _from_spec_helper(v):
    if isinstance(v, Spec):
      return Specifiable.from_spec(v)

    if isinstance(v, List):
      return [Specifiable._from_spec_helper(e) for e in v]

    return v

  @classmethod
  def from_spec(cls, spec: Spec) -> Self:
    if spec.type is None:
      raise ValueError(f"Spec type not found in {spec}")

    subspace = get_subspace(cls)
    subclass: Type[Self] = KNOWN_SPECIFIABLE[subspace].get(spec.type, None)
    if subclass is None:
      raise ValueError(f"Unknown spec type '{spec.type}' in {spec}")

    args = {k: Specifiable._from_spec_helper(v) for k, v in spec.config.items()}

    return subclass(**args)

  @staticmethod
  def _to_spec_helper(v):
    if isinstance(v, Specifiable):
      return v.to_spec()

    if isinstance(v, List):
      return [Specifiable._to_spec_helper(e) for e in v]

    return v

  def to_spec(self) -> Spec:
    if getattr(type(self), '_key', None) is None:
      raise ValueError(
          f"'{type(self).__name__}' not registered as Specifiable. "
          f"Decorate ({type(self).__name__}) with @specifiable")

    args = {k: self._to_spec_helper(v) for k, v in self._init_params.items()}

    return Spec(type=self.__class__._key, config=args)


def register(cls, key, error_if_exists) -> None:
  if key is None:
    key = cls.__name__

  subspace = get_subspace(cls)
  if subspace in KNOWN_SPECIFIABLE and key in KNOWN_SPECIFIABLE[
      subspace] and error_if_exists:
    raise ValueError(f"{key} is already registered for specifiable")

  if subspace not in KNOWN_SPECIFIABLE:
    KNOWN_SPECIFIABLE[subspace] = {}
  KNOWN_SPECIFIABLE[subspace][key] = cls

  cls._key = key


def track_init_params(inst, init_method, *args, **kwargs):
  params = dict(
      zip(inspect.signature(init_method).parameters.keys(), (None, ) + args))
  del params['self']
  params.update(**kwargs)
  inst._init_params = params


def specifiable(
    my_cls=None,
    /,
    *,
    key=None,
    error_if_exists=True,
    on_demand_init=True,
    just_in_time_init=True):

  # register a specifiable, track init params for each instance, lazy init
  def _wrapper(cls):
    register(cls, key, error_if_exists)

    original_init = cls.__init__
    class_name = cls.__name__

    def new_init(self, *args, **kwargs):
      self._initialized = False
      #self._nested_getattr = False

      if kwargs.get("_run_init", False):
        run_init = True
        del kwargs['_run_init']
      else:
        run_init = False

      if '_init_params' not in self.__dict__:
        track_init_params(self, original_init, *args, **kwargs)

        # If it is not a nested specifiable, we choose whether to skip original
        # init call based on options. Otherwise, we always call original init
        # for inner (parent/grandparent/etc) specifiable.
        if (on_demand_init and not run_init) or \
            (not on_demand_init and just_in_time_init):
          return

      logging.debug("call original %s.__init__ in new_init", class_name)
      original_init(self, *args, **kwargs)
      self._initialized = True

    def run_init(self):
      original_init(self, **self._init_params)

    def new_getattr(self, name):
      if name == '_nested_getattr' or \
          ('_nested_getattr' in self.__dict__ and self._nested_getattr):
        #self._nested_getattr = False
        delattr(self, "_nested_getattr")
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'")

      # set it before original init, in case getattr is called in original init
      self._nested_getattr = True

      if not self._initialized and name != "__getstate__":
        logging.debug("call original %s.__init__ in new_getattr", class_name)
        original_init(self, **self._init_params)
        self._initialized = True

      try:
        logging.debug("call original %s.getattr in new_getattr", class_name)
        ret = getattr(self, name)
      finally:
        # self._nested_getattr = False
        delattr(self, "_nested_getattr")
      return ret

    if just_in_time_init:
      cls.__getattr__ = new_getattr

    cls.__init__ = new_init
    cls._run_init = run_init
    cls.to_spec = Specifiable.to_spec
    cls._to_spec_helper = staticmethod(Specifiable._to_spec_helper)
    cls.from_spec = classmethod(Specifiable.from_spec)
    cls._from_spec_helper = staticmethod(Specifiable._from_spec_helper)
    return cls

  if my_cls is None:
    # support @specifiable(...)
    return _wrapper

  # support @specifiable without arguments
  return _wrapper(my_cls)
