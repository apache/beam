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

"""Pickler for values, functions, and classes.

For internal use only. No backwards compatibility guarantees.

Uses the cloudpickle library to pickle data, functions, lambdas
and classes.

dump_session and load_session are no-ops.
"""

# pytype: skip-file

import base64
import bz2
import io
import logging
import sys
import threading
import zlib

from apache_beam.internal import code_object_pickler
from apache_beam.internal.cloudpickle import cloudpickle
from apache_beam.internal.code_object_pickler import get_normalized_path

DEFAULT_CONFIG = cloudpickle.CloudPickleConfig(
    skip_reset_dynamic_type_state=True,
    filepath_interceptor=get_normalized_path)
STABLE_CODE_IDENTIFIER_CONFIG = cloudpickle.CloudPickleConfig(
    skip_reset_dynamic_type_state=True,
    filepath_interceptor=get_normalized_path,
    get_code_object_params=cloudpickle.GetCodeObjectParams(
        get_code_object_identifier=code_object_pickler.
        get_code_object_identifier,
        get_code_from_identifier=code_object_pickler.get_code_from_identifier))

try:
  from absl import flags
except (ImportError, ModuleNotFoundError):
  pass

try:
  from google.protobuf import descriptor_pb2
  MessageDescriptor = type(descriptor_pb2.DescriptorProto.DESCRIPTOR)
  EnumDescriptor = type(descriptor_pb2.FieldDescriptorProto.Type.DESCRIPTOR)
except (ImportError, ModuleNotFoundError):
  MessageDescriptor = None
  EnumDescriptor = None

# Pickling, especially unpickling, causes broken module imports on Python 3
# if executed concurrently, see: BEAM-8651, http://bugs.python.org/issue38884.
_pickle_lock = threading.RLock()
RLOCK_TYPE = type(_pickle_lock)
LOCK_TYPE = type(threading.Lock())
_LOGGER = logging.getLogger(__name__)


# Helper to return an object directly during unpickling.
def _return_obj(obj):
  return obj


# Optional import for Python 3.12 TypeAliasType
try:  # pragma: no cover - dependent on Python version
  from typing import TypeAliasType as _TypeAliasType  # type: ignore[attr-defined]
except Exception:
  _TypeAliasType = None


def _typealias_reduce(obj):
  # Unwrap typing.TypeAliasType to its underlying value for robust pickling.
  underlying = getattr(obj, '__value__', None)
  if underlying is None:
    # Fallback: return the object itself; lets default behavior handle it.
    return _return_obj, (obj, )
  return _return_obj, (underlying, )


def _reconstruct_message_descriptor(full_name):
  from google.protobuf import descriptor_pool
  return descriptor_pool.Default().FindMessageTypeByName(full_name)


def _pickle_message_descriptor(obj):
  return _reconstruct_message_descriptor, (obj.full_name, )


def _reconstruct_enum_descriptor(full_name):
  from google.protobuf import descriptor_pool
  return descriptor_pool.Default().FindEnumTypeByName(full_name)


def _pickle_enum_descriptor(obj):
  return _reconstruct_enum_descriptor, (obj.full_name, )


def dumps(
    o,
    enable_trace=True,
    use_zlib=False,
    enable_best_effort_determinism=False,
    enable_stable_code_identifier_pickling=False,
    config: cloudpickle.CloudPickleConfig = DEFAULT_CONFIG) -> bytes:
  """For internal use only; no backwards-compatibility guarantees."""
  s = _dumps(
      o,
      enable_best_effort_determinism,
      enable_stable_code_identifier_pickling,
      config)

  # Compress as compactly as possible (compresslevel=9) to decrease peak memory
  # usage (of multiple in-memory copies) and to avoid hitting protocol buffer
  # limits.
  # WARNING: Be cautious about compressor change since it can lead to pipeline
  # representation change, and can break streaming job update compatibility on
  # runners such as Dataflow.
  if use_zlib:
    c = zlib.compress(s, 9)
  else:
    c = bz2.compress(s, compresslevel=9)
  del s  # Free up some possibly large and no-longer-needed memory.

  return base64.b64encode(c)


def _dumps(
    o,
    enable_best_effort_determinism=False,
    enable_stable_code_identifier_pickling=False,
    config: cloudpickle.CloudPickleConfig = DEFAULT_CONFIG) -> bytes:

  if enable_best_effort_determinism:
    # TODO: Add support once https://github.com/cloudpipe/cloudpickle/pull/563
    # is merged in.
    _LOGGER.warning(
        'Ignoring unsupported option: enable_best_effort_determinism. '
        'This has only been implemented for dill.')
  with _pickle_lock:
    with io.BytesIO() as file:
      if enable_stable_code_identifier_pickling:
        config = STABLE_CODE_IDENTIFIER_CONFIG
      pickler = cloudpickle.CloudPickler(file, config=config)
      try:
        pickler.dispatch_table[type(flags.FLAGS)] = _pickle_absl_flags
      except NameError:
        pass
      # Register Python 3.12 `type` alias reducer to unwrap to underlying value.
      if _TypeAliasType is not None:
        pickler.dispatch_table[_TypeAliasType] = _typealias_reduce
      try:
        pickler.dispatch_table[RLOCK_TYPE] = _pickle_rlock
      except NameError:
        pass
      try:
        pickler.dispatch_table[LOCK_TYPE] = _lock_reducer
      except NameError:
        pass
      if MessageDescriptor is not None:
        pickler.dispatch_table[MessageDescriptor] = _pickle_message_descriptor
      if EnumDescriptor is not None:
        pickler.dispatch_table[EnumDescriptor] = _pickle_enum_descriptor
      pickler.dump(o)
      return file.getvalue()


def loads(encoded, enable_trace=True, use_zlib=False):
  """For internal use only; no backwards-compatibility guarantees."""

  c = base64.b64decode(encoded)

  if use_zlib:
    s = zlib.decompress(c)
  else:
    s = bz2.decompress(c)

  del c  # Free up some possibly large and no-longer-needed memory.
  return _loads(s)


def _loads(s):
  with _pickle_lock:
    unpickled = cloudpickle.loads(s)
    return unpickled


def roundtrip(o):
  """Internal utility for testing round-trip pickle serialization."""
  return _loads(_dumps(o))


def _pickle_absl_flags(obj):
  return _create_absl_flags, tuple([])


def _create_absl_flags():
  return flags.FLAGS


def _pickle_rlock(obj):
  return RLOCK_TYPE, tuple([])


def _lock_reducer(obj):
  return threading.Lock, tuple([])


def dump_session(file_path):
  # Since References are saved (https://s.apache.org/beam-picklers), we only
  # dump supported Beam Registries (currently only logical type registry)
  from apache_beam.coders import typecoders
  from apache_beam.typehints import schemas
  from apache_beam.typehints.schema_registry import SCHEMA_REGISTRY

  with _pickle_lock, open(file_path, 'wb') as file:
    coder_reg = typecoders.registry.get_custom_type_coder_tuples()
    logical_type_reg = schemas.LogicalType._known_logical_types.copy_custom()
    schema_reg = SCHEMA_REGISTRY.get_registered_typings()

    pickler = cloudpickle.CloudPickler(file)
    # TODO(https://github.com/apache/beam/issues/18500) add file system registry
    # once implemented
    pickler.dump({
        "coder": coder_reg,
        "logical_type": logical_type_reg,
        "schema": schema_reg
    })


def load_session(file_path):
  from apache_beam.coders import typecoders
  from apache_beam.typehints import schemas
  from apache_beam.typehints.schema_registry import SCHEMA_REGISTRY

  with _pickle_lock, open(file_path, 'rb') as file:
    registries = cloudpickle.load(file)
    if type(registries) != dict:
      raise ValueError(
          "Faled loading session: expected dict, got {}", type(registries))
    if "coder" in registries:
      typecoders.registry.load_custom_type_coder_tuples(registries["coder"])
    else:
      _LOGGER.warning('No coder registry found in saved session')
    if "logical_type" in registries:
      schemas.LogicalType._known_logical_types.load(registries["logical_type"])
    else:
      _LOGGER.warning('No logical type registry found in saved session')
    if "schema" in registries:
      SCHEMA_REGISTRY.load_registered_typings(registries["schema"])
    else:
      _LOGGER.warning('No schema registry found in saved session')
