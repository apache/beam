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

from apache_beam.internal.cloudpickle import cloudpickle

DEFAULT_CONFIG = cloudpickle.CloudPickleConfig(
    skip_reset_dynamic_type_state=True)
NO_DYNAMIC_CLASS_TRACKING_CONFIG = cloudpickle.CloudPickleConfig(
    id_generator=None, skip_reset_dynamic_type_state=True)

try:
  from absl import flags
except (ImportError, ModuleNotFoundError):
  pass


def _get_proto_enum_descriptor_class():
  try:
    from google.protobuf.internal import api_implementation
  except ImportError:
    return None

  implementation_type = api_implementation.Type()

  if implementation_type == 'upb':
    try:
      from google._upb._message import EnumDescriptor
      return EnumDescriptor
    except ImportError:
      pass
  elif implementation_type == 'cpp':
    try:
      from google.protobuf.pyext._message import EnumDescriptor
      return EnumDescriptor
    except ImportError:
      pass
  elif implementation_type == 'python':
    try:
      from google.protobuf.internal.python_message import EnumDescriptor
      return EnumDescriptor
    except ImportError:
      pass

  return None


EnumDescriptor = _get_proto_enum_descriptor_class()

# Pickling, especially unpickling, causes broken module imports on Python 3
# if executed concurrently, see: BEAM-8651, http://bugs.python.org/issue38884.
_pickle_lock = threading.RLock()
RLOCK_TYPE = type(_pickle_lock)
LOCK_TYPE = type(threading.Lock())
_LOGGER = logging.getLogger(__name__)


def _reconstruct_enum_descriptor(full_name):
  for _, module in list(sys.modules.items()):
    if not hasattr(module, 'DESCRIPTOR'):
      continue

    if hasattr(module.DESCRIPTOR, 'enum_types_by_name'):
      for (_, enum_desc) in module.DESCRIPTOR.enum_types_by_name.items():
        if enum_desc.full_name == full_name:
          return enum_desc

    for _, attr_value in vars(module).items():
      if not hasattr(attr_value, 'DESCRIPTOR'):
        continue

      if hasattr(attr_value.DESCRIPTOR, 'enum_types_by_name'):
        for (_, enum_desc) in attr_value.DESCRIPTOR.enum_types_by_name.items():
          if enum_desc.full_name == full_name:
            return enum_desc
  raise ImportError(f'Could not find enum descriptor: {full_name}')


def _pickle_enum_descriptor(obj):
  full_name = obj.full_name
  return _reconstruct_enum_descriptor, (full_name, )


def dumps(
    o,
    enable_trace=True,
    use_zlib=False,
    enable_best_effort_determinism=False,
    config: cloudpickle.CloudPickleConfig = DEFAULT_CONFIG) -> bytes:
  """For internal use only; no backwards-compatibility guarantees."""
  if enable_best_effort_determinism:
    # TODO: Add support once https://github.com/cloudpipe/cloudpickle/pull/563
    # is merged in.
    _LOGGER.warning(
        'Ignoring unsupported option: enable_best_effort_determinism. '
        'This has only been implemented for dill.')
  with _pickle_lock:
    with io.BytesIO() as file:
      pickler = cloudpickle.CloudPickler(file, config=config)
      try:
        pickler.dispatch_table[type(flags.FLAGS)] = _pickle_absl_flags
      except NameError:
        pass
      try:
        pickler.dispatch_table[RLOCK_TYPE] = _pickle_rlock
      except NameError:
        pass
      try:
        pickler.dispatch_table[LOCK_TYPE] = _lock_reducer
      except NameError:
        pass
      if EnumDescriptor is not None:
        pickler.dispatch_table[EnumDescriptor] = _pickle_enum_descriptor
      pickler.dump(o)
      s = file.getvalue()

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


def loads(encoded, enable_trace=True, use_zlib=False):
  """For internal use only; no backwards-compatibility guarantees."""

  c = base64.b64decode(encoded)

  if use_zlib:
    s = zlib.decompress(c)
  else:
    s = bz2.decompress(c)

  del c  # Free up some possibly large and no-longer-needed memory.

  with _pickle_lock:
    unpickled = cloudpickle.loads(s)
    return unpickled


def _pickle_absl_flags(obj):
  return _create_absl_flags, tuple([])


def _create_absl_flags():
  return flags.FLAGS


def _pickle_rlock(obj):
  return RLOCK_TYPE, tuple([])


def _lock_reducer(obj):
  return threading.Lock, tuple([])


def dump_session(file_path):
  # It is possible to dump session with cloudpickle. However, since references
  # are saved it should not be necessary. See https://s.apache.org/beam-picklers
  pass


def load_session(file_path):
  # It is possible to load_session with cloudpickle. However, since references
  # are saved it should not be necessary. See https://s.apache.org/beam-picklers
  pass
