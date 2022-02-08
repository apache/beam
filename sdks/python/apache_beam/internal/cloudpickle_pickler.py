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
import threading
import zlib

import cloudpickle

try:
  from absl import flags
except (ImportError, ModuleNotFoundError):
  pass

# Pickling, especially unpickling, causes broken module imports on Python 3
# if executed concurrently, see: BEAM-8651, http://bugs.python.org/issue38884.
_pickle_lock = threading.RLock()
RLOCK_TYPE = type(_pickle_lock)


def dumps(o, enable_trace=True, use_zlib=False):
  # type: (...) -> bytes

  """For internal use only; no backwards-compatibility guarantees."""
  with _pickle_lock:
    with io.BytesIO() as file:
      pickler = cloudpickle.CloudPickler(file)
      try:
        pickler.dispatch_table[type(flags.FLAGS)] = _pickle_absl_flags
      except NameError:
        pass
      try:
        pickler.dispatch_table[RLOCK_TYPE] = _pickle_rlock
      except NameError:
        pass
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


def dump_session(file_path):
  # It is possible to dump session with cloudpickle. However, since references
  # are saved it should not be necessary. See https://s.apache.org/beam-picklers
  pass


def load_session(file_path):
  # It is possible to load_session with cloudpickle. However, since references
  # are saved it should not be necessary. See https://s.apache.org/beam-picklers
  pass
