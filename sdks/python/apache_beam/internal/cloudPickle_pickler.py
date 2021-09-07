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

Pickles created by the pickling library contain non-ASCII characters, so
we base64-encode the results so that we can put them in a JSON objects.
The pickler is used to embed FlatMap callable objects into the workflow JSON
description.

The pickler module should be used to pickle functions and modules; for values,
the coders.*PickleCoder classes should be used instead.
"""

# pytype: skip-file

import base64
import bz2
import logging
import sys
import threading
import traceback
import types
import zlib
from typing import Any
from typing import Dict
from typing import Tuple

import cloudpickle

# Pickling, especially unpickling, causes broken module imports on Python 3
# if executed concurrently, see: BEAM-8651, http://bugs.python.org/issue38884.
_pickle_lock = threading.RLock()
import __main__ as _main_module


def dumps(o, enable_trace=True, use_zlib=False):
  # type: (...) -> bytes

  """For internal use only; no backwards-compatibility guarantees."""
  with _pickle_lock:
    try:
      s = cloudpickle.dumps(o)
    except Exception:  # pylint: disable=broad-except
      # TODO: decide what to do on exceptions.
      print('TODO figure out what to do with cloudpickle exceptions.')
      raise

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
    try:
      return cloudpickle.loads(s)
    except Exception:  # pylint: disable=broad-except
      print('TODO figure out what to do with cloudpickle exceptions.')
      raise

def dump_session(file_path):
  with _pickle_lock:
    try:
      module_dict = _main_module.__dict__.copy()
      with open(file_path, 'wb') as file:
        cloudpickle.dump(module_dict, file)
    except Exception:
      print('TODO figure out what to do with cloudpickle exceptions.')
      raise



def load_session(file_path):
  with _pickle_lock:
    try:
      module_dict = _main_module.__dict__.copy()
      with open(file_path, 'wb') as file:
        cloudpickle.dump(module_dict, file)
    except Exception:
      print('TODO figure out what to do with cloudpickle exceptions.')
      raise
