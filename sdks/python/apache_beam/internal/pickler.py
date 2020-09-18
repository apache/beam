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

from __future__ import absolute_import

import base64
import bz2
import logging
import sys
import threading
import traceback
import types
from typing import Any
from typing import Dict
from typing import Tuple

import dill


class _NoOpContextManager(object):
  def __enter__(self):
    pass

  def __exit__(self, *unused_exc_info):
    pass


if sys.version_info[0] > 2:
  # Pickling, especially unpickling, causes broken module imports on Python 3
  # if executed concurrently, see: BEAM-8651, http://bugs.python.org/issue38884.
  _pickle_lock_unless_py2 = threading.RLock()
else:
  # Avoid slow reentrant locks on Py2. See: https://bugs.python.org/issue3001.
  _pickle_lock_unless_py2 = _NoOpContextManager()
# Dill 0.28.0 renamed dill.dill to dill._dill:
# https://github.com/uqfoundation/dill/commit/f0972ecc7a41d0b8acada6042d557068cac69baa
# TODO: Remove this once Beam depends on dill >= 0.2.8
if not getattr(dill, 'dill', None):
  dill.dill = dill._dill
  sys.modules['dill.dill'] = dill._dill

# TODO: Remove once Dataflow has containers with a preinstalled dill >= 0.2.8
if not getattr(dill, '_dill', None):
  dill._dill = dill.dill
  sys.modules['dill._dill'] = dill.dill


def _is_nested_class(cls):
  """Returns true if argument is a class object that appears to be nested."""
  return (
      isinstance(cls, type) and cls.__module__ is not None and
      cls.__module__ != 'builtins'  # Python 3
      and cls.__module__ != '__builtin__'  # Python 2
      and cls.__name__ not in sys.modules[cls.__module__].__dict__)


def _find_containing_class(nested_class):
  """Finds containing class of a nested class passed as argument."""

  seen = set()

  def _find_containing_class_inner(outer):
    if outer in seen:
      return None
    seen.add(outer)
    for k, v in outer.__dict__.items():
      if v is nested_class:
        return outer, k
      elif isinstance(v, type) and hasattr(v, '__dict__'):
        res = _find_containing_class_inner(v)
        if res: return res

  return _find_containing_class_inner(sys.modules[nested_class.__module__])


def _nested_type_wrapper(fun):
  """A wrapper for the standard pickler handler for class objects.

  Args:
    fun: Original pickler handler for type objects.

  Returns:
    A wrapper for type objects that handles nested classes.

  The wrapper detects if an object being pickled is a nested class object.
  For nested class object only it will save the containing class object so
  the nested structure is recreated during unpickle.
  """
  def wrapper(pickler, obj):
    # When the nested class is defined in the __main__ module we do not have to
    # do anything special because the pickler itself will save the constituent
    # parts of the type (i.e., name, base classes, dictionary) and then
    # recreate it during unpickling.
    if _is_nested_class(obj) and obj.__module__ != '__main__':
      containing_class_and_name = _find_containing_class(obj)
      if containing_class_and_name is not None:
        return pickler.save_reduce(getattr, containing_class_and_name, obj=obj)
    try:
      return fun(pickler, obj)
    except dill.dill.PicklingError:
      # pylint: disable=protected-access
      return pickler.save_reduce(
          dill.dill._create_type,
          (
              type(obj),
              obj.__name__,
              obj.__bases__,
              dill.dill._dict_from_dictproxy(obj.__dict__)),
          obj=obj)
      # pylint: enable=protected-access

  return wrapper


# Monkey patch the standard pickler dispatch table entry for type objects.
# Dill, for certain types, defers to the standard pickler (including type
# objects). We wrap the standard handler using type_wrapper() because
# for nested class we want to pickle the actual enclosing class object so we
# can recreate it during unpickling.
# TODO(silviuc): Make sure we submit the fix upstream to GitHub dill project.
dill.dill.Pickler.dispatch[type] = _nested_type_wrapper(
    dill.dill.Pickler.dispatch[type])


# Dill pickles generators objects without complaint, but unpickling produces
# TypeError: object.__new__(generator) is not safe, use generator.__new__()
# on some versions of Python.
def _reject_generators(unused_pickler, unused_obj):
  raise TypeError("can't (safely) pickle generator objects")


dill.dill.Pickler.dispatch[types.GeneratorType] = _reject_generators

# This if guards against dill not being full initialized when generating docs.
if 'save_module' in dir(dill.dill):

  # Always pickle non-main modules by name.
  old_save_module = dill.dill.save_module

  @dill.dill.register(dill.dill.ModuleType)
  def save_module(pickler, obj):
    if dill.dill.is_dill(pickler) and obj is pickler._main:
      return old_save_module(pickler, obj)
    else:
      dill.dill.log.info('M2: %s' % obj)
      # pylint: disable=protected-access
      pickler.save_reduce(dill.dill._import_module, (obj.__name__, ), obj=obj)
      # pylint: enable=protected-access
      dill.dill.log.info('# M2')

  # Pickle module dictionaries (commonly found in lambda's globals)
  # by referencing their module.
  old_save_module_dict = dill.dill.save_module_dict
  known_module_dicts: Dict[int, Tuple[types.ModuleType, Dict[str, Any]]] = {}

  @dill.dill.register(dict)
  def new_save_module_dict(pickler, obj):
    obj_id = id(obj)
    if not known_module_dicts or '__file__' in obj or '__package__' in obj:
      if obj_id not in known_module_dicts:
        # Trigger loading of lazily loaded modules (such as pytest vendored
        # modules).
        # This pass over sys.modules needs to iterate on a copy of sys.modules
        # since lazy loading modifies the dictionary, hence the use of list().
        for m in list(sys.modules.values()):
          try:
            _ = m.__dict__
          except AttributeError:
            pass

        for m in list(sys.modules.values()):
          try:
            if (m and m.__name__ != '__main__' and
                isinstance(m, dill.dill.ModuleType)):
              d = m.__dict__
              known_module_dicts[id(d)] = m, d
          except AttributeError:
            # Skip modules that do not have the __name__ attribute.
            pass
    if obj_id in known_module_dicts and dill.dill.is_dill(pickler):
      m = known_module_dicts[obj_id][0]
      try:
        # pylint: disable=protected-access
        dill.dill._import_module(m.__name__)
        return pickler.save_reduce(
            getattr, (known_module_dicts[obj_id][0], '__dict__'), obj=obj)
      except (ImportError, AttributeError):
        return old_save_module_dict(pickler, obj)
    else:
      return old_save_module_dict(pickler, obj)

  dill.dill.save_module_dict = new_save_module_dict

  def _nest_dill_logging():
    """Prefix all dill logging with its depth in the callstack.

    Useful for debugging pickling of deeply nested structures.
    """
    old_log_info = dill.dill.log.info

    def new_log_info(msg, *args, **kwargs):
      old_log_info(
          ('1 2 3 4 5 6 7 8 9 0 ' * 10)[:len(traceback.extract_stack())] + msg,
          *args,
          **kwargs)

    dill.dill.log.info = new_log_info


# Turn off verbose logging from the dill pickler.
logging.getLogger('dill').setLevel(logging.WARN)


def dumps(o, enable_trace=True) -> bytes:
  """For internal use only; no backwards-compatibility guarantees."""
  with _pickle_lock_unless_py2:
    try:
      s = dill.dumps(o)
    except Exception:  # pylint: disable=broad-except
      if enable_trace:
        dill.dill._trace(True)  # pylint: disable=protected-access
        s = dill.dumps(o)
      else:
        raise
    finally:
      dill.dill._trace(False)  # pylint: disable=protected-access

  # Compress as compactly as possible (compresslevel=9) to decrease peak memory
  # usage (of multiple in-memory copies) and to avoid hitting protocol buffer
  # limits.
  c = bz2.compress(s, compresslevel=9)
  del s  # Free up some possibly large and no-longer-needed memory.

  return base64.b64encode(c)


def loads(encoded, enable_trace=True):
  """For internal use only; no backwards-compatibility guarantees."""

  c = base64.b64decode(encoded)

  s = bz2.decompress(c)
  del c  # Free up some possibly large and no-longer-needed memory.

  with _pickle_lock_unless_py2:
    try:
      return dill.loads(s)
    except Exception:  # pylint: disable=broad-except
      if enable_trace:
        dill.dill._trace(True)  # pylint: disable=protected-access
        return dill.loads(s)
      else:
        raise
    finally:
      dill.dill._trace(False)  # pylint: disable=protected-access


def dump_session(file_path):
  """For internal use only; no backwards-compatibility guarantees.

  Pickle the current python session to be used in the worker.

  Note: Due to the inconsistency in the first dump of dill dump_session we
  create and load the dump twice to have consistent results in the worker and
  the running session. Check: https://github.com/uqfoundation/dill/issues/195
  """
  with _pickle_lock_unless_py2:
    dill.dump_session(file_path)
    dill.load_session(file_path)
    return dill.dump_session(file_path)


def load_session(file_path):
  with _pickle_lock_unless_py2:
    return dill.load_session(file_path)
