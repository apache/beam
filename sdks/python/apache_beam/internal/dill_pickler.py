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

import dill

from apache_beam.internal.code_object_pickler import get_normalized_path
from apache_beam.internal.set_pickler import save_frozenset
from apache_beam.internal.set_pickler import save_set

settings = {'dill_byref': None}

patch_save_code = sys.version_info >= (3, 10) and dill.__version__ == "0.3.1.1"

if patch_save_code:
  # The following function is based on 'save_code' from 'dill'
  # Author: Mike McKerns (mmckerns @caltech and @uqfoundation)
  # Copyright (c) 2008-2015 California Institute of Technology.
  # Copyright (c) 2016-2023 The Uncertainty Quantification Foundation.
  # License: 3-clause BSD.  The full license text is available at:
  #  - https://github.com/uqfoundation/dill/blob/master/LICENSE

  # The following function is also based on 'save_codeobject' from 'cloudpickle'
  # Copyright (c) 2012, Regents of the University of California.
  # Copyright (c) 2009 `PiCloud, Inc. <http://www.picloud.com>`_.
  # License: 3-clause BSD.  The full license text is available at:
  #  - https://github.com/cloudpipe/cloudpickle/blob/master/LICENSE

  from types import CodeType

  @dill.register(CodeType)
  def save_code(pickler, obj):
    co_filename = get_normalized_path(obj.co_filename)
    if hasattr(obj, "co_endlinetable"):  # python 3.11a (20 args)
      args = (
          obj.co_argcount,
          obj.co_posonlyargcount,
          obj.co_kwonlyargcount,
          obj.co_nlocals,
          obj.co_stacksize,
          obj.co_flags,
          obj.co_code,
          obj.co_consts,
          obj.co_names,
          obj.co_varnames,
          co_filename,
          obj.co_name,
          obj.co_qualname,
          obj.co_firstlineno,
          obj.co_linetable,
          obj.co_endlinetable,
          obj.co_columntable,
          obj.co_exceptiontable,
          obj.co_freevars,
          obj.co_cellvars)
    elif hasattr(obj, "co_exceptiontable"):  # python 3.11 (18 args)
      args = (
          obj.co_argcount,
          obj.co_posonlyargcount,
          obj.co_kwonlyargcount,
          obj.co_nlocals,
          obj.co_stacksize,
          obj.co_flags,
          obj.co_code,
          obj.co_consts,
          obj.co_names,
          obj.co_varnames,
          co_filename,
          obj.co_name,
          obj.co_qualname,
          obj.co_firstlineno,
          obj.co_linetable,
          obj.co_exceptiontable,
          obj.co_freevars,
          obj.co_cellvars)
    elif hasattr(obj, "co_linetable"):  # python 3.10 (16 args)
      args = (
          obj.co_argcount,
          obj.co_posonlyargcount,
          obj.co_kwonlyargcount,
          obj.co_nlocals,
          obj.co_stacksize,
          obj.co_flags,
          obj.co_code,
          obj.co_consts,
          obj.co_names,
          obj.co_varnames,
          co_filename,
          obj.co_name,
          obj.co_firstlineno,
          obj.co_linetable,
          obj.co_freevars,
          obj.co_cellvars)
    elif hasattr(obj, "co_posonlyargcount"):  # python 3.8 (16 args)
      args = (
          obj.co_argcount,
          obj.co_posonlyargcount,
          obj.co_kwonlyargcount,
          obj.co_nlocals,
          obj.co_stacksize,
          obj.co_flags,
          obj.co_code,
          obj.co_consts,
          obj.co_names,
          obj.co_varnames,
          co_filename,
          obj.co_name,
          obj.co_firstlineno,
          obj.co_lnotab,
          obj.co_freevars,
          obj.co_cellvars)
    else:  # python 3.7 (15 args)
      args = (
          obj.co_argcount,
          obj.co_kwonlyargcount,
          obj.co_nlocals,
          obj.co_stacksize,
          obj.co_flags,
          obj.co_code,
          obj.co_consts,
          obj.co_names,
          obj.co_varnames,
          obj.co_filename,
          obj.co_name,
          obj.co_firstlineno,
          obj.co_lnotab,
          obj.co_freevars,
          obj.co_cellvars)
    pickler.save_reduce(CodeType, args, obj=obj)

  dill._dill.save_code = save_code


class _NoOpContextManager(object):
  def __enter__(self):
    pass

  def __exit__(self, *unused_exc_info):
    pass


# Pickling, especially unpickling, causes broken module imports on Python 3
# if executed concurrently, see: BEAM-8651, http://bugs.python.org/issue38884.
_pickle_lock = threading.RLock()
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

dill_log = getattr(dill.dill, 'log', None)

# dill v0.3.6 changed the attribute name from 'log' to 'logger'
if not dill_log:
  dill_log = getattr(dill.dill, 'logger')


def _is_nested_class(cls):
  """Returns true if argument is a class object that appears to be nested."""
  return (
      isinstance(cls, type) and cls.__module__ is not None and
      cls.__module__ != 'builtins' and
      cls.__name__ not in sys.modules[cls.__module__].__dict__)


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


def _dict_from_mappingproxy(mp):
  d = mp.copy()
  d.pop('__dict__', None)
  d.pop('__prepare__', None)
  d.pop('__weakref__', None)
  return d


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
              _dict_from_mappingproxy(obj.__dict__)),
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
      dill_log.info('M2: %s' % obj)
      # pylint: disable=protected-access
      pickler.save_reduce(
          dill.dill._import_module, (obj.__name__, ), obj=obj)
      # pylint: enable=protected-access
      dill_log.info('# M2')

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
    old_log_info = dill_log.info

    def new_log_info(msg, *args, **kwargs):
      old_log_info(
          ('1 2 3 4 5 6 7 8 9 0 ' * 10)[:len(traceback.extract_stack())] + msg,
          *args,
          **kwargs)

    dill_log.info = new_log_info


# Turn off verbose logging from the dill pickler.
logging.getLogger('dill').setLevel(logging.WARN)


def dumps(
    o,
    enable_trace=True,
    use_zlib=False,
    enable_best_effort_determinism=False) -> bytes:
  """For internal use only; no backwards-compatibility guarantees."""
  with _pickle_lock:
    if enable_best_effort_determinism:
      old_save_set = dill.dill.Pickler.dispatch[set]
      old_save_frozenset = dill.dill.Pickler.dispatch[frozenset]
      dill.dill.pickle(set, save_set)
      dill.dill.pickle(frozenset, save_frozenset)
    try:
      s = dill.dumps(o, byref=settings['dill_byref'])
    except Exception:  # pylint: disable=broad-except
      if enable_trace:
        dill.dill._trace(True)  # pylint: disable=protected-access
        s = dill.dumps(o, byref=settings['dill_byref'])
      else:
        raise
    finally:
      dill.dill._trace(False)  # pylint: disable=protected-access
      if enable_best_effort_determinism:
        dill.dill.pickle(set, old_save_set)
        dill.dill.pickle(frozenset, old_save_frozenset)

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
  with _pickle_lock:
    dill.dump_session(file_path)
    dill.load_session(file_path)
    return dill.dump_session(file_path)


def load_session(file_path):
  with _pickle_lock:
    return dill.load_session(file_path)


def override_pickler_hooks(extend=True):
  """ Extends the dill library hooks into that of the standard pickler library.

  If false all hooks that dill overrides will be removed.
  If true dill hooks will be injected into the pickler library dispatch_table.
  """
  dill.extend(extend)
