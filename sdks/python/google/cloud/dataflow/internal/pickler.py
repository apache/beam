# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Pickler for values, functions, and classes.

Pickles created by the pickling library contain non-ASCII characters, so
we base64-encode the results so that we can put them in a JSON objects.
The pickler is used to embed FlatMap callable objects into the workflow JSON
description.

The pickler module should be used to pickle functions and modules; for values,
the coders.*PickleCoder classes should be used instead.
"""

import base64
import logging
import sys
import types

import dill

# Monkey patch dill to handle DictionaryTypes correctly.
# TODO(silviuc): Make sure we submit the fix upstream to GitHub dill project.
# pylint: disable=protected-access
dill.dill._reverse_typemap['DictionaryType'] = type({})
dill.dill._reverse_typemap['DictType'] = type({})
# pylint: enable=protected-access


def is_nested_class(cls):
  """Returns true if argument is a class object that appears to be nested."""
  return (isinstance(cls, type)
          and cls.__module__ != '__builtin__'
          and cls.__name__ not in sys.modules[cls.__module__].__dict__)


def find_containing_class(nested_class):
  """Finds containing class of a nestec class passed as argument."""

  def find_containing_class_inner(outer):
    for v in outer.__dict__.values():
      if v is nested_class:
        return outer
      elif isinstance(v, (type, types.ClassType)) and hasattr(v, '__dict__'):
        res = find_containing_class_inner(v)
        if res: return res

  return find_containing_class_inner(sys.modules[nested_class.__module__])


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
    if is_nested_class(obj) and obj.__module__ != '__main__':
      containing_class = find_containing_class(obj)
      if containing_class is not None:
        return pickler.save_reduce(
            getattr, (containing_class, obj.__name__), obj=obj)
    return fun(pickler, obj)

  return wrapper

# Monkey patch the standard pickler dispatch table entry for type objects.
# Dill, for certain types, defers to the standard pickler (including type
# objects). We wrap the standard handler using type_wrapper() because
# for nested class we want to pickle the actual enclosing class object so we
# can recreate it during unpickling.
# TODO(silviuc): Make sure we submit the fix upstream to GitHub dill project.
dill.dill.Pickler.dispatch[type] = _nested_type_wrapper(
    dill.dill.Pickler.dispatch[type])


# Turn off verbose logging from the dill pickler.
logging.getLogger('dill').setLevel(logging.WARN)


# TODO(ccy): Currently, there are still instances of pickler.dumps() and
# pickler.loads() being used for data, which results in an unnecessary base64
# encoding.  This should be cleaned up.
def dumps(o):
  return base64.b64encode(dill.dumps(o))


def loads(s):
  return dill.loads(base64.b64decode(s))


def dump_session(file_path):
  return dill.dump_session(file_path)


def load_session(file_path):
  return dill.load_session(file_path)
