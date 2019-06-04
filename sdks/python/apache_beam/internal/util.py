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

"""Utility functions used throughout the package.

For internal use only. No backwards compatibility guarantees.
"""

from __future__ import absolute_import

import logging
import threading
import weakref
from builtins import object
from multiprocessing.pool import ThreadPool


class ArgumentPlaceholder(object):
  """For internal use only; no backwards-compatibility guarantees.

  A place holder object replacing PValues in argument lists.

  A Fn object can take any number of "side inputs", which are PValues that will
  be evaluated during pipeline execution and will be provided to the function
  at the moment of its execution as positional or keyword arguments.

  This is used only internally and should never be used by user code. A custom
  Fn object by the time it executes will have such values replaced with real
  computed values.
  """

  def __eq__(self, other):
    """Tests for equality of two placeholder objects.

    Args:
      other: Another placeholder object to compare to.

    This method is used only for test code. All placeholder objects are
    equal to each other.
    """
    return isinstance(other, ArgumentPlaceholder)

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __hash__(self):
    return hash(type(self))


def remove_objects_from_args(args, kwargs, pvalue_classes):
  """For internal use only; no backwards-compatibility guarantees.

  Replaces all objects of a given type in args/kwargs with a placeholder.

  Args:
    args: A list of positional arguments.
    kwargs: A dictionary of keyword arguments.
    pvalue_classes: A tuple of class objects representing the types of the
      arguments that must be replaced with a placeholder value (instance of
      ArgumentPlaceholder)

  Returns:
    A 3-tuple containing a modified list of positional arguments, a modified
    dictionary of keyword arguments, and a list of all objects replaced with
    a placeholder value.
  """
  pvals = []

  def swapper(value):
    pvals.append(value)
    return ArgumentPlaceholder()
  new_args = [swapper(v) if isinstance(v, pvalue_classes) else v for v in args]
  # Make sure the order in which we process the dictionary keys is predictable
  # by sorting the entries first. This will be important when putting back
  # PValues.
  new_kwargs = dict((k, swapper(v)) if isinstance(v, pvalue_classes) else (k, v)
                    for k, v in sorted(kwargs.items()))
  return (new_args, new_kwargs, pvals)


def insert_values_in_args(args, kwargs, values):
  """For internal use only; no backwards-compatibility guarantees.

  Replaces all placeholders in args/kwargs with actual values.

  Args:
    args: A list of positional arguments.
    kwargs: A dictionary of keyword arguments.
    values: A list of values that will be used to replace placeholder values.

  Returns:
    A 2-tuple containing a modified list of positional arguments, and a
    modified dictionary of keyword arguments.
  """
  # Use a local iterator so that we don't modify values.
  v_iter = iter(values)
  new_args = [
      next(v_iter) if isinstance(arg, ArgumentPlaceholder) else arg
      for arg in args]
  new_kwargs = dict(
      (k, next(v_iter)) if isinstance(v, ArgumentPlaceholder) else (k, v)
      for k, v in sorted(kwargs.items()))
  return (new_args, new_kwargs)


def run_using_threadpool(fn_to_execute, inputs, pool_size):
  """For internal use only; no backwards-compatibility guarantees.

  Runs the given function on given inputs using a thread pool.

  Args:
    fn_to_execute: Function to execute
    inputs: Inputs on which given function will be executed in parallel.
    pool_size: Size of thread pool.
  Returns:
    Results retrieved after executing the given function on given inputs.
  """

  # ThreadPool crashes in old versions of Python (< 2.7.5) if created
  # from a child thread. (http://bugs.python.org/issue10015)
  if not hasattr(threading.current_thread(), '_children'):
    threading.current_thread()._children = weakref.WeakKeyDictionary()
  pool = ThreadPool(min(pool_size, len(inputs)))
  try:
    # We record and reset logging level here since 'apitools' library Beam
    # depends on updates the logging level when used with a threadpool -
    # https://github.com/google/apitools/issues/141
    # TODO: Remove this once above issue in 'apitools' is fixed.
    old_level = logging.getLogger().level
    return pool.map(fn_to_execute, inputs)
  finally:
    pool.terminate()
    logging.getLogger().setLevel(old_level)
