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

from apache_beam.internal import cloudpickle_pickler
import sys

try:
  from apache_beam.internal import dill_pickler
except ImportError:
  dill_pickler = None

USE_CLOUDPICKLE = 'cloudpickle'
USE_DILL = 'dill'

if sys.version_info < (3, 11):
  DEFAULT_PICKLE_LIB = USE_DILL
  desired_pickle_lib = dill_pickler
else:
  DEFAULT_PICKLE_LIB = USE_CLOUDPICKLE
  desired_pickle_lib = cloudpickle_pickler


def dumps(o, enable_trace=True, use_zlib=False):
  # type: (...) -> bytes

  return desired_pickle_lib.dumps(
      o, enable_trace=enable_trace, use_zlib=use_zlib)


def loads(encoded, enable_trace=True, use_zlib=False):
  """For internal use only; no backwards-compatibility guarantees."""

  return desired_pickle_lib.loads(
      encoded, enable_trace=enable_trace, use_zlib=use_zlib)


def dump_session(file_path):
  """For internal use only; no backwards-compatibility guarantees.

  Pickle the current python session to be used in the worker.
  """

  return desired_pickle_lib.dump_session(file_path)


def load_session(file_path):
  return desired_pickle_lib.load_session(file_path)


def set_library(selected_library=DEFAULT_PICKLE_LIB):
  """ Sets pickle library that will be used. """
  global desired_pickle_lib
  # If switching to or from dill, update the pickler hook overrides.
  if (selected_library == USE_DILL) != (desired_pickle_lib == dill_pickler):
    dill_pickler.override_pickler_hooks(selected_library == USE_DILL)

  if selected_library == 'default':
    selected_library = DEFAULT_PICKLE_LIB

  if selected_library == USE_DILL:
    if sys.version_info > (3, 10) and dill_pickler is None:
      raise RuntimeError(
          "The optional `dill` package is not installed. "
          "To use the `--pickle_library==dill` pipeline option, please make "
          "dill a dependency of your pipeline. Make sure to use the exact "
          "same version of dill at pipeline submission and at pipeline "
          "runtime. For more information, see: "
          "https://beam.apache.org/documentation/sdks/python-pipeline-dependencies"  # pylint: disable=line-too-long
      )
    desired_pickle_lib = dill_pickler
  elif selected_library == USE_CLOUDPICKLE:
    desired_pickle_lib = cloudpickle_pickler
  else:
    raise ValueError(f'Unknown pickler library: {selected_library}')
