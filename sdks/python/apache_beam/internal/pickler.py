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

PICKLE_LIB = 'cloudPickle'
if PICKLE_LIB  == 'dill':
  from apache_beam.internal import dill_pickler as pickler_lib
elif PICKLE_LIB == 'cloudPickle':
  from apache_beam.internal import cloudPickle_pickler as pickler_lib

def dumps(o, enable_trace=True, use_zlib=False):
  # type: (...) -> bytes

  return pickler_lib.dumps(o, enable_trace=enable_trace, use_zlib=use_zlib)

def loads(encoded, enable_trace=True, use_zlib=False):
  """For internal use only; no backwards-compatibility guarantees."""
  return pickler_lib.loads(encoded, enable_trace=enable_trace, use_zlib=use_zlib)

def dump_session(file_path):
  """For internal use only; no backwards-compatibility guarantees.

  Pickle the current python session to be used in the worker.
  """

  return pickler_lib.dump_session(file_path)

def load_session(file_path):
  return pickler_lib.load_session(file_path)
