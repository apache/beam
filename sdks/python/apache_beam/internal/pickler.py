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
# pylint: skip-file

import logging

from apache_beam.internal import cloudpickle_pickler
from apache_beam.internal import dill_pickler

USE_CLOUDPICKLE = 'cloudpickle'
USE_DILL = 'dill'
DEFAULT_PICKLE_LIB = USE_DILL

desired_pickle_lib = dill_pickler


def dumps(o, enable_trace=True, use_zlib=False):
  # type: (...) -> bytes

  logging.info("****************dumps*****************************")
  serialized_fn = desired_pickle_lib.dumps(
      o, enable_trace=enable_trace, use_zlib=use_zlib)

  # pylint: disable=line-too-long
  if serialized_fn == b'QlpoOTFBWSZTWW2DOMwAAoP/0P//////////////8r///+ZjwQAAkABAAvZLMKAIaptKmgD1G1NHqPUGQ9QABoGRtQPUGgeiAyANPU9QaDxINDTT1BKIBAITI1M9U8p6ptPVPTSNGDIjQyGT1GamahoAGAaRoaZpPapk3qQanopooAMgAABoAAyGgAAA0AAA0AaADQEGAAAAAAAAAAATAAAAARgACYAM5MUrCptpkfPPVhjTXwKIU3oFiIJ/CmyKm2KIZ1A18ooB+j4G8x4RVknC4yKFH0rinqKEj8RACpHX3hkYJ5lTMPMdIKIjxIJLJEEKSWKSUBYhCIgBFQxHB/PDjH7GXX5+F4WG8bQ28I1HM8gN8O+MHn9DPGSnGjxh4zcN0oIQI7Yv1G4WNxjRhrCUDLResXxg0wWTJY0RkAs4YoXfYNHiK1PUibLpaDbVdM0lVFU2Ga4xG50HXL9B8IIJ3S3fGpfgZYIeoqgSmvHXY4pat1MZa7ks9yFCSDapYAMsDHfyzSb4zGUw5t70rhZZiNtIu4vMPDECfHMIUodUKP0j5TMfMfEdFQkidA+NLOQI6ArlUgx/Ue4ZtnU3xD5Nogb4vYoAjegh16U641+yaJJHXKN4if4ZnmkolBy1Kpzh29b6NzuvmJ8N0aBQ4vcGtWNS7paBLBRKKJpCPJn6GnMVjYmA1CFsd3GJM1XrYBOkXTQGV0NzA+shJZyobgOKghGWSFYnJQBArEUgOdM9kEYqk2a+e9ILaq+pPzb0YygDFo1mIMh62i3XGgjGQtNQx8cIZTMziESoLDIE9ZYgmTis3lmHF3LxVvmCsgIxROJHR7w4cgVCwvDSD7EMzkHTPIgpMKhMgrVRSByLk/JpwYRpMwGiYOWVIYb/2jyk0i6JqnoRqkVL5chLUoVeScA2PT6YNNPPqzszrZ23SdVGa9b+znUxbwnsfO/Lswr1n1TabWQXvNYuhcqVLvqYsPUxlCyVZPe565HNWiTIQvjZRL9gxDu+vOKUotqgY4SGilwxDlaK0XP0WobRwjhlpcfOTmhckNqGmsXXapFS8Y3MICo8xBAxSe07BsFi2ZUuDHJE49mqRUl7SP1yYtCiWApKQUpaPGR+xRVJBxjaedDdhykYHvA+J/0f8ofsOK9IoUZBk/7TeXorWosw1/i7kinChINsGcZg':
    import dis
    logging.info(dis.dis(o[0]))

  deserialized_fn = desired_pickle_lib.loads(
      serialized_fn, enable_trace=enable_trace, use_zlib=use_zlib)

  logging.info(f"serialized_fn: {serialized_fn}")
  logging.info(f"deserialized_fn: {deserialized_fn}")
  import sys
  logging.info("Python version: {}".format(sys.version))
  logging.info("****************dumps*****************************")

  return desired_pickle_lib.dumps(
      o, enable_trace=enable_trace, use_zlib=use_zlib)


def loads(encoded, enable_trace=True, use_zlib=False):
  """For internal use only; no backwards-compatibility guarantees."""
  logging.info("******************loads****************************")
  logging.info(f"encoded: {encoded}")

  decoded = desired_pickle_lib.loads(
      encoded, enable_trace=enable_trace, use_zlib=use_zlib)
  logging.info(f"decoded: {decoded}")

  import sys
  logging.info("Python version: {}".format(sys.version))
  logging.info("******************loads****************************")
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
  if selected_library == USE_DILL and desired_pickle_lib != dill_pickler:
    desired_pickle_lib = dill_pickler
    dill_pickler.override_pickler_hooks(True)
  elif (selected_library == USE_CLOUDPICKLE and
        desired_pickle_lib != cloudpickle_pickler):
    desired_pickle_lib = cloudpickle_pickler
    dill_pickler.override_pickler_hooks(False)
