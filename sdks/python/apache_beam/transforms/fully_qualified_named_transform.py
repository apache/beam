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
# pytype: skip-file

import contextlib
import fnmatch
import importlib

from apache_beam import coders
from apache_beam.portability.api.external_transforms_pb2 import ExternalConfigurationPayload
from apache_beam.pvalue import Row
from apache_beam.transforms import ptransform
from apache_beam.typehints.native_type_compatibility import convert_to_typing_type
from apache_beam.typehints.schemas import named_fields_to_schema
from apache_beam.typehints.trivial_inference import instance_to_type

PYTHON_FULLY_QUALIFIED_NAMED_TRANSFORM_URN = (
    'beam:transforms:python:fully_qualified_named')


@ptransform.PTransform.register_urn(
    PYTHON_FULLY_QUALIFIED_NAMED_TRANSFORM_URN, ExternalConfigurationPayload)
class FullyQualifiedNamedTransform(ptransform.PTransform):

  _FILTER_GLOB = None

  @classmethod
  @contextlib.contextmanager
  def with_filter(cls, filter):
    old_filter, cls._FILTER_GLOB = cls._FILTER_GLOB, filter
    yield
    cls._FILTER_GLOB = old_filter

  def __init__(self, constructor, args, kwargs):
    self._constructor = constructor
    self._args = args
    self._kwargs = kwargs

  def expand(self, pinput):
    return pinput | self._resolve(self._constructor)(
        *self._args, **self._kwargs)

  @classmethod
  def _resolve(cls, fully_qualified_name):
    if not cls._FILTER_GLOB or not fnmatch.fnmatchcase(fully_qualified_name,
                                                       cls._FILTER_GLOB):
      raise ValueError(
          f'Fully qualifed name "{fully_qualified_name}" '
          f'not allowed by filter {cls._FILTER_GLOB}.')
    o = None
    path = ''
    for segment in fully_qualified_name.split('.'):
      if o is not None and hasattr(o, segment):
        o = getattr(o, segment)
      else:
        o = importlib.import_module(segment, path)
      path = '.'.join([path, segment])
    return o

  def to_runner_api_parameter(self, unused_context):
    _args_schema = named_fields_to_schema([
        (f'arg{ix}', convert_to_typing_type(instance_to_type(value)))
        for (ix, value) in enumerate(self._args)
    ])
    _kwargs_schema = named_fields_to_schema([
        (key, convert_to_typing_type(instance_to_type(value)))
        for (key, value) in self._kwargs.items()
    ])
    payload_schema = named_fields_to_schema({
        'constructor': str,
        'args': _args_schema,
        'kwargs': _kwargs_schema,
    })
    return (
        PYTHON_FULLY_QUALIFIED_NAMED_TRANSFORM_URN,
        ExternalConfigurationPayload(
            schema=payload_schema,
            payload=coders.RowCoder(payload_schema).encode(
                Row(
                    constructor=self._constructor,
                    args=Row(
                        **{
                            f'arg{ix}': arg
                            for (ix, arg) in enumerate(self._args)
                        }),
                    kwargs=Row(**self._kwargs)),
            )))

  @staticmethod
  def from_runner_api_parameter(unused_ptransform, payload, unused_context):
    row = coders.RowCoder(payload.schema).decode(payload.payload)
    maybe_as_dict = lambda x: x._asdict() if x else {}
    return FullyQualifiedNamedTransform(
        row.constructor,
        tuple(getattr(row, 'args', ())),
        maybe_as_dict(getattr(row, 'kwargs', None)))
