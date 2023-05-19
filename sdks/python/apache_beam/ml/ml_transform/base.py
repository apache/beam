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

# pylint: skip-file
import typing
from typing import Dict, Optional, TypeVar, Generic

import apache_beam as beam

# TODO: Abstract methods are not getting pickled with dill.
# https://github.com/uqfoundation/dill/issues/332
# import abc

__all__ = ['MLTransform', 'MLTransformOutput', 'ProcessHandler']

TransformedDatasetT = TypeVar('TransformedDatasetT')
TransformedMetadataT = TypeVar('TransformedMetadataT')

# Input/Output types to the MLTransform.
ExampleT = TypeVar('ExampleT', bound=typing.Union[typing.NamedTuple, beam.Row])
MLTransformOutputT = TypeVar('MLTransformOutputT')

# Input to the process data. This could be same or different from ExampleT.
ProcessInputT = TypeVar('ProcessInputT')
# Output of the process data. This could be same or different from MLTransformOutputT
ProcessOutputT = TypeVar('ProcessOutputT')


class _BaseOperation():

  # @abc.abstractmethod
  def apply(self, inputs, *args, **kwargs):
    """
    Define any preprocessing logic in the apply() method.
    pre-processing logics are applied on inputs and returns a transformed output.

    Args:
      inputs: input data.
    """
    raise NotImplementedError


class MLTransformOutput(typing.NamedTuple):
  transformed_data: TransformedDatasetT
  transformed_metadata: Optional[TransformedMetadataT] = None
  asset_map: Optional[Dict[str, str]] = None


class ProcessHandler(Generic[ProcessInputT, ProcessOutputT]):
  # @abc.abstractmethod
  def process_data(
      self, pcoll: beam.PCollection[ProcessInputT]
  ) -> beam.PCollection[ProcessOutputT]:
    """
    Logic to process the data. This will be the entrypoint in beam.MLTransform to process 
    incoming data:
    """
    raise NotImplementedError


class MLTransform(beam.PTransform[beam.PCollection[ExampleT],
                                  beam.PCollection[MLTransformOutputT]]):
  def __init__(
      self,
      process_handler: ProcessHandler[ProcessInputT, ProcessOutputT],
  ):
    self._process_handler = process_handler

  def expand(
      self, pcoll: beam.PCollection[ExampleT]
  ) -> beam.PCollection[MLTransformOutputT]:
    return self._process_handler.process_data(pcoll)

  def with_transform(self, transform: _BaseOperation):
    self._process_handler._transforms.append(transform)
    return self
