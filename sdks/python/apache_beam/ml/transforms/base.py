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

from typing import Generic
from typing import TypeVar

import apache_beam as beam

# TODO: Abstract methods are not getting pickled with dill.
# https://github.com/uqfoundation/dill/issues/332
# import abc

__all__ = ['MLTransform']

TransformedDatasetT = TypeVar('TransformedDatasetT')
TransformedMetadataT = TypeVar('TransformedMetadataT')

# Input/Output types to the MLTransform.
ExampleT = TypeVar('ExampleT')
MLTransformOutputT = TypeVar('MLTransformOutputT')

# Input to the process data. This could be same or different from ExampleT.
ProcessInputT = TypeVar('ProcessInputT')
# Output of the process data. This could be same or different
# from MLTransformOutputT
ProcessOutputT = TypeVar('ProcessOutputT')

# Input to the apply() method of BaseOperation.
OperationInputT = TypeVar('OperationInputT')
# Output of the apply() method of BaseOperation.
OperationOutputT = TypeVar('OperationOutputT')


class BaseOperation(Generic[OperationInputT, OperationOutputT]):
  def apply(
      self, inputs: OperationInputT, column_name: str, *args,
      **kwargs) -> OperationOutputT:
    """
    Define any processing logic in the apply() method.
    processing logics are applied on inputs and returns a transformed
    output.
    Args:
      inputs: input data.
    """
    raise NotImplementedError


class _ProcessHandler(Generic[ProcessInputT, ProcessOutputT]):
  """
  Only for internal use. No backwards compatibility guarantees.
  """
  def process_data(
      self, pcoll: beam.PCollection[ProcessInputT]
  ) -> beam.PCollection[ProcessOutputT]:
    """
    Logic to process the data. This will be the entrypoint in
    beam.MLTransform to process incoming data.
    """
    raise NotImplementedError

  def append_transform(self, transform: BaseOperation):
    raise NotImplementedError


class MLTransform(beam.PTransform[beam.PCollection[ExampleT],
                                  beam.PCollection[MLTransformOutputT]],
                  Generic[ExampleT, MLTransformOutputT, ]):
  def __init__(
      self,
      process_handler: _ProcessHandler[ExampleT, MLTransformOutputT],
  ):
    """
    Args:
      process_handler: A _ProcessHandler instance that defines the logic to
        process the data.
    """
    self._process_handler = process_handler

  def expand(
      self, pcoll: beam.PCollection[ExampleT]
  ) -> beam.PCollection[MLTransformOutputT]:
    """
    This is the entrypoint for the MLTransform. This method will
    invoke the process_data() method of the _ProcessHandler instance
    to process the incoming data.

    process_data takes in a PCollection and applies the PTransforms
    necessary to process the data and returns a PCollection of
    transformed data.
    Args:
      pcoll: A PCollection of ExampleT type.
    Returns:
      A PCollection of MLTransformOutputT type.
    """
    return self._process_handler.process_data(pcoll)

  def with_transform(self, transform: BaseOperation):
    """
    Add a transform to the MLTransform pipeline.
    Args:
      transform: A BaseOperation instance.
    Returns:
      A MLTransform instance.
    """
    self._process_handler.append_transform(transform)
    return self
