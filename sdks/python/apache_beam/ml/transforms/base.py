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
from typing import List
from typing import Optional
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


class ArtifactMode(object):
  PRODUCE = 'produce'
  CONSUME = 'consume'


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
                  Generic[ExampleT, MLTransformOutputT]):
  def __init__(
      self,
      *,
      artifact_location: str,
      artifact_mode: str = ArtifactMode.PRODUCE,
      transforms: Optional[List[BaseOperation]] = None,
      is_input_record_batches: bool = False,
      output_record_batches: bool = False,
  ):
    """
    Args:
      artifact_location: A storage location for artifacts resulting from
        MLTransform. These artifacts include transformations applied to
        the dataset and generated values like min, max from ScaleTo01,
        and mean, var from ScaleToZScore. Artifacts are produced and stored
        in this location when the `artifact_mode` is set to 'produce'.
        Conversely, when `artifact_mode` is set to 'consume', artifacts are
        retrieved from this location. Note that when consuming artifacts,
        it is not necessary to pass the transforms since they are inherently
        stored within the artifacts themselves. The value assigned to
        `artifact_location` should be a valid storage path where the artifacts
        can be written to or read from.
      transforms: A list of transforms to apply to the data. All the transforms
        are applied in the order they are specified. The input of the
        i-th transform is the output of the (i-1)-th transform. Multi-input
        transforms are not supported yet.
      is_input_record_batches: Whether the input is a RecordBatch.
      output_record_batches: Output RecordBatches instead of beam.Row().
      artifact_mode: Whether to produce or consume artifacts. If set to
        'consume', the handler will assume that the artifacts are already
        computed and stored in the artifact_location. Pass the same artifact
        location that was passed during produce phase to ensure that the
        right artifacts are read. If set to 'produce', the handler
        will compute the artifacts and store them in the artifact_location.
        The artifacts will be read from this location during the consume phase.
        There is no need to pass the transforms in this case since they are
        already embedded in the stored artifacts.
    """
    # avoid circular import
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.ml.transforms.handlers import TFTProcessHandler
    process_handler = TFTProcessHandler(
        artifact_location=artifact_location,
        artifact_mode=artifact_mode,
        transforms=transforms,
        is_input_record_batches=is_input_record_batches,
        output_record_batches=output_record_batches)

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
