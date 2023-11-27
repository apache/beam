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

# pytype: skip-file

import abc
from typing import Dict
from typing import Generic
from typing import List
from typing import Optional
from typing import Sequence
from typing import TypeVar

import apache_beam as beam
from apache_beam.metrics.metric import Metrics

__all__ = ['MLTransform', 'ProcessHandler', 'BaseOperation']

TransformedDatasetT = TypeVar('TransformedDatasetT')
TransformedMetadataT = TypeVar('TransformedMetadataT')

# Input/Output types to the MLTransform.
MLTransformOutputT = TypeVar('MLTransformOutputT')
ExampleT = TypeVar('ExampleT')

# Input to the apply() method of BaseOperation.
OperationInputT = TypeVar('OperationInputT')
# Output of the apply() method of BaseOperation.
OperationOutputT = TypeVar('OperationOutputT')


class ArtifactMode(object):
  PRODUCE = 'produce'
  CONSUME = 'consume'


class BaseOperation(Generic[OperationInputT, OperationOutputT], abc.ABC):
  def __init__(self, columns: List[str]) -> None:
    """
    Base Opertation class data processing transformations.
    Args:
      columns: List of column names to apply the transformation.
    """
    self.columns = columns

  @abc.abstractmethod
  def apply_transform(self, data: OperationInputT,
                      output_column_name: str) -> Dict[str, OperationOutputT]:
    """
    Define any processing logic in the apply_transform() method.
    processing logics are applied on inputs and returns a transformed
    output.
    Args:
      inputs: input data.
    """

  def __call__(self, data: OperationInputT,
               output_column_name: str) -> Dict[str, OperationOutputT]:
    """
    This method is called when the instance of the class is called.
    This method will invoke the apply() method of the class.
    """
    transformed_data = self.apply_transform(data, output_column_name)
    return transformed_data

  def get_counter(self):
    """
    Returns the counter name for the operation.
    """
    counter_name = self.__class__.__name__
    return Metrics.counter(MLTransform, f'BeamML_{counter_name}')


class ProcessHandler(Generic[ExampleT, MLTransformOutputT], abc.ABC):
  """
  Only for internal use. No backwards compatibility guarantees.
  """
  @abc.abstractmethod
  def process_data(
      self, pcoll: beam.PCollection[ExampleT]
  ) -> beam.PCollection[MLTransformOutputT]:
    """
    Logic to process the data. This will be the entrypoint in
    beam.MLTransform to process incoming data.
    """

  @abc.abstractmethod
  def append_transform(self, transform: BaseOperation):
    """
    Append transforms to the ProcessHandler.
    """


class MLTransform(beam.PTransform[beam.PCollection[ExampleT],
                                  beam.PCollection[MLTransformOutputT]],
                  Generic[ExampleT, MLTransformOutputT]):
  def __init__(
      self,
      *,
      write_artifact_location: Optional[str] = None,
      read_artifact_location: Optional[str] = None,
      transforms: Optional[Sequence[BaseOperation]] = None):
    """
    MLTransform is a Beam PTransform that can be used to apply
    transformations to the data. MLTransform is used to wrap the
    data processing transforms provided by Apache Beam. MLTransform
    works in two modes: write and read. In the write mode,
    MLTransform will apply the transforms to the data and store the
    artifacts in the write_artifact_location. In the read mode,
    MLTransform will read the artifacts from the
    read_artifact_location and apply the transforms to the data. The
    artifact location should be a valid storage path where the artifacts
    can be written to or read from.

    Note that when consuming artifacts, it is not necessary to pass the
    transforms since they are inherently stored within the artifacts
    themselves.

    Args:
      write_artifact_location: A storage location for artifacts resulting from
        MLTransform. These artifacts include transformations applied to
        the dataset and generated values like min, max from ScaleTo01,
        and mean, var from ScaleToZScore. Artifacts are produced and written
        to this location when using `write_artifact_mode`.
        Later MLTransforms can reuse produced artifacts by setting
        `read_artifact_mode` instead of `write_artifact_mode`. The value
        assigned to `write_artifact_location` should be a valid storage
        directory that the artifacts from this transform can be written to.
        If no directory exists at this location, one will be created. This will
        overwrite any artifacts already in this location, so distinct locations
        should be used for each instance of MLTransform. Only one of
        write_artifact_location and read_artifact_location should be specified.
      read_artifact_location: A storage location to read artifacts resulting
        froma previous MLTransform. These artifacts include transformations
        applied to the dataset and generated values like min, max from
        ScaleTo01, and mean, var from ScaleToZScore. Note that when consuming
        artifacts, it is not necessary to pass the transforms since they are
        inherently stored within the artifacts themselves. The value assigned
        to `read_artifact_location` should be a valid storage path where the
        artifacts can be read from. Only one of write_artifact_location and
        read_artifact_location should be specified.
      transforms: A list of transforms to apply to the data. All the transforms
        are applied in the order they are specified. The input of the
        i-th transform is the output of the (i-1)-th transform. Multi-input
        transforms are not supported yet.
    """
    if transforms:
      _ = [self._validate_transform(transform) for transform in transforms]

    if read_artifact_location and write_artifact_location:
      raise ValueError(
          'Only one of read_artifact_location or write_artifact_location can '
          'be specified to initialize MLTransform')

    if not read_artifact_location and not write_artifact_location:
      raise ValueError(
          'Either a read_artifact_location or write_artifact_location must be '
          'specified to initialize MLTransform')

    if read_artifact_location:
      artifact_location = read_artifact_location
      artifact_mode = ArtifactMode.CONSUME
    else:
      artifact_location = write_artifact_location  # type: ignore[assignment]
      artifact_mode = ArtifactMode.PRODUCE

    # avoid circular import
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.ml.transforms.handlers import TFTProcessHandler
    # TODO: When new ProcessHandlers(eg: JaxProcessHandler) are introduced,
    # create a mapping between transforms and ProcessHandler since
    # ProcessHandler is not exposed to the user.
    process_handler: ProcessHandler = TFTProcessHandler(
        artifact_location=artifact_location,
        artifact_mode=artifact_mode,
        transforms=transforms)  # type: ignore[arg-type]

    self._process_handler = process_handler
    self.transforms = transforms
    self._counter = Metrics.counter(
        MLTransform, f'BeamML_{self.__class__.__name__}')

  def expand(
      self, pcoll: beam.PCollection[ExampleT]
  ) -> beam.PCollection[MLTransformOutputT]:
    """
    This is the entrypoint for the MLTransform. This method will
    invoke the process_data() method of the ProcessHandler instance
    to process the incoming data.

    process_data takes in a PCollection and applies the PTransforms
    necessary to process the data and returns a PCollection of
    transformed data.
    Args:
      pcoll: A PCollection of ExampleT type.
    Returns:
      A PCollection of MLTransformOutputT type
    """
    _ = (
        pcoll.pipeline
        | "MLTransformMetricsUsage" >> MLTransformMetricsUsage(self))
    return self._process_handler.process_data(pcoll)

  def with_transform(self, transform: BaseOperation):
    """
    Add a transform to the MLTransform pipeline.
    Args:
      transform: A BaseOperation instance.
    Returns:
      A MLTransform instance.
    """
    self._validate_transform(transform)
    self._process_handler.append_transform(transform)
    return self

  def _validate_transform(self, transform):
    if not isinstance(transform, BaseOperation):
      raise TypeError(
          'transform must be a subclass of BaseOperation. '
          'Got: %s instead.' % type(transform))


class MLTransformMetricsUsage(beam.PTransform):
  def __init__(self, ml_transform: MLTransform):
    self._ml_transform = ml_transform
    self._ml_transform._counter.inc()

  def expand(self, pipeline):
    def _increment_counters():
      # increment for MLTransform.
      self._ml_transform._counter.inc()
      # increment if data processing transforms are passed.
      transforms = (
          self._ml_transform.transforms or
          self._ml_transform._process_handler.transforms)
      if transforms:
        for transform in transforms:
          transform.get_counter().inc()

    _ = (
        pipeline
        | beam.Create([None])
        | beam.Map(lambda _: _increment_counters()))
