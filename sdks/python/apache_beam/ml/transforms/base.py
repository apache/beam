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
# pylint: skip-file

import abc
import collections
from dataclasses import dataclass
import jsonpickle
import os
from typing import Any, Mapping
from typing import Callable
from typing import Dict
from typing import Generic
from typing import Iterable
from typing import List
from typing import Optional
from typing import Sequence
from typing import TypeVar
from typing import Union
import uuid
import numpy as np

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics.metric import Metrics
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import ModelT
from apache_beam.ml.inference.base import PredictionResult

_ATTRIBUTE_FILE_NAME = 'attributes.json'

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


def _convert_list_of_dicts_to_dict_of_lists(
    list_of_dicts) -> Dict[str, List[Any]]:
  keys_to_element_list = collections.defaultdict(list)
  for d in list_of_dicts:
    for key, value in d.items():
      keys_to_element_list[key].append(value)
  return keys_to_element_list


def _convert_dict_of_lists_to_dict(
    dict_of_lists: Dict[str, List[Any]], batch_length: int) -> Dict[str, Any]:
  result = [{} for _ in range(batch_length)]
  for key, values in dict_of_lists.items():
    for i in range(len(values)):
      result[i][key] = values[i]
  return result


class ArtifactMode(object):
  PRODUCE = 'produce'
  CONSUME = 'consume'


# BaseOperation and EmbeddingsManager are inheriting this class.
class PTransformManager(metaclass=abc.ABCMeta):
  @abc.abstractmethod
  def get_ptransform_for_processing(self, **kwargs) -> beam.PTransform:
    """
    Returns a PTransform that can be used to process the data.
    """

  def requires_chaining(self):
    """
    Returns True if the data processing transforms needs to be
    chained within the respective PTransform.
    """
    return True


class BaseOperation(Generic[OperationInputT, OperationOutputT],
                    PTransformManager,
                    abc.ABC):
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
    pass

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


class ProcessHandler(beam.PTransform[ExampleT, MLTransformOutputT], abc.ABC):
  """
  Only for internal use. No backwards compatibility guarantees.
  """
  # @abc.abstractmethod
  # def process_data(
  #     self, pcoll: beam.PCollection[ExampleT]
  # ) -> beam.PCollection[MLTransformOutputT]:
  #   """
  #   Logic to process the data. This will be the entrypoint in
  #   beam.MLTransform to process incoming data.
  #   """

  @abc.abstractmethod
  def append_transform(self, transform: BaseOperation):
    """
    Append transforms to the ProcessHandler.
    """


# Create abstraction layer for how the MLTransform can split the transforms
# and assings right artifacts/artifact location
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

    self._parent_artifact_location = artifact_location

    self._artifact_mode = artifact_mode
    self._process_handler: Optional[ProcessHandler] = None
    self.transforms = transforms or []
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
    if self._artifact_mode == ArtifactMode.PRODUCE:
      ptransform_partitioner = TransformsPartitioner(
          transforms=self.transforms,
          artifact_location=self._parent_artifact_location,
          artifact_mode=self._artifact_mode)
      ptransform_list = ptransform_partitioner.create_and_save_ptransform_list()
    else:
      ptransform_list = (
          TransformsPartitioner.load_transforms_from_artifact_location(
              self._parent_artifact_location))

    # the saved transforms has artifact mode set to PRODUCE.
    # set the artifact mode to CONSUME.
    if self._artifact_mode == ArtifactMode.CONSUME:
      for i in range(len(ptransform_list)):
        if hasattr(ptransform_list[i], 'artifact_mode'):
          ptransform_list[i].artifact_mode = self._artifact_mode

    for ptransform in ptransform_list:
      pcoll = pcoll | ptransform

    _ = (
        pcoll.pipeline
        | "MLTransformMetricsUsage" >> MLTransformMetricsUsage(self))
    return pcoll

  def with_transform(self, transform: BaseOperation):
    """
    Add a transform to the MLTransform pipeline.
    Args:
      transform: A BaseOperation instance.
    Returns:
      A MLTransform instance.
    """
    # self._validate_transform(transform)
    # avoid circular import
    # pylint: disable=wrong-import-order, wrong-import-position
    # TODO: raise an error when both transforms and with_transform() are used.
    self.transforms.append(transform)
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
          self._ml_transform._process_handler.transforms
          if self._ml_transform._process_handler else None)
      if transforms:
        for transform in transforms:
          transform.get_counter().inc()

    _ = (
        pipeline
        | beam.Create([None])
        | beam.Map(lambda _: _increment_counters()))


class EmbeddingsManager(PTransformManager):
  def __init__(
      self,
      device: str = 'CPU',
      inference_fn: Optional[Callable[..., Iterable[PredictionResult]]] = None,
      load_model_args: Optional[Dict[str, Any]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      large_model: bool = False,
      columns: Optional[List[str]] = None,
      **kwargs):
    self.device = device
    self.inference_fn = inference_fn
    self.load_model_args = load_model_args or {}
    self.min_batch_size = min_batch_size
    self.max_batch_size = max_batch_size
    self.large_model = large_model
    self.kwargs = kwargs
    self.columns = columns

  @abc.abstractmethod
  def get_model_handler(self) -> ModelHandler:
    """
    Return framework specific model handler.
    """

  def set_model_handler(self, model_handler: ModelHandler) -> None:
    """
    Set framework specific model handler.
    """
    self.model_handler = model_handler

  def requires_chaining(self):
    # each embedding config requires a separate PTransform. so no chaining.
    return False


class TransformAttributeManager:
  def save_attributes(self, artifact_location):
    """
    Save the attributes to json file using stdlib json.
    """
    raise NotImplementedError

  def load_attributes(self, artifact_location):
    """
    Load the attributes from json file.
    """
    raise NotImplementedError


class TransformsPartitioner:
  def __init__(
      self,
      transforms: List[Union[BaseOperation, EmbeddingsManager]],
      artifact_location,
      artifact_mode):
    self.transforms = transforms
    self._parent_artifact_location = artifact_location
    self.artifact_mode = artifact_mode

  def create_and_save_ptransform_list(self):
    self.ptransform_list = self.create_ptransform_list()
    self.save_transforms_in_artifact_location()
    return self.ptransform_list

  def create_ptransform_list(self):
    previous_ptransform = None
    current_ptransform = None
    ptransform_list = []
    for transform in self.transforms:
      if not isinstance(transform, PTransformManager):
        raise RuntimeError(
            'Transforms must inherit of PTransformManager class and '
            'implement get_ptransform_for_processing() method.')
      # for each instance of PTransform, create a new artifact location
      current_ptransform = transform.get_ptransform_for_processing(
          artifact_location=os.path.join(
              self._parent_artifact_location, uuid.uuid4().hex[:6]),
          artifact_mode=self.artifact_mode)
      if (type(current_ptransform) != type(previous_ptransform) or
          not transform.requires_chaining()):
        ptransform_list.append(current_ptransform)
        previous_ptransform = current_ptransform

      if hasattr(ptransform_list[-1], 'append_transform'):
        ptransform_list[-1].append_transform(transform)

    return ptransform_list

  def save_transforms_in_artifact_location(self):
    """
    Save the ptransform references to json file.
    """
    with open(os.path.join(self._parent_artifact_location,
                           _ATTRIBUTE_FILE_NAME),
              'w+') as f:
      f.write(jsonpickle.encode(self.ptransform_list))

  @staticmethod
  def load_transforms_from_artifact_location(artifact_location):
    with open(os.path.join(artifact_location, _ATTRIBUTE_FILE_NAME), 'r') as f:
      return jsonpickle.decode(f.read())


# TODO: Should this prefixed with underscore?
class TextEmbeddingHandler(ModelHandler):
  """
  A ModelHandler intended to be work on text inputs.

  The inputs to the model handler are expected to be a list of dicts.

  For example, if the original mode is used with RunInference to take a
  PCollection[E] to a PCollection[P], this ModelHandler would take a
  PCollection[Dict[str, E]] to a PCollection[Dict[str, P]].

  TextEmbeddingHandler will accept an EmbeddingsManager instance, which
  contains the details of the model to be loaded and the inference_fn to be
  used. The purpose of TextEmbeddingHandler is to generate embeddings for
  text inputs using the EmbeddingsManager instance.

  If the input is not a text column, a RuntimeError will be raised.

  This is an internal class and offers no backwards compatibility guarantees.

  Args:
    embedding_config: An EmbeddingsManager instance.
  """
  def __init__(self, embedding_config: EmbeddingsManager):
    self.embedding_config = embedding_config
    self._underlying = self.embedding_config.get_model_handler()
    # these are the columns on which embeddings should be generated.
    self.columns = self.embedding_config.columns

  def load_model(self):
    model = self._underlying.load_model()
    return model

  def run_inference(
      self,
      batch: Sequence[Dict[str, List[str]]],
      model: ModelT,
      inference_args: Optional[Dict[str, Any]] = None,
  ) -> Iterable[PredictionResult]:
    """
    Runs inference on a batch of text inputs. The inputs are expected to be
    a list of dicts. Each dict should have the same keys, and the shape
    should be of the same size for a single key across the batch.
    """
    # Works because we restricted the batch size to be 1.
    batch_len = len(batch)
    dict_batch = _convert_list_of_dicts_to_dict_of_lists(list_of_dicts=batch)
    result = collections.defaultdict(list)
    for key, batch in dict_batch.items():
      if key in self.columns:
        if not isinstance(batch[0], (str, bytes)):
          raise RuntimeError(
              'Embeddings can only be generated on text columns.')
        prediction = self._underlying.run_inference(
            batch=batch, model=model, inference_args=inference_args)
        if isinstance(prediction, np.ndarray):
          prediction = prediction.tolist()
          result[key] = prediction
        else:
          result[key] = prediction
      else:
        result[key] = batch
    result = _convert_dict_of_lists_to_dict(
        dict_of_lists=result, batch_length=batch_len)
    return result
