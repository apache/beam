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

import abc
import functools
import logging
import os
import tempfile
import uuid
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Generic
from typing import Iterable
from typing import List
from typing import Optional
from typing import TypeVar
from typing import Union
from typing import cast

import jsonpickle
import numpy as np

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics.metric import Metrics
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import ModelT
from apache_beam.ml.inference.base import RunInferenceDLQ
from apache_beam.options.pipeline_options import PipelineOptions

_LOGGER = logging.getLogger(__name__)
_ATTRIBUTE_FILE_NAME = 'attributes.json'

__all__ = [
    'MLTransform',
    'ProcessHandler',
    'MLTransformProvider',
    'BaseOperation',
    'EmbeddingsManager'
]

TransformedDatasetT = TypeVar('TransformedDatasetT')
TransformedMetadataT = TypeVar('TransformedMetadataT')

# Input/Output types to the MLTransform.
MLTransformOutputT = TypeVar('MLTransformOutputT')
ExampleT = TypeVar('ExampleT')

# Input to the apply() method of BaseOperation.
OperationInputT = TypeVar('OperationInputT')
# Output of the apply() method of BaseOperation.
OperationOutputT = TypeVar('OperationOutputT')

# Input to the EmbeddingTypeAdapter input_fn
EmbeddingTypeAdapterInputT = TypeVar(
    'EmbeddingTypeAdapterInputT')  # e.g., Chunk
# Output of the EmbeddingTypeAdapter output_fn
EmbeddingTypeAdapterOutputT = TypeVar(
    'EmbeddingTypeAdapterOutputT')  # e.g., Embedding


@dataclass
class EmbeddingTypeAdapter(Generic[EmbeddingTypeAdapterInputT,
                                   EmbeddingTypeAdapterOutputT]):
  """Adapts input types to text for embedding and converts output embeddings.

    Args:
        input_fn: Function to extract text for embedding from input type
        output_fn: Function to create output type from input and embeddings
    """
  input_fn: Callable[[Sequence[EmbeddingTypeAdapterInputT]], List[str]]
  output_fn: Callable[[Sequence[EmbeddingTypeAdapterInputT], Sequence[Any]],
                      List[EmbeddingTypeAdapterOutputT]]

  def __reduce__(self):
    """Custom serialization that preserves type information during
    jsonpickle."""
    return (self.__class__, (self.input_fn, self.output_fn))


def _map_errors_to_beam_row(element, cls_name=None):
  row_elements = {
      'element': element[0],
      'msg': str(element[1][1]),
      'stack': str(element[1][2]),
  }
  if cls_name is not None:
    row_elements['transform_name'] = cls_name
  return beam.Row(**row_elements)


class ArtifactMode(object):
  PRODUCE = 'produce'
  CONSUME = 'consume'


class MLTransformProvider:
  """
  Data processing transforms that are intended to be used with MLTransform
  should subclass MLTransformProvider and implement
  get_ptransform_for_processing().

  get_ptransform_for_processing() method should return a PTransform that can be
  used to process the data.

  """
  @abc.abstractmethod
  def get_ptransform_for_processing(self, **kwargs) -> beam.PTransform:
    """
    Returns a PTransform that can be used to process the data.
    """

  def get_counter(self):
    """
    Returns the counter name for the data processing transform.
    """
    counter_name = self.__class__.__name__
    return Metrics.counter(MLTransform, f'BeamML_{counter_name}')


class BaseOperation(Generic[OperationInputT, OperationOutputT],
                    MLTransformProvider,
                    abc.ABC):
  def __init__(self, columns: list[str]) -> None:
    """
    Base Opertation class data processing transformations.
    Args:
      columns: List of column names to apply the transformation.
    """
    self.columns = columns

  @abc.abstractmethod
  def apply_transform(self, data: OperationInputT,
                      output_column_name: str) -> dict[str, OperationOutputT]:
    """
    Define any processing logic in the apply_transform() method.
    processing logics are applied on inputs and returns a transformed
    output.
    Args:
      inputs: input data.
    """

  def __call__(self, data: OperationInputT,
               output_column_name: str) -> dict[str, OperationOutputT]:
    """
    This method is called when the instance of the class is called.
    This method will invoke the apply() method of the class.
    """
    transformed_data = self.apply_transform(data, output_column_name)
    return transformed_data


class ProcessHandler(
    beam.PTransform[beam.PCollection[ExampleT],
                    Union[beam.PCollection[MLTransformOutputT],
                          tuple[beam.PCollection[MLTransformOutputT],
                                beam.PCollection[beam.Row]]]],
    abc.ABC):
  """
  Only for internal use. No backwards compatibility guarantees.
  """
  @abc.abstractmethod
  def append_transform(self, transform: BaseOperation):
    """
    Append transforms to the ProcessHandler.
    """


def _dict_input_fn(columns: Sequence[str],
                   batch: Sequence[Dict[str, Any]]) -> List[str]:
  """Extract text from specified columns in batch."""
  if not batch or not isinstance(batch[0], dict):
    raise TypeError(
        'Expected data to be dicts, got '
        f'{type(batch[0])} instead.')

  result = []
  expected_keys = set(batch[0].keys())
  expected_columns = set(columns)
  # Process one batch item at a time
  for item in batch:
    item_keys = item.keys()
    if set(item_keys) != expected_keys:
      extra_keys = item_keys - expected_keys
      missing_keys = expected_keys - item_keys
      raise RuntimeError(
          f'All dicts in batch must have the same keys. '
          f'extra keys: {extra_keys}, '
          f'missing keys: {missing_keys}')
    missing_columns = expected_columns - item_keys
    if (missing_columns):
      raise RuntimeError(
          f'Data does not contain the following columns '
          f': {missing_columns}.')

    # Get all columns for this item
    for col in columns:
      result.append(item[col])
  return result


def _dict_output_fn(
    columns: Sequence[str],
    batch: Sequence[Dict[str, Any]],
    embeddings: Sequence[Any]) -> List[Dict[str, Any]]:
  """Map embeddings back to columns in batch."""
  result = []
  for batch_idx, item in enumerate(batch):
    for col_idx, col in enumerate(columns):
      embedding_idx = batch_idx * len(columns) + col_idx
      item[col] = embeddings[embedding_idx]
    result.append(item)
  return result


def _create_dict_adapter(
    columns: List[str]) -> EmbeddingTypeAdapter[Dict[str, Any], Dict[str, Any]]:
  """Create adapter for dict-based processing."""
  return EmbeddingTypeAdapter[Dict[str, Any], Dict[str, Any]](
      input_fn=cast(
          Callable[[Sequence[Dict[str, Any]]], List[str]],
          functools.partial(_dict_input_fn, columns)),
      output_fn=cast(
          Callable[[Sequence[Dict[str, Any]], Sequence[Any]],
                   List[Dict[str, Any]]],
          functools.partial(_dict_output_fn, columns)))


# TODO:https://github.com/apache/beam/issues/29356
#  Add support for inference_fn
class EmbeddingsManager(MLTransformProvider):
  def __init__(
      self,
      *,
      columns: Optional[list[str]] = None,
      type_adapter: Optional[EmbeddingTypeAdapter] = None,
      # common args for all ModelHandlers.
      load_model_args: Optional[dict[str, Any]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      large_model: bool = False,
      **kwargs):
    self.load_model_args = load_model_args or {}
    self.min_batch_size = min_batch_size
    self.max_batch_size = max_batch_size
    self.large_model = large_model
    self.columns = columns
    if columns is not None:
      self.type_adapter = _create_dict_adapter(columns)
    elif type_adapter is not None:
      self.type_adapter = type_adapter
    else:
      raise ValueError("Either columns or type_adapter must be specified")
    self.inference_args = kwargs.pop('inference_args', {})

    if kwargs:
      _LOGGER.warning("Ignoring the following arguments: %s", kwargs.keys())

  # TODO:https://github.com/apache/beam/pull/29564 add set_model_handler method
  @abc.abstractmethod
  def get_model_handler(self) -> ModelHandler:
    """
    Return framework specific model handler.
    """

  def get_columns_to_apply(self):
    return self.columns


class MLTransform(
    beam.PTransform[beam.PCollection[ExampleT],
                    Union[beam.PCollection[MLTransformOutputT],
                          tuple[beam.PCollection[MLTransformOutputT],
                                beam.PCollection[beam.Row]]]],
    Generic[ExampleT, MLTransformOutputT]):
  def __init__(
      self,
      *,
      write_artifact_location: Optional[str] = None,
      read_artifact_location: Optional[str] = None,
      transforms: Optional[list[MLTransformProvider]] = None):
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
      if transforms:
        raise ValueError(
            'Transforms should not be passed in read mode. In read mode, '
            'the transforms are read from the artifact location.')

    else:
      artifact_location = write_artifact_location  # type: ignore[assignment]
      artifact_mode = ArtifactMode.PRODUCE

    self._parent_artifact_location = artifact_location

    self._artifact_mode = artifact_mode
    self.transforms = transforms or []
    self._counter = Metrics.counter(
        MLTransform, f'BeamML_{self.__class__.__name__}')
    self._with_exception_handling = False
    self._exception_handling_args: dict[str, Any] = {}

  def expand(
      self, pcoll: beam.PCollection[ExampleT]
  ) -> Union[beam.PCollection[MLTransformOutputT],
             tuple[beam.PCollection[MLTransformOutputT],
                   beam.PCollection[beam.Row]]]:
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
    upstream_errors = []
    _ = [self._validate_transform(transform) for transform in self.transforms]
    if self._artifact_mode == ArtifactMode.PRODUCE:
      ptransform_partitioner = _MLTransformToPTransformMapper(
          transforms=self.transforms,
          artifact_location=self._parent_artifact_location,
          artifact_mode=self._artifact_mode,
          pipeline_options=pcoll.pipeline.options)
      ptransform_list = ptransform_partitioner.create_and_save_ptransform_list()
    else:
      ptransform_list = (
          _MLTransformToPTransformMapper.load_transforms_from_artifact_location(
              self._parent_artifact_location))

      # the saved transforms has artifact mode set to PRODUCE.
      # set the artifact mode to CONSUME.
      for i in range(len(ptransform_list)):
        if hasattr(ptransform_list[i], 'artifact_mode'):
          ptransform_list[i].artifact_mode = self._artifact_mode

    transform_name = None
    for ptransform in ptransform_list:
      if self._with_exception_handling:
        if hasattr(ptransform, 'with_exception_handling'):
          ptransform = ptransform.with_exception_handling(
              **self._exception_handling_args)
          pcoll, bad_results = pcoll | ptransform
          # RunInference outputs a RunInferenceDLQ instead of a PCollection.
          # since TFTProcessHandler and RunInferene are supported, try to infer
          # the type of bad_results and append it to the list of errors.
          if isinstance(bad_results, RunInferenceDLQ):
            bad_results = bad_results.failed_inferences
            transform_name = ptransform.annotations()['model_handler']
          elif not isinstance(bad_results, beam.PCollection):
            raise NotImplementedError(
                f'Unexpected type for bad_results: {type(bad_results)}')
          bad_results = bad_results | beam.Map(
              lambda x: _map_errors_to_beam_row(x, transform_name))
          upstream_errors.append(bad_results)

      else:
        pcoll = pcoll | ptransform
    _ = (
        pcoll.pipeline
        | "MLTransformMetricsUsage" >> MLTransformMetricsUsage(self))

    if self._with_exception_handling:
      bad_pcoll = (upstream_errors | beam.Flatten())
      return pcoll, bad_pcoll  # type: ignore[return-value]
    return pcoll  # type: ignore[return-value]

  def with_transform(self, transform: MLTransformProvider):
    """
    Add a transform to the MLTransform pipeline.
    Args:
      transform: A BaseOperation instance.
    Returns:
      A MLTransform instance.
    """
    self._validate_transform(transform)
    self.transforms.append(transform)
    return self

  def _validate_transform(self, transform):
    if not isinstance(transform, MLTransformProvider):
      raise TypeError(
          'transform must be a subclass of BaseOperation. '
          'Got: %s instead.' % type(transform))

  def with_exception_handling(
      self, *, exc_class=Exception, use_subprocess=False, threshold=1):
    self._with_exception_handling = True
    self._exception_handling_args = {
        'exc_class': exc_class,
        'use_subprocess': use_subprocess,
        'threshold': threshold
    }
    return self


class MLTransformMetricsUsage(beam.PTransform):
  def __init__(self, ml_transform: MLTransform):
    self._ml_transform = ml_transform
    self._ml_transform._counter.inc()

  def expand(self, pipeline):
    def _increment_counters():
      # increment for MLTransform.
      self._ml_transform._counter.inc()
      # increment if data processing transforms are passed.
      transforms = self._ml_transform.transforms
      if transforms:
        for transform in transforms:
          transform.get_counter().inc()

    _ = (
        pipeline
        | beam.Create([None])
        | beam.Map(lambda _: _increment_counters()))


class _TransformAttributeManager:
  """
  Base class used for saving and loading the attributes.
  """
  @staticmethod
  def save_attributes(artifact_location):
    """
    Save the attributes to json file using stdlib json.
    """
    raise NotImplementedError

  @staticmethod
  def load_attributes(artifact_location):
    """
    Load the attributes from json file.
    """
    raise NotImplementedError


class _JsonPickleTransformAttributeManager(_TransformAttributeManager):
  """
  Use Jsonpickle to save and load the attributes. Here the attributes refer
  to the list of PTransforms that are used to process the data.

  jsonpickle is used to serialize the PTransforms and save it to a json file and
  is compatible across python versions.
  """
  @staticmethod
  def _is_remote_path(path):
    is_gcs = path.find('gs://') != -1
    # TODO:https://github.com/apache/beam/issues/29356
    #  Add support for other remote paths.
    if not is_gcs and path.find('://') != -1:
      raise RuntimeError(
          "Artifact locations are currently supported for only available for "
          "local paths and GCS paths. Got: %s" % path)
    return is_gcs

  @staticmethod
  def save_attributes(
      ptransform_list,
      artifact_location,
      **kwargs,
  ):
    # if an artifact location is present, instead of overwriting the
    # existing file, raise an error since the same artifact location
    # can be used by multiple beam jobs and this could result in undesired
    # behavior.
    if FileSystems.exists(FileSystems.join(artifact_location,
                                           _ATTRIBUTE_FILE_NAME)):
      raise FileExistsError(
          "The artifact location %s already exists and contains %s. Please "
          "specify a different location." %
          (artifact_location, _ATTRIBUTE_FILE_NAME))

    if _JsonPickleTransformAttributeManager._is_remote_path(artifact_location):
      temp_dir = tempfile.mkdtemp()
      temp_json_file = os.path.join(temp_dir, _ATTRIBUTE_FILE_NAME)
      with open(temp_json_file, 'w+') as f:
        f.write(jsonpickle.encode(ptransform_list))
      with open(temp_json_file, 'rb') as f:
        from apache_beam.runners.dataflow.internal import apiclient
        _LOGGER.info('Creating artifact location: %s', artifact_location)
        # pipeline options required to for the client to configure project.
        options = kwargs.get('options')
        try:
          apiclient.DataflowApplicationClient(options=options).stage_file(
              gcs_or_local_path=artifact_location,
              file_name=_ATTRIBUTE_FILE_NAME,
              stream=f,
              mime_type='application/json')
        except Exception as exc:
          if not options:
            raise RuntimeError(
                "Failed to create Dataflow client. "
                "Pipeline options are required to save the attributes."
                "in the artifact location %s" % artifact_location) from exc
          raise
    else:
      if not FileSystems.exists(artifact_location):
        FileSystems.mkdirs(artifact_location)
      # FileSystems.open() fails if the file does not exist.
      with open(os.path.join(artifact_location, _ATTRIBUTE_FILE_NAME),
                'w+') as f:
        f.write(jsonpickle.encode(ptransform_list))

  @staticmethod
  def load_attributes(artifact_location):
    with FileSystems.open(os.path.join(artifact_location, _ATTRIBUTE_FILE_NAME),
                          'rb') as f:
      return jsonpickle.decode(f.read())


_transform_attribute_manager = _JsonPickleTransformAttributeManager


class _MLTransformToPTransformMapper:
  """
  This class takes in a list of data processing transforms compatible to be
  wrapped around MLTransform and returns a list of PTransforms that are used to
  run the data processing transforms.

  The _MLTransformToPTransformMapper is responsible for loading and saving the
  PTransforms or attributes of PTransforms to the artifact location to seal
  the gap between the training and inference pipelines.
  """
  def __init__(
      self,
      transforms: list[MLTransformProvider],
      artifact_location: str,
      artifact_mode: str = ArtifactMode.PRODUCE,
      pipeline_options: Optional[PipelineOptions] = None,
  ):
    self.transforms = transforms
    self._parent_artifact_location = artifact_location
    self.artifact_mode = artifact_mode
    self.pipeline_options = pipeline_options

  def create_and_save_ptransform_list(self):
    ptransform_list = self.create_ptransform_list()
    self.save_transforms_in_artifact_location(ptransform_list)
    return ptransform_list

  def create_ptransform_list(self):
    previous_ptransform_type = None
    current_ptransform = None
    ptransform_list = []
    for transform in self.transforms:
      if not isinstance(transform, MLTransformProvider):
        raise RuntimeError(
            'Transforms must be instances of MLTransformProvider and '
            'implement get_ptransform_for_processing() method.')
      # for each instance of PTransform, create a new artifact location
      current_ptransform = transform.get_ptransform_for_processing(
          artifact_location=os.path.join(
              self._parent_artifact_location, uuid.uuid4().hex[:6]),
          artifact_mode=self.artifact_mode)
      append_transform = hasattr(current_ptransform, 'append_transform')
      if (type(current_ptransform) !=
          previous_ptransform_type) or not append_transform:
        ptransform_list.append(current_ptransform)
        previous_ptransform_type = type(current_ptransform)
      # If different PTransform is appended to the list and the PTransform
      # supports append_transform, append the transform to the PTransform.
      if append_transform:
        ptransform_list[-1].append_transform(transform)
    return ptransform_list

  def save_transforms_in_artifact_location(self, ptransform_list):
    """
    Save the ptransform references to json file.
    """
    _transform_attribute_manager.save_attributes(
        ptransform_list=ptransform_list,
        artifact_location=self._parent_artifact_location,
        options=self.pipeline_options)

  @staticmethod
  def load_transforms_from_artifact_location(artifact_location):
    return _transform_attribute_manager.load_attributes(artifact_location)


class _EmbeddingHandler(ModelHandler):
  """
  A ModelHandler intended to be work on list[dict[str, Any]] inputs.

  The inputs to the model handler are expected to be a list of dicts.

  For example, if the original mode is used with RunInference to take a
  PCollection[E] to a PCollection[P], this ModelHandler would take a
  PCollection[dict[str, E]] to a PCollection[dict[str, P]].

  _EmbeddingHandler will accept an EmbeddingsManager instance, which
  contains the details of the model to be loaded and the inference_fn to be
  used. The purpose of _EmbeddingHandler is to generate embeddings for
  general inputs using the EmbeddingsManager instance.

  This is an internal class and offers no backwards compatibility guarantees.

  Args:
    embeddings_manager: An EmbeddingsManager instance.
  """
  def __init__(self, embeddings_manager: EmbeddingsManager):
    self.embedding_config = embeddings_manager
    self._underlying = self.embedding_config.get_model_handler()
    self.columns = self.embedding_config.get_columns_to_apply()

  def load_model(self):
    model = self._underlying.load_model()
    return model

  def _validate_column_data(self, batch):
    pass

  def _validate_batch(self, batch: Sequence[dict[str, Any]]):
    if not batch or not isinstance(batch[0], dict):
      raise TypeError(
          'Expected data to be dicts, got '
          f'{type(batch[0])} instead.')

  def run_inference(
      self,
      batch: Sequence[dict[str, list[str]]],
      model: ModelT,
      inference_args: Optional[dict[str, Any]] = None,
  ) -> list[dict[str, Union[list[float], list[str]]]]:
    """
    Runs inference on a batch of text inputs. The inputs are expected to be
    a list of dicts. Each dict should have the same keys, and the shape
    should be of the same size for a single key across the batch.
    """
    embedding_input = self.embedding_config.type_adapter.input_fn(batch)
    self._validate_column_data(batch=embedding_input)
    prediction = self._underlying.run_inference(
        embedding_input, model, inference_args)
    # Convert prediction to Sequence[Any]
    if isinstance(prediction, np.ndarray):
      prediction_seq = prediction.tolist()
    elif isinstance(prediction, Iterable) and not isinstance(prediction,
                                                             (str, bytes)):
      prediction_seq = list(prediction)
    else:
      prediction_seq = [prediction]
    return self.embedding_config.type_adapter.output_fn(batch, prediction_seq)

  def get_metrics_namespace(self) -> str:
    return (
        self._underlying.get_metrics_namespace() or 'BeamML_EmbeddingHandler')

  def batch_elements_kwargs(self) -> Mapping[str, Any]:
    batch_sizes_map = {}
    if self.embedding_config.max_batch_size:
      batch_sizes_map['max_batch_size'] = self.embedding_config.max_batch_size
    if self.embedding_config.min_batch_size:
      batch_sizes_map['min_batch_size'] = self.embedding_config.min_batch_size
    return (self._underlying.batch_elements_kwargs() or batch_sizes_map)

  def __repr__(self):
    return self._underlying.__repr__()

  def validate_inference_args(self, _):
    pass


class _TextEmbeddingHandler(_EmbeddingHandler):
  """
  A ModelHandler intended to be work on list[dict[str, str]] inputs.

  The inputs to the model handler are expected to be a list of dicts.

  For example, if the original mode is used with RunInference to take a
  PCollection[E] to a PCollection[P], this ModelHandler would take a
  PCollection[dict[str, E]] to a PCollection[dict[str, P]].

  _TextEmbeddingHandler will accept an EmbeddingsManager instance, which
  contains the details of the model to be loaded and the inference_fn to be
  used. The purpose of _TextEmbeddingHandler is to generate embeddings for
  text inputs using the EmbeddingsManager instance.

  If the input is not a text column, a RuntimeError will be raised.

  This is an internal class and offers no backwards compatibility guarantees.

  Args:
    embeddings_manager: An EmbeddingsManager instance.
  """
  def _validate_column_data(self, batch):
    if not isinstance(batch[0], (str, bytes)):
      raise TypeError(
          'Embeddings can only be generated on dict[str, str].'
          f'Got dict[str, {type(batch[0])}] instead.')

  def get_metrics_namespace(self) -> str:
    return (
        self._underlying.get_metrics_namespace() or
        'BeamML_TextEmbeddingHandler')


class _ImageEmbeddingHandler(_EmbeddingHandler):
  """
  A ModelHandler intended to be work on list[dict[str, Image]] inputs.

  The inputs to the model handler are expected to be a list of dicts.

  For example, if the original mode is used with RunInference to take a
  PCollection[E] to a PCollection[P], this ModelHandler would take a
  PCollection[dict[str, E]] to a PCollection[dict[str, P]].

  _ImageEmbeddingHandler will accept an EmbeddingsManager instance, which
  contains the details of the model to be loaded and the inference_fn to be
  used. The purpose of _ImageEmbeddingHandler is to generate embeddings for
  image inputs using the EmbeddingsManager instance.

  If the input is not an Image representation column, a RuntimeError will be
  raised.

  This is an internal class and offers no backwards compatibility guarantees.

  Args:
    embeddings_manager: An EmbeddingsManager instance.
  """
  def _validate_column_data(self, batch):
    # Don't want to require framework-specific imports
    # here, so just catch columns of primatives for now.
    if isinstance(batch[0], (int, str, float, bool)):
      raise TypeError(
          'Embeddings can only be generated on dict[str, Image].'
          f'Got dict[str, {type(batch[0])}] instead.')

  def get_metrics_namespace(self) -> str:
    return (
        self._underlying.get_metrics_namespace() or
        'BeamML_ImageEmbeddingHandler')
