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

import collections
import os
import typing
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Union

import numpy as np

import apache_beam as beam
import tensorflow as tf
import tensorflow_transform.beam as tft_beam
from apache_beam.ml.transforms.base import ArtifactMode
from apache_beam.ml.transforms.base import ProcessHandler
from apache_beam.ml.transforms.tft import _EXPECTED_TYPES
from apache_beam.ml.transforms.tft import TFTOperation
from apache_beam.typehints import native_type_compatibility
from apache_beam.typehints.row_type import RowTypeConstraint
from tensorflow_metadata.proto.v0 import schema_pb2
from tensorflow_transform import common_types
from tensorflow_transform.beam.tft_beam_io import beam_metadata_io
from tensorflow_transform.beam.tft_beam_io import transform_fn_io
from tensorflow_transform.tf_metadata import dataset_metadata
from tensorflow_transform.tf_metadata import metadata_io
from tensorflow_transform.tf_metadata import schema_utils

__all__ = [
    'TFTProcessHandler',
]

RAW_DATA_METADATA_DIR = 'raw_data_metadata'
SCHEMA_FILE = 'schema.pbtxt'
# tensorflow transform doesn't support the types other than tf.int64,
# tf.float32 and tf.string.
_default_type_to_tensor_type_map = {
    int: tf.int64,
    float: tf.float32,
    str: tf.string,
    bytes: tf.string,
    np.int64: tf.int64,
    np.int32: tf.int64,
    np.float32: tf.float32,
    np.float64: tf.float32,
    np.bytes_: tf.string,
    np.str_: tf.string,
}
_primitive_types_to_typing_container_type = {
    int: List[int], float: List[float], str: List[str], bytes: List[bytes]
}

tft_process_handler_input_type = typing.Union[typing.NamedTuple,
                                              beam.Row,
                                              Dict[str,
                                                   typing.Union[str,
                                                                float,
                                                                int,
                                                                bytes,
                                                                np.ndarray]]]
tft_process_handler_output_type = typing.Union[beam.Row, Dict[str, np.ndarray]]


class ConvertScalarValuesToListValues(beam.DoFn):
  def process(
      self,
      element,
  ):
    new_dict = {}
    for key, value in element.items():
      if isinstance(value,
                    tuple(_primitive_types_to_typing_container_type.keys())):
        new_dict[key] = [value]
      else:
        new_dict[key] = value
    yield new_dict


class ConvertNamedTupleToDict(
    beam.PTransform[beam.PCollection[typing.Union[beam.Row, typing.NamedTuple]],
                    beam.PCollection[Dict[str,
                                          common_types.InstanceDictType]]]):
  """
    A PTransform that converts a collection of NamedTuples or Rows into a
    collection of dictionaries.
  """
  def expand(
      self, pcoll: beam.PCollection[typing.Union[beam.Row, typing.NamedTuple]]
  ) -> beam.PCollection[common_types.InstanceDictType]:
    """
    Args:
      pcoll: A PCollection of NamedTuples or Rows.
    Returns:
      A PCollection of dictionaries.
    """
    if isinstance(pcoll.element_type, RowTypeConstraint):
      # Row instance
      return pcoll | beam.Map(lambda x: x.as_dict())
    else:
      # named tuple
      return pcoll | beam.Map(lambda x: x._asdict())


class TFTProcessHandler(ProcessHandler[tft_process_handler_input_type,
                                       tft_process_handler_output_type]):
  def __init__(
      self,
      *,
      artifact_location: str,
      transforms: Optional[Sequence[TFTOperation]] = None,
      artifact_mode: str = ArtifactMode.PRODUCE):
    """
    A handler class for processing data with TensorFlow Transform (TFT)
    operations.
    """
    self.transforms = transforms if transforms else []
    self.transformed_schema: Dict[str, type] = {}
    self.artifact_location = artifact_location
    self.artifact_mode = artifact_mode
    if artifact_mode not in ['produce', 'consume']:
      raise ValueError('artifact_mode must be either `produce` or `consume`.')

  def append_transform(self, transform):
    self.transforms.append(transform)

  def _map_column_names_to_types(self, row_type):
    """
    Return a dictionary of column names and types.
    Args:
      element_type: A type of the element. This could be a NamedTuple or a Row.
    Returns:
      A dictionary of column names and types.
    """
    try:
      if not isinstance(row_type, RowTypeConstraint):
        row_type = RowTypeConstraint.from_user_type(row_type)

      inferred_types = {name: typ for name, typ in row_type._fields}

      for k, t in inferred_types.items():
        if t in _primitive_types_to_typing_container_type:
          inferred_types[k] = _primitive_types_to_typing_container_type[t]

      # sometimes a numpy type can be provided as np.dtype('int64').
      # convert numpy.dtype to numpy type since both are same.
      for name, typ in inferred_types.items():
        if isinstance(typ, np.dtype):
          inferred_types[name] = typ.type

      return inferred_types
    except:  # pylint: disable=bare-except
      return {}

  def _map_column_names_to_types_from_transforms(self):
    column_type_mapping = {}
    for transform in self.transforms:
      for col in transform.columns:
        if col not in column_type_mapping:
          # we just need to dtype of first occurance of column in transforms.
          class_name = transform.__class__.__name__
          if class_name not in _EXPECTED_TYPES:
            raise KeyError(
                f"Transform {class_name} is not registered with a supported "
                "type. Please register the transform with a supported type "
                "using register_input_dtype decorator.")
          column_type_mapping[col] = _EXPECTED_TYPES[
              transform.__class__.__name__]
    return column_type_mapping

  def get_raw_data_feature_spec(
      self, input_types: Dict[str, type]) -> Dict[str, tf.io.VarLenFeature]:
    """
    Return a DatasetMetadata object to be used with
    tft_beam.AnalyzeAndTransformDataset.
    Args:
      input_types: A dictionary of column names and types.
    Returns:
      A DatasetMetadata object.
    """
    raw_data_feature_spec = {}
    for key, value in input_types.items():
      raw_data_feature_spec[key] = self._get_raw_data_feature_spec_per_column(
          typ=value, col_name=key)
    return raw_data_feature_spec

  def convert_raw_data_feature_spec_to_dataset_metadata(
      self, raw_data_feature_spec) -> dataset_metadata.DatasetMetadata:
    raw_data_metadata = dataset_metadata.DatasetMetadata(
        schema_utils.schema_from_feature_spec(raw_data_feature_spec))
    return raw_data_metadata

  def _get_raw_data_feature_spec_per_column(
      self, typ: type, col_name: str) -> tf.io.VarLenFeature:
    """
    Return a FeatureSpec object to be used with
    tft_beam.AnalyzeAndTransformDataset
    Args:
      typ: A type of the column.
      col_name: A name of the column.
    Returns:
      A FeatureSpec object.
    """
    # lets conver the builtin types to typing types for consistency.
    typ = native_type_compatibility.convert_builtin_to_typing(typ)
    primitive_containers_type = (
        list,
        collections.abc.Sequence,
    )
    is_primitive_container = (
        typing.get_origin(typ) in primitive_containers_type)

    if is_primitive_container:
      dtype = typing.get_args(typ)[0]
      if len(typing.get_args(typ)) > 1 or typing.get_origin(dtype) == Union:
        raise RuntimeError(
            f"Union type is not supported for column: {col_name}. "
            f"Please pass a PCollection with valid schema for column "
            f"{col_name} by passing a single type "
            "in container. For example, List[int].")
    elif issubclass(typ, np.generic) or typ in _default_type_to_tensor_type_map:
      dtype = typ
    else:
      raise TypeError(
          f"Unable to identify type: {typ} specified on column: {col_name}. "
          f"Please provide a valid type from the following: "
          f"{_default_type_to_tensor_type_map.keys()}")
    return tf.io.VarLenFeature(_default_type_to_tensor_type_map[dtype])

  def get_raw_data_metadata(
      self, input_types: Dict[str, type]) -> dataset_metadata.DatasetMetadata:
    raw_data_feature_spec = self.get_raw_data_feature_spec(input_types)
    return self.convert_raw_data_feature_spec_to_dataset_metadata(
        raw_data_feature_spec)

  def write_transform_artifacts(self, transform_fn, location):
    """
    Write transform artifacts to the given location.
    Args:
      transform_fn: A transform_fn object.
      location: A location to write the artifacts.
    Returns:
      A PCollection of WriteTransformFn writing a TF transform graph.
    """
    return (
        transform_fn
        | 'Write Transform Artifacts' >>
        transform_fn_io.WriteTransformFn(location))

  def _fail_on_non_default_windowing(self, pcoll: beam.PCollection):
    if not pcoll.windowing.is_default():
      raise RuntimeError(
          "MLTransform only supports GlobalWindows when producing "
          "artifacts such as min, max, variance etc over the dataset."
          "Please use beam.WindowInto(beam.transforms.window.GlobalWindows()) "
          "to convert your PCollection to GlobalWindow.")

  def process_data_fn(
      self, inputs: Dict[str, common_types.ConsistentTensorType]
  ) -> Dict[str, common_types.ConsistentTensorType]:
    """
    This method is used in the AnalyzeAndTransformDataset step. It applies
    the transforms to the `inputs` in sequential order on the columns
    provided for a given transform.
    Args:
      inputs: A dictionary of column names and data.
    Returns:
      A dictionary of column names and transformed data.
    """
    outputs = inputs.copy()
    for transform in self.transforms:
      columns = transform.columns
      for col in columns:
        intermediate_result = transform(outputs[col], output_column_name=col)
        for key, value in intermediate_result.items():
          outputs[key] = value
    return outputs

  def _get_transformed_data_schema(
      self,
      metadata: dataset_metadata.DatasetMetadata,
  ):
    schema = metadata._schema
    transformed_types = {}
    for feature in schema.feature:
      name = feature.name
      feature_type = feature.type
      if feature_type == schema_pb2.FeatureType.FLOAT:
        transformed_types[name] = typing.Sequence[np.float32]
      elif feature_type == schema_pb2.FeatureType.INT:
        transformed_types[name] = typing.Sequence[np.int64]  # type: ignore[assignment]
      else:
        transformed_types[name] = typing.Sequence[bytes]  # type: ignore[assignment]
    return transformed_types

  def process_data(
      self, raw_data: beam.PCollection[tft_process_handler_input_type]
  ) -> beam.PCollection[tft_process_handler_output_type]:
    """
    This method also computes the required dataset metadata for the tft
    AnalyzeDataset/TransformDataset step.

    This method uses tensorflow_transform's Analyze step to produce the
    artifacts and Transform step to apply the transforms on the data.
    Artifacts are only produced if the artifact_mode is set to `produce`.
    If artifact_mode is set to `consume`, then the artifacts are read from the
    artifact_location, which was previously used to store the produced
    artifacts.
    """
    if self.artifact_mode == ArtifactMode.PRODUCE:
      # If we are computing artifacts, we should fail for windows other than
      # default windowing since for example, for a fixed window, each window can
      # be treated as a separate dataset and we might need to compute artifacts
      # for each window. This is not supported yet.
      self._fail_on_non_default_windowing(raw_data)
      element_type = raw_data.element_type
      column_type_mapping = {}
      if (isinstance(element_type, RowTypeConstraint) or
          native_type_compatibility.match_is_named_tuple(element_type)):
        column_type_mapping = self._map_column_names_to_types(
            row_type=element_type)
        # convert Row or NamedTuple to Dict
        raw_data = (
            raw_data
            | ConvertNamedTupleToDict().with_output_types(
                Dict[str, typing.Union[tuple(column_type_mapping.values())]]))  # type: ignore
        # AnalyzeAndTransformDataset raise type hint since this is
        # schema'd PCollection and the current output type would be a
        # custom type(NamedTuple) or a beam.Row type.
      else:
        column_type_mapping = self._map_column_names_to_types_from_transforms()
      raw_data_metadata = self.get_raw_data_metadata(
          input_types=column_type_mapping)
      # Write untransformed metadata to a file so that it can be re-used
      # during Transform step.
      metadata_io.write_metadata(
          metadata=raw_data_metadata,
          path=os.path.join(self.artifact_location, RAW_DATA_METADATA_DIR))
    else:
      # Read the metadata from the artifact_location.
      if not os.path.exists(os.path.join(
          self.artifact_location, RAW_DATA_METADATA_DIR, SCHEMA_FILE)):
        raise FileNotFoundError(
            "Artifacts not found at location: %s when artifact_mode=consume."
            "Make sure you've run the pipeline in `produce` mode using "
            "this artifact location before setting artifact_mode to `consume`."
            % os.path.join(self.artifact_location, RAW_DATA_METADATA_DIR))
      raw_data_metadata = metadata_io.read_metadata(
          os.path.join(self.artifact_location, RAW_DATA_METADATA_DIR))

    # To maintain consistency by outputting numpy array all the time,
    # whether a scalar value or list or np array is passed as input,
    #  we will convert scalar values to list values and TFT will ouput
    # numpy array all the time.
    raw_data |= beam.ParDo(ConvertScalarValuesToListValues())

    with tft_beam.Context(temp_dir=self.artifact_location):
      data = (raw_data, raw_data_metadata)
      if self.artifact_mode == ArtifactMode.PRODUCE:
        transform_fn = (
            data
            | "AnalyzeDataset" >> tft_beam.AnalyzeDataset(self.process_data_fn))
        self.write_transform_artifacts(transform_fn, self.artifact_location)
      else:
        transform_fn = (
            raw_data.pipeline
            | "ReadTransformFn" >> tft_beam.ReadTransformFn(
                self.artifact_location))
      (transformed_dataset, transformed_metadata) = (
          ((raw_data, raw_data_metadata), transform_fn)
          | "TransformDataset" >> tft_beam.TransformDataset())

      if isinstance(transformed_metadata, beam_metadata_io.BeamDatasetMetadata):
        self.transformed_schema = self._get_transformed_data_schema(
            metadata=transformed_metadata.dataset_metadata)
      else:
        self.transformed_schema = self._get_transformed_data_schema(
            transformed_metadata)

      # We will a pass a schema'd PCollection to the next step.
      # So we will use a RowTypeConstraint to create a schema'd PCollection.
      # this is needed since new columns are included in the
      # transformed_dataset.
      row_type = RowTypeConstraint.from_fields(
          list(self.transformed_schema.items()))

      transformed_dataset |= "ConvertToRowType" >> beam.Map(
          lambda x: beam.Row(**x)).with_output_types(row_type)
      return transformed_dataset
