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
import collections
import logging
import tempfile
import typing
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import numpy as np

import apache_beam as beam
from apache_beam.ml.transforms.base import _ProcessHandler
from apache_beam.ml.transforms.base import ProcessInputT
from apache_beam.ml.transforms.base import ProcessOutputT
from apache_beam.ml.transforms.tft_transforms import TFTOperation
from apache_beam.typehints import native_type_compatibility
from apache_beam.typehints.row_type import RowTypeConstraint
import tensorflow as tf
from tensorflow_metadata.proto.v0 import schema_pb2
import tensorflow_transform.beam as tft_beam
from tensorflow_transform import common_types
from tensorflow_transform.beam.tft_beam_io import beam_metadata_io
from tensorflow_transform.beam.tft_beam_io import transform_fn_io
from tensorflow_transform.tf_metadata import dataset_metadata
from tensorflow_transform.tf_metadata import schema_utils

__all__ = [
    'TFTProcessHandlerSchema',
]

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

tft_process_handler_schema_input_type = typing.Union[typing.NamedTuple,
                                                     beam.Row]


class ConvertNamedTupleToDict(
    beam.PTransform[beam.PCollection[tft_process_handler_schema_input_type],
                    beam.PCollection[Dict[str,
                                          common_types.InstanceDictType]]]):
  """
    A PTransform that converts a collection of NamedTuples or Rows into a
    collection of dictionaries.
  """
  def expand(
      self, pcoll: beam.PCollection[tft_process_handler_schema_input_type]
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


class TFTProcessHandler(_ProcessHandler[ProcessInputT, ProcessOutputT]):
  def __init__(
      self,
      *,
      transforms: Optional[List[TFTOperation]] = None,
      artifact_location: typing.Optional[str] = None,
      preprocessing_fn: typing.Optional[typing.Callable] = None,
  ):
    """
    A handler class for processing data with TensorFlow Transform (TFT)
    operations. This class is intended to be subclassed, with subclasses
    implementing the `preprocessing_fn` method.

    Args:
      transforms: A list of transforms to apply to the data. All the transforms
        are applied in the order they are specified. The input of the
        i-th transform is the output of the (i-1)-th transform. Multi-input
        transforms are not supported yet.
      artifact_location: A location to store the artifacts, which includes
        the tensorflow graph produced by analyzers such as ScaleTo01,
        ScaleToZScore, etc.
    """
    self.transforms = transforms if transforms else []
    self.transformed_schema = None
    self.artifact_location = artifact_location
    self.preprocessing_fn = preprocessing_fn

    if not self.artifact_location:
      self.artifact_location = tempfile.mkdtemp()

  def append_transform(self, transform):
    self.transforms.append(transform)

  def get_raw_data_feature_spec(
      self, input_types: Dict[str, type]) -> dataset_metadata.DatasetMetadata:
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
    raw_data_metadata = dataset_metadata.DatasetMetadata(
        schema_utils.schema_from_feature_spec(raw_data_feature_spec))
    return raw_data_metadata

  def _get_raw_data_feature_spec_per_column(self, typ: type, col_name: str):
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
      dtype = typing.get_args(typ)[0]  # type: ignore[attr-defined]
      if len(typing.get_args(typ)) > 1 or typing.get_origin(dtype) == Union:  # type: ignore[attr-defined]
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

  def get_raw_data_metadata(self, input_types: Dict[str, type]):
    """
    Return metadata to be used with tft_beam.AnalyzeAndTransformDataset
    Args:
      input_types: A dictionary of column names and types.
    """
    raise NotImplementedError

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

  def process_data_fn(
      self, inputs: Dict[str, common_types.ConsistentTensorType]
  ) -> Dict[str, common_types.ConsistentTensorType]:
    """
    A preprocessing_fn which should be implemented by subclasses
    of TFTProcessHandlers. In this method, tft data transforms
    such as scale_0_to_1 functions are called.
    Args:
      inputs: A dictionary of column names and associated data.
    """
    raise NotImplementedError

  def _fail_on_non_default_windowing(self, pcoll: beam.PCollection):
    if not pcoll.windowing.is_default():
      raise RuntimeError(
          "TFTProcessHandler only supports GlobalWindows. "
          "Please use beam.WindowInto(beam.transforms.window.GlobalWindows()) "
          "to convert your PCollection to GlobalWindow.")


class TFTProcessHandlerSchema(
    TFTProcessHandler[tft_process_handler_schema_input_type, beam.Row]):
  """
    A subclass of TFTProcessHandler specifically for handling
    data in beam.Row or NamedTuple format.
    TFTProcessHandlerSchema creates a beam graph that applies
    TensorFlow Transform (TFT) operations to the input data and
    outputs a beam.Row object containing the transformed data as numpy arrays.

    This only works on the Schema'd PCollection. Please refer to
    https://beam.apache.org/documentation/programming-guide/#schemas
    for more information on Schema'd PCollection.

    Currently, there are two ways to define a schema for a PCollection:

    1) Register a `typing.NamedTuple` type to use RowCoder, and specify it as
      the output type. For example::

        Purchase = typing.NamedTuple('Purchase',
                                    [('item_name', unicode), ('price', float)])
        coders.registry.register_coder(Purchase, coders.RowCoder)
        with Pipeline() as p:
          purchases = (p | beam.Create(...)
                        | beam.Map(..).with_output_types(Purchase))

    2) Produce `beam.Row` instances. Note this option will fail if Beam is
      unable to infer data types for any of the fields. For example::

        with Pipeline() as p:
          purchases = (p | beam.Create(...)
                        | beam.Map(lambda x: beam.Row(item_name=unicode(..),
                                                      price=float(..))))
    In the schema, TFTProcessHandlerSchema accepts the following types:
    1. Primitive types: int, float, str, bytes
    2. List of the primitive types.
    3. Numpy arrays.

    For any other types, TFTProcessHandler will raise a TypeError.
  """
  def _map_column_names_to_types(self, element_type):
    """
    Return a dictionary of column names and types.
    Args:
      element_type: A type of the element. This could be a NamedTuple or a Row.
    Returns:
      A dictionary of column names and types.
    """

    if not isinstance(element_type, RowTypeConstraint):
      row_type = RowTypeConstraint.from_user_type(element_type)
      if not row_type:
        raise TypeError(
            "Element type must be compatible with Beam Schemas ("
            "https://beam.apache.org/documentation/programming-guide/#schemas)"
            " for to use with MLTransform and TFTProcessHandlerSchema.")
    else:
      row_type = element_type
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
        intermediate_result = transform.apply(
            outputs[col], output_column_name=col)
        for key, value in intermediate_result.items():
          outputs[key] = value
    return outputs

  def _get_transformed_data_schema(
      self,
      metadata: dataset_metadata.DatasetMetadata,
  ) -> Dict[str, typing.Sequence[typing.Union[np.float32, np.int64, bytes]]]:
    schema = metadata._schema
    transformed_types = {}
    logging.info("Schema: %s", schema)
    for feature in schema.feature:
      name = feature.name
      feature_type = feature.type
      if feature_type == schema_pb2.FeatureType.FLOAT:
        transformed_types[name] = typing.Sequence[np.float32]
      elif feature_type == schema_pb2.FeatureType.INT:
        transformed_types[name] = typing.Sequence[np.int64]
      elif feature_type == schema_pb2.FeatureType.BYTES:
        transformed_types[name] = typing.Sequence[bytes]
      else:
        # TODO: This else condition won't be hit since TFT doesn't output
        # other than float, int and bytes. Refactor the code here.
        raise RuntimeError(
            'Unsupported feature type: %s encountered' % feature_type)
    logging.info(transformed_types)
    return transformed_types

  def process_data(
      self, pcoll: beam.PCollection[tft_process_handler_schema_input_type]
  ) -> beam.PCollection[beam.Row]:

    self._fail_on_non_default_windowing(pcoll)

    element_type = pcoll.element_type
    column_type_mapping = self._map_column_names_to_types(
        element_type=element_type)
    # AnalyzeAndTransformDataset raise type hint since we only accept
    # schema'd PCollection and the current output type would be a
    # custom type(NamedTuple) or a beam.Row type.
    raw_data = (
        pcoll
        | ConvertNamedTupleToDict()
        # Also, to maintain consistency by outputting numpy array all the time,
        #  we will convert scalar values to list values.
        | beam.ParDo(_ConvertScalarValuesToListValues()).with_output_types(
            Dict[str, typing.Union[tuple(column_type_mapping.values())]]))

    raw_data_metadata = self.get_raw_data_metadata(
        input_types=column_type_mapping)

    preprocessing_fn = self.preprocessing_fn if self.preprocessing_fn else (
        self.process_data_fn)
    with tft_beam.Context(temp_dir=self.artifact_location):
      data = (raw_data, raw_data_metadata)
      transformed_metadata: beam_metadata_io.BeamDatasetMetadata
      (transformed_dataset, transformed_metadata), transform_fn = (
      data
      | "AnalyzeAndTransformDataset" >> tft_beam.AnalyzeAndTransformDataset(
      preprocessing_fn,
        )
      )
      self.write_transform_artifacts(transform_fn, self.artifact_location)
      self.transformed_schema = self._get_transformed_data_schema(
          metadata=transformed_metadata.dataset_metadata)

      # We need to pass a schema'd PCollection to the next step.
      # So we will use a RowTypeConstraint to create a schema'd PCollection.
      # this is needed since new columns are included in the
      # transformed_dataset.
      row_type = RowTypeConstraint.from_fields(
          list(self.transformed_schema.items()))

      transformed_dataset |= "ConvertToRowType" >> beam.Map(
          lambda x: beam.Row(**x)).with_output_types(row_type)
      return transformed_dataset

  def get_raw_data_metadata(
      self, input_types: Dict[str, type]) -> dataset_metadata.DatasetMetadata:
    return self.get_raw_data_feature_spec(input_types)


class _ConvertScalarValuesToListValues(beam.DoFn):
  def process(
      self, element: Dict[str, typing.Any]
  ) -> typing.Iterable[Dict[str, typing.List[typing.Any]]]:
    new_dict = {}
    for key, value in element.items():
      if isinstance(value,
                    tuple(_primitive_types_to_typing_container_type.keys())):
        new_dict[key] = [value]
      else:
        new_dict[key] = value
    yield new_dict
