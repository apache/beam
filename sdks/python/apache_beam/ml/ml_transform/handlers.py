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

# pylint: skip-file

"""
Observations and some constants: 

Containers: non-scalars, such as list, numpy.ndarray

List[Any]: VarLenFeature
Numpy: VarLenFeature or FixedLenFeature with shape

int: FixedLenFeature
str: FixedLenFeature
float: FixedLenFeature
bytes: FixedLenFeature
"""

import logging
import tempfile
import typing
# from abc import abstractmethod
from typing import Dict, List, Optional, Tuple, TypeVar, Union

import apache_beam as beam
import numpy as np
import pyarrow as pa
import tensorflow as tf
import tensorflow_transform as tft
import tensorflow_transform.beam as tft_beam
from apache_beam import typehints
from apache_beam.typehints import native_type_compatibility
from apache_beam.typehints.row_type import RowTypeConstraint
from tensorflow_transform import common_types
from tensorflow_transform.beam.tft_beam_io import transform_fn_io
from tensorflow_transform.tf_metadata import dataset_metadata, schema_utils
from tfx_bsl.tfxio import tf_example_record
from apache_beam.ml.ml_transform.base import ProcessHandler
from apache_beam.ml.ml_transform.base import MLTransformOutput

# tensorflow transform doesn't support the types other than tf.int64, tf.float32 and tf.string.
_default_type_to_tensor_type_map = {
    int: tf.int64,
    float: tf.float32,
    str: tf.string,
    bytes: tf.string,
    np.int64: tf.int64,
    np.int32: tf.int64,
    np.float32: tf.float32,
    np.float64: tf.float32
}

InputT = TypeVar('InputT', bound=typing.Union[typing.NamedTuple, beam.Row])


class ConvertNamedTupleToDict(beam.PTransform[beam.PCollection[InputT],
                                              beam.PCollection[Dict]]):
  def expand(self, pcoll: beam.PCollection[InputT]) -> beam.PCollection[Dict]:
    if isinstance(pcoll.element_type, RowTypeConstraint):
      # Row instance
      return pcoll | beam.Map(lambda x: x.asdict())
    else:
      # named tuple
      return pcoll | beam.Map(lambda x: x._asdict())


class TFTProcessHandler(ProcessHandler):
  def __init__(
      self,
      *,
      input_types: Optional[Dict[str, type]] = None,
      output_record_batches=False,
      transforms=None,
      artifact_location=None,
  ):
    self._input_types = input_types
    self._transforms = transforms if transforms else []
    self._input_types = input_types
    self._output_record_batches = output_record_batches
    self._artifact_location = artifact_location

  def get_input_types(self, element_type) -> Dict[str, type]:
    inferred_schema = None
    if not isinstance(element_type, RowTypeConstraint):
      inferred_schema = RowTypeConstraint.from_user_type(element_type)
    if not inferred_schema:
      raise RuntimeError("MLTransform supports schema-aware PCollection only.")

    inferred_types = {name: typ for name, typ in inferred_schema._fields}
    return inferred_types

  def get_raw_data_feature_spec(self, input_types: Dict[str, type]):

    raw_data_feature_spec = {}
    for key, value in input_types.items():
      raw_data_feature_spec[key] = self._get_raw_data_feature_spec_per_element(
          typ=value)
    raw_data_metadata = dataset_metadata.DatasetMetadata(
        schema_utils.schema_from_feature_spec(raw_data_feature_spec))
    return raw_data_metadata

  def _get_raw_data_feature_spec_per_element(self, typ: type):
    containers_type = (List._name, Tuple._name)
    is_container = False
    if typ in native_type_compatibility._BUILTINS_TO_TYPING:
      typ = native_type_compatibility.convert_builtin_to_typing(type)

    # we get typing types here.
    if hasattr(typ, '_name') and typ._name in containers_type:
      args = typ.__args__
      if len(args) > 1:
        raise RuntimeError(
            f"Multiple types are specified in the container type {typ}")
      dtype = args[0]
      is_container = True
    else:
      dtype = typ

    if issubclass(dtype, np.generic):
      is_container = True

    if is_container:
      return tf.io.VarLenFeature(_default_type_to_tensor_type_map[dtype])
    else:
      return tf.io.FixedLenFeature([], _default_type_to_tensor_type_map[dtype])

  def get_metadata(self, input_types: Dict[str, type]):
    """
    Return metadata to be used with tft_beam.AnalyzeAndTransformDataset
    """
    raise NotImplementedError

  def write_transform_artifacts(self, transform_fn, location):
    return (
        transform_fn
        | 'Write Transform Artifacts' >>
        transform_fn_io.WriteTransformFn(location))

  def infer_output_type(self, input_type):
    if not isinstance(input_type, RowTypeConstraint):
      row_type = RowTypeConstraint.from_user_type(input_type)
    fields = row_type._inner_types()
    return Dict[str, Union[tuple(fields)]]

  def _infer_schema(self, element_type):
    if not isinstance(element_type, RowTypeConstraint):
      inferred_types = RowTypeConstraint.from_user_type(element_type)
      return inferred_types

  def process_data(self, element_type: Optional[Dict[str, type]] = None):
    raise NotImplementedError

  # @abstractmethod
  def preprocessing_fn(
      self, inputs: Dict[str, common_types.ConsistentTensorType]
  ) -> Dict[str, common_types.ConsistentTensorType]:
    """
    A preprocessing_fn which should be implemented by subclasses of TFTProcessHandlers.
    In this method, tft data transforms such as scale_0_to_1 functions are called.
    """
    pass


class TFTProcessHandlerDict(TFTProcessHandler):
  def _validate_input_types(self, input_types: Dict[str, type]):
    # Fail for the cases like
    # Union[List[int], float] and List[List[int]]
    _valid_types_in_container = (int, str, bytes, float)
    for _, typ in input_types.items():
      if hasattr(typ, '__args__'):
        args = typ.__args__
        if len(args) > 1 and args[0] not in _valid_types_in_container:
          return False
    return True

  def preprocessing_fn(
      self, inputs: Dict[str, common_types.ConsistentTensorType]
  ) -> Dict[str, common_types.ConsistentTensorType]:
    outputs = inputs.copy()
    for transform in self._transforms:
      columns = transform.columns
      if not columns:
        # if columns are not specified, apply the transform on every column.
        # columns = inputs.keys()
        # TODO: Make a decision here.
        raise NotImplementedError
      for col in columns:
        if transform.has_artifacts:
          artifacts = transform.get_analyzer_artifacts(
              inputs[col], col_name=col)
          for key, value in artifacts.items():
            outputs[key] = value
        outputs[col] = transform.apply(inputs[col])
    return outputs

  def _get_ptransform(self):
    @beam.ptransform_fn
    @typehints.with_input_types(InputT)
    def ptransform_fn(
        raw_data: beam.PCollection[InputT]
    ) -> beam.PCollection[MLTransformOutput]:

      element_type = raw_data.element_type
      input_types = self.get_input_types(element_type=element_type)

      if not self._validate_input_types(input_types):
        raise RuntimeError(
            "Unable to infer schema. Please pass a schema'd PCollection")

      metadata = self.get_metadata(input_types=input_types)

      # AnalyzeAndTransformDataset raise type hint since we only accept schema'd PCollection and
      # the current output type would be a custom type(NamedTuple) or a beam.Row type.
      output_type = self.infer_output_type(element_type)
      raw_data = (
          raw_data | ConvertNamedTupleToDict().with_output_types(output_type))
      with tft_beam.Context(temp_dir=tempfile.mkdtemp()):
        data = (raw_data, metadata)
        (transformed_dataset, transformed_metadata), transform_fn = (
          data
          | "AnalyzeAndTransformDataset"  >> tft_beam.AnalyzeAndTransformDataset(
          self.preprocessing_fn,
          output_record_batches=self._output_record_batches
            )
          )
        if self._artifact_location:
          artifact_location = self._artifact_location
          # if not given, create a temp directory and store the transform_fn
          self.write_transform_artifacts(transform_fn, artifact_location)
        else:
          # TODO
          pass
          # raise NotImplementedError

        return transformed_dataset | beam.Map(
            lambda x: self.convert_to_ml_transform_output(
                x,
                transformed_metadata.dataset_metadata,
                transformed_metadata.asset_map))

    return ptransform_fn()

  def process_data(self, pcoll):
    return (pcoll | self._get_ptransform())

  def convert_to_ml_transform_output(self, element, metadata, asset_map):
    return MLTransformOutput(
        transformed_data=element,
        transformed_metadata=metadata,
        asset_map=asset_map)

  def get_metadata(self, input_types: Dict[str, type]):
    return self.get_raw_data_feature_spec(input_types)
