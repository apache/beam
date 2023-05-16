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

import tempfile
from abc import abstractmethod
from typing import Dict, List, Tuple

import apache_beam as beam
import numpy as np
import pyarrow as pa
import tensorflow as tf
import tensorflow_transform as tft
import tensorflow_transform.beam as tft_beam
from apache_beam.typehints import native_type_compatibility
from tensorflow_transform.tf_metadata import schema_utils
from tensorflow_transform.tf_metadata import dataset_metadata
from tfx_bsl.tfxio import tf_example_record
from tensorflow_transform.beam.tft_beam_io import transform_fn_io

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


class ProcessHandler:
  @abstractmethod
  def process_data(self, data) -> beam.PTransform:
    """
    Logic to process the data. This will be the entrypoint in beam.MLTransform to process 
    incoming data:
    """
    raise NotImplementedError


class TFTProcessHandler(ProcessHandler):
  def __init__(
      self,
      *,
      input_types: Dict[str, type],
      output_record_batches=False,
      transforms=None,
      artifact_location=None,
  ):
    self._input_types = input_types
    self._transforms = transforms if transforms else []
    self._input_types = input_types
    self._output_record_batches = output_record_batches
    self._artifact_location = artifact_location

  def preprocessing_fn(self, inputs: Dict) -> Dict:
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

  @property
  def get_raw_data_feature_spec(self):
    raw_data_feature_spec = {}
    for key, value in self._input_types.items():
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

  # TODO: remove this and uncomment the above lines
  # def preprocessing_fn(self, inputs):
  #   outputs = inputs.copy()
  #   # reshape needed because of https://github.com/tensorflow/transform/issues/128
  #   outputs['y_min'] = tf.reshape(tft.min(inputs['y']), [-1])
  #   outputs['y'] = tft.scale_to_0_1(inputs['y'])
  #   return outputs

  @abstractmethod
  def get_metadata(self):
    """
    Return metadata to be used with tft_beam.AnalyzeAndTransformDataset
    """

  def write_transform_artifacts(self, transform_fn, location):
    return (
        transform_fn
        | 'Write Transform Artifacts' >>
        transform_fn_io.WriteTransformFn(location))

  def process_data(self):
    @beam.ptransform_fn
    def ptransform_fn(raw_data):
      metadata = self.get_metadata()
      with tft_beam.Context(temp_dir=tempfile.mkdtemp()):
        data = (raw_data, metadata)
        transformed_dataset, transform_fn = (
          data
          | "AnalyzeAndTransformDataset"  >> tft_beam.AnalyzeAndTransformDataset(
          self.preprocessing_fn,
          output_record_batches=self._output_record_batches
          )
        )

        if self._artifact_location:
          self.write_transform_artifacts(transform_fn, self._artifact_location)
        return transformed_dataset[0]

    return ptransform_fn()


class TFTProcessHandlerPyArrow(TFTProcessHandler):
  def get_metadata(self):
    raw_data_feature_spec = self.get_raw_data_feature_spec
    schema = raw_data_feature_spec.schema
    _tfxio = tf_example_record.TFExampleBeamRecord(
        physical_format='inmem',
        telemetry_descriptors=['StandaloneTFTransform'],
        schema=schema)
    tensor_adapter_config = _tfxio.TensorAdapterConfig()
    return tensor_adapter_config


class TFTProcessHandlerDict(TFTProcessHandler):
  def get_metadata(self):
    return self.get_raw_data_feature_spec
