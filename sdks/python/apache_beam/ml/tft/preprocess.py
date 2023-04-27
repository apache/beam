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

import tempfile
import typing
from collections import defaultdict
from typing import Any
from typing import Dict
from typing import List
from typing import Union

import apache_beam as beam
import numpy as np
import pyarrow as pa
import tensorflow as tf
import tensorflow_transform as tft
import tensorflow_transform.beam as tft_beam
from apache_beam.ml.tft.wrapper import _TFTOperation
from apache_beam.ml.tft.wrapper import bucketize
from apache_beam.ml.tft.wrapper import *
from apache_beam.transforms import ptransform
from tensorflow_transform.schema_inference import schema_utils
"""
Make use of the tft.AnalyzeTransformDataset -> this needs preprocessingfn

The transforms should support `is instance dict` format and also the `TFXIO` format.

Figure out typing as well.
"""
"""
Input types accepted by AnalyzeAndTransformDataset -> 
"""
from tensorflow_transform import common_types
# typing.Union[typing.List[typing.Dict[str, typing.Union[numpy.ndarray, numpy.generic, str, bytes, float, int, typing.List[typing.Any], NoneType]]], pyarrow.lib.RecordBatch] # pylint: disable=line-too-long
INPUT_TYPE = Union[List[common_types.InstanceDictType], pa.RecordBatch]

primitive_type_to_tensor_type = {
    int: tf.int64,
    str: tf.string,
    float: tf.float32,
}

numpy_to_tensor_type = {
    np.dtype(np.int32): tf.int64,
    np.dtype(np.int64): tf.int64,
    np.dtype(np.float32): tf.float32,
    np.dtype(np.float16): tf.float32,
    np.dtype(np.float64): tf.float32
}

fixed_len_feature_dtypes = (int, str, float, list, bytes, np.ndarray)
var_len_feature_dtypes = (tf.Tensor)


class ProcessHandler:
  def process_data(self):
    raise NotImplementedError

  def convert_data(self):
    """No op here."""
    def _PTransformFn(pcoll_or_pipeline):
      return pcoll_or_pipeline

    return beam.ptransform_fn(_PTransformFn)()


class MLTransform(ptransform.PTransform):
  """
  1. Implement dead letter queue?
  """
  def __init__(self, process_handler):
    self.process_handler = process_handler

  def expand(self, pvalue: typing.Dict):
    return (
        pvalue
        | "BatchElements" >> beam.BatchElements()
        | "DataConversion" >> beam.ParDo(_DictToRecordBatches())
        # | "ProcessParDo" >> beam.ParDo(_MLTransformDoFn(self.process_handler))
    )


class _DictToRecordBatches(beam.DoFn):
  def process(self, element):
    yield pa.RecordBatch.from_pylist(element)


class _MLTransformDoFn(beam.DoFn):
  def __init__(self, process_handler: ProcessHandler):
    self.process_handler = process_handler

  def process(self, element):
    self.process_handler.process_data(element)


class TFTProcessHandler(ProcessHandler):
  """
    This should support `instance dict` and also `TFXIO` data formats.
    The inputs should support native python lists, dicts, np.ndarray
    and pandas DataFrame. 
    """
  def __init__(
      self,
      process_fn_config: typing.List[_TFTOperation],
      output_record_batches=False):
    self.process_fn_config = process_fn_config
    self.output_record_batches = output_record_batches

  def convert_data(self, element):
    def _PTransformFn():
      import pyarrow as pa
      return (pcoll_or_pipeline | beam.Map(pa.RecordBatch.from_pydict))

    return beam.ptransform_fn(_PTransformFn)()

  def _get_raw_data_metadata(self, data: Dict[str, Any]):
    """
        Assume we only support tf.string, tf.float32
        """
    raw_data_feature_spec = {}
    for name in data.keys():

      dtype = self._deduce_dtype(data[name])
      # define the schema based on the dtype.
      if isinstance(data[name], fixed_len_feature_dtypes):
        if hasattr(data[name], 'shape'):
          shape = data[name].shape
        else:
          shape = []
        raw_data_feature_spec[name] = tf.io.FixedLenFeature(shape, dtype)
      elif isinstance(data[name], var_len_feature_dtypes):
        raw_data_feature_spec[name] = tf.io.VarLenFeature(dtype)
    return tft.DatasetMetadata(
        schema_utils.schema_from_feature_spec(raw_data_feature_spec))

  def _deduce_dtype(self, element):
    # improve the logic here
    if hasattr(element, 'dtype'):
      # containers like numpy, tensorflow tensor.
      dtype = element.dtype
    else:
      dtype = type(element)

      if dtype in primitive_type_to_tensor_type:
        dtype = primitive_type_to_tensor_type[dtype]
      elif dtype in numpy_to_tensor_type:
        dtype = numpy_to_tensor_type[dtype]
      elif dtype == bytes:
        dtype = tf.string
      # else:
      #   raise RuntimeError(f"Type {type(element)} is not supported yet")
    return dtype

  def _get_all_transforms_per_column(self):
    column_ops = defaultdict(list)
    for process_fn in self.process_fn_config:
      for column in process_fn.columns:
        if column not in column_ops:
          column_ops[column].append(process_fn)
    return column_ops

  def preprocess_fn(self, raw_data):
    """
        What should be the type of data in `raw_data`?
    """
    # since we will be changing the raw_data, copy it and then change it.
    output = raw_data.copy()
    column_ops = self._get_all_transforms_per_column()
    for column, ops in column_ops.items():
      o = output[column]
      for op in ops:
        o = op.apply(o)
      output[column] = o
    return output

  def process_data(self, raw_data: List[Dict[str, Any]]):
    """
    This method gets called in the _MLTransformDoFn.
    Every handler should implement this method.
    """
    if not raw_data:
      raise RuntimeError("Provide atlease one data point in the raw data.")

    raw_data_metadata = self._get_raw_data_metadata(raw_data[0])
    data = (raw_data, raw_data_metadata)
    with tft_beam.Context(tempfile.mkdtemp()):
      transformed_dataset, transform_fn = (
          data
          | tft_beam.AnalyzeAndTransformDataset(
        self.preprocess_fn,
        output_record_batches=self.output_record_batches
      )
      )
    return transformed_dataset


def preprocess_fn(inputs):
  return {
      'y': tft.apply_buckets(
          inputs['y'], bucket_boundaries=tf.constant([[2.0, 5.0, 10.0]]))
  }


# Remove the main.
if __name__ == '__main__':
  raw_data = [{
      'x': 1, 'y': np.array([1, 10, 15], dtype=np.int32), 's': 'hello'
  }, {
      'x': 2, 'y': 2, 's': 'world'
  }, {
      'x': 3, 'y': 3, 's': 'hello'
  }]

  # output_record_batches = False
  # tft_handler = TFTProcessHandler(
  #     process_fn_config=[
  #         # compute_and_apply_vocabulary(columns=['s']),
  #         apply_buckets(
  #             columns=['y'], bucket_boundaries=tf.constant([[2.0, 5.0, 10.0]])),
  #         # scale_to_z_score(columns=['y']),
  #         # tf.reduce_mean,
  #     ],
  #     output_record_batches=output_record_batches)
  # transformed_dataset = tft_handler.process_data(raw_data)
  # print(transformed_dataset[0])

  # raw_data_feature_spec = {'y' : tf.io.FixedLenFeature((2,3), tf.int64)}
  # raw_data_metadata = tft.DatasetMetadata(
  #       schema_utils.schema_from_feature_spec(raw_data_feature_spec))
  # data = (raw_data, raw_data_metadata)
  # with tft_beam.Context(tempfile.mkdtemp()):
  #   transformed_dataset, transform_fn = (
  #         data
  #         | tft_beam.AnalyzeAndTransformDataset(
  #       preprocess_fn,
  #     ))

  # print(transformed_dataset[0])
  tft_process_handler = TFTProcessHandler(process_fn_config=[])
  with beam.Pipeline() as pipeline:
    (
        pipeline
        | "Create" >> beam.Create(raw_data)
        | "ConvertToTFX" >> MLTransform(tft_process_handler)
        | "print" >> beam.Map(print))
