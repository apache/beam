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
import logging
from collections import defaultdict
from typing import Any
from typing import Dict
from typing import List
from typing import Union
from typing import Callable
from typing import Optional

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

fixed_len_feature_dtypes = (int, str, float, bytes)
var_len_feature_dtypes = (tf.Tensor, list, np.ndarray)


class ProcessHandler:
  def process_data(self):
    raise NotImplementedError


class MLTransform(ptransform.PTransform):
  """
  1. Implement dead letter queue?
  """
  def __init__(self, process_step=Union[List[Callable], Callable]):
    self.process_step = process_step
    # We will use proceess handler to handle the data conversion and etc.
    self._process_handler = self._get_process_handler()

  def _get_process_handler(self):
    if callable(self.process_step):
      self.process_step = [self.process_step]
    elif self.process_step and isinstance(self.process_step[0], _TFTOperation):
      raise NotImplementedError

    return TFTProcessHandler(process_fn_config=self.process_step)

  def expand(self, pvalue):
    return (
        pvalue
        | "ProcessData" >> beam.ParDo(
            _MLTransformDoFn(process_handler=self._process_handler)))


class _MLTransformDoFn(beam.DoFn):
  def __init__(self, process_handler: ProcessHandler):
    self.process_handler = process_handler

  def process(self, element):
    yield self.process_handler.process_data(element)


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

  def _get_raw_data_metadata(self, data: Dict[str, Any]):
    """
        Assume we only support tf.string, tf.float32
        """
    data = data[0]
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
    return tft.DatasetMetadata.from_feature_spec(raw_data_feature_spec)

  def _deduce_dtype(self, element):
    # improve the logic here
    if hasattr(element, 'dtype'):
      # containers like numpy, tensorflow tensor.
      dtype = element.dtype
    else:
      dtype = type(element)

      if dtype == list:
        dtype = type(element[0])

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

  # def preprocess_fn(self, raw_data):
  #   """
  #       What should be the type of data in `raw_data`?
  #   """
  #   # since we will be changing the raw_data, copy it and then change it.
  #   output = raw_data.copy()
  #   column_ops = self._get_all_transforms_per_column()
  #   for column, ops in column_ops.items():
  #     o = output[column]
  #     for op in ops:
  #       o = op.apply(o)
  #     output[column] = o
  #   return output
  def preprocess_fn(self, output):
    # assuming process_config in the list are to do processed in sequential order

    # output = data.copy()
    for i in range(len(self.process_fn_config)):
      op = self.process_fn_config[i]
      # apply the op on specified column
      columns = op.columns
      for col in columns:
        print("**************")
        print(op.name)
        # output[col+ '_' + op.name] = op.apply(output[col])
        output[col] = op.apply(output[col])
    # output = data.copy()
    # output['x'] = self.process_fn_config[0].apply(output['x'])
    # output['x'] = self.process_fn_config[1].apply(output['x'])

    return output

  def process_data(self, raw_data: Dict[str, Any]):
    """
    This method gets called in the _MLTransformDoFn.
    Every handler should implement this method.
    """
    if not raw_data:
      raise RuntimeError("Provide atlease one data point in the raw data.")
    print("raw_data: {}".format(raw_data))
    raw_data_metadata = self._get_raw_data_metadata(raw_data)
    data = (raw_data, raw_data_metadata)
    with tft_beam.Context(tempfile.mkdtemp()):
      transformed_dataset, _ = (
          data
          | tft_beam.AnalyzeAndTransformDataset(
        self.preprocess_fn,
        output_record_batches=self.output_record_batches))
    return transformed_dataset


# Remove the main.
if __name__ == '__main__':
  # logging.basicConfig(level=logging.WARNING)
  # raw_data = [{
  #     'x': 1,
  #     'y': 2,
  #     's': 'hello'
  # },
  # # {
  #     # 'x': 2, 'y': 2, 's': 'world'
  # # },
  #   # {
  #     # 'x': 3, 'y': 3, 's': 'hello'
  # # }
  # ]

  raw_data = [
      dict(x=["I", "like", "pie", "pie", "pie"]), dict(x=["yum", "yum", "pie"])
  ]

  # output_record_batches = False
  # tft_handler = TFTProcessHandler(
  #     process_fn_config=[
  #         # tfidf(columns=['x']),
  #         compute_and_apply_vocabulary(columns=['x']),
  #         get_num_buckets_after_transformation(columns=['x'])
  #     ],
  #     output_record_batches=output_record_batches)
  # transformed_dataset = tft_handler.process_data(raw_data)
  # print(transformed_dataset)

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

  # with beam.Pipeline() as pipeline:
  #   (
  #       pipeline
  #       | "Create" >> beam.Create(raw_data)
  #       | "ComputeAndApplyVocab" >> MLTransform(compute_and_apply_vocabulary(columns=['x',]))
  #       # | "GetNumBucketsForTransformedFeatures" >> MLTransform(get_num_buckets_after_transformation(columns=['x']))
  #       | "print" >> beam.Map(print)
  #       )


  def convert_(d=Dict):
    out = defaultdict(list)
    for key, value in d.items():
      out[key].extend(value)
    return out

  #   return out
  import pyarrow as pa

  def convert_to_record_batch(list_dict):
    return pa.RecordBatch.from_pydict(list_dict)

  with beam.Pipeline() as p:
    (
        p | beam.Create(raw_data)
        # | beam.Map(lambda x: beam.Row(**x))
        # | beam.BatchElements(min_batch_size=2, max_batch_size=2)
        # | beam.GroupByKey()
        | beam.Map(convert_)
        | beam.Map(convert_to_record_batch)
        | beam.Map(lambda x: x.to_pandas())
        | beam.Map(print))
"""
We need ease of use transform.

"""

# data | MLTransform(compute_and_apply_vocabulary(columns=['x'])) | MLTransform(apply_buckets(columns=['x']))
