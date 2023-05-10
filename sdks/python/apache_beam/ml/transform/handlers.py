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
from abc import abstractmethod
from typing import Dict
import pyarrow as pa
import numpy as np
import tensorflow as tf
import tensorflow_transform as tft
import tensorflow_transform.beam as tft_beam
from typing import List


class ProcessHandler:
  @abstractmethod
  def process_data(self, data):
    """
    Logic to process the data. This will be the entrypoint in beam.MLTransform to process 
    incoming data:
    """
    pass


class TFTProcessHandler(ProcessHandler):
  def __init__(self, transforms):
    self._transforms = transforms

  # def preprocessing_fn(self, inputs: Dict) -> Dict:
  #   outputs = inputs.copy()
  #   for transform in self._transforms:
  #     columns = transform.columns
  #     if not columns:
  #       # if columns are not specified, apply the transform on every column.
  #       columns = inputs.keys()
  #     for col in columns:
  #       outputs[col] = transform.apply(inputs[col])

  def preprocessing_fn(self, inputs):
    outputs = inputs.copy()
    outputs['y'] = tft.scale_to_0_1(inputs['y'])
    return outputs

  def get_raw_data_feature_spec(self, data):
    if isinstance(data, str):
      return tf.io.VarLenFeature(tf.string)
    elif isinstance(data, int):
      return tf.io.FixedLenFeature([], tf.int64)
    elif isinstance(data, float):
      return tf.io.FixedLenFeature([], tf.float32)
    elif isinstance(data, list):
      return self.get_raw_data_feature_spec(data[0])
    else:
      raise NotImplementedError

  def process_data(self, raw_data: List):
    """
    Instance dict
    1. raw_data needs to be a list so that it can be passed to AnalyzeAndTransformDataset
    """
    # find raw_data_metadata
    if isinstance(raw_data, pa.RecordBatch):
      raise NotImplementedError

    # TODO: change this elif condition.
    elif isinstance(raw_data, list):
      raw_data_feature_spec = {}
      d = raw_data[0]
      for key, value in d.items():
        raw_data_feature_spec[key] = self.get_raw_data_feature_spec(value)

    elif isinstance(raw_data, dict):
      raw_data_feature_spec = {}
      for key, value in raw_data.items():
        raw_data_feature_spec[key] = self.get_raw_data_feature_spec(value)

    else:
      raise NotImplementedError

    raw_data_metadata = tft.DatasetMetadata.from_feature_spec(
        raw_data_feature_spec)

    if isinstance(raw_data, list):
      data = (raw_data, raw_data_metadata)
    else:
      data = ([raw_data], raw_data_metadata)
    # print(type(raw_data))
    # while True:pass

    with tft_beam.Context(temp_dir=tempfile.mkdtemp()):
      transformed_dataset, transform_fn = (
        data
        | "AnalyzeAndTransformDataset"  >> tft_beam.AnalyzeAndTransformDataset(
        self.preprocessing_fn,
        )
      )
      return transformed_dataset, transform_fn
