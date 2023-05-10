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

# pylint: skip-file

import logging
from typing import Any
from typing import Callable
from typing import List
from typing import Optional

import apache_beam as beam
from apache_beam.ml.transform.handlers import TFTProcessHandler
import tensorflow as tf
import tensorflow_transform as tft
from tensorflow_transform import analyzers
from tensorflow_transform import common_types

__all__ = ['compute_and_apply_vocabulary', 'scale_to_z_score', 'MLTransform']


class _BaseTransform:
  def __init__(self, columns, *args, **kwargs):
    self.columns = columns
    self._args = args
    self._kwargs = kwargs

    if not columns:
      # (TODO): Shoud we apply the transform or skip the transform?
      logging.warning(
          "Columns are not specified. Applying the "
          "transform %s on all columns." % self)

  def apply(self, data):
    raise NotImplementedError

  def validate_args(self):
    raise NotImplementedError

  def __call__(self, data):
    return self.apply(data, *self._args, **self._kwargs)


class _ComputeAndApplyVocab(_BaseTransform):
  def apply(self, data: common_types.ConsistentTensorType):
    return tft.compute_and_apply_vocabulary(x=data, *self._args, **self._kwargs)

  def __str__(self):
    return "compute_and_apply_vocabulary"


class _Scale_To_Z_Score(_BaseTransform):
  def apply(self, data):
    return tft.scale_to_z_score(x=data, *self._args, **self._kwargs)

  def __str__(self):
    return "scale_to_z_score"


# API visible to the user.
def compute_and_apply_vocabulary(
    columns: Optional[List[str]] = None,
    *,
    default_value: Any = -1,
    top_k: Optional[int] = None,
    frequency_threshold: Optional[int] = None,
    num_oov_buckets: int = 0,
    vocab_filename: Optional[str] = None,
    weights: Optional[tf.Tensor] = None,
    labels: Optional[tf.Tensor] = None,
    use_adjusted_mutual_info: bool = False,
    min_diff_from_avg: float = 0.0,
    coverage_top_k: Optional[int] = None,
    coverage_frequency_threshold: Optional[int] = None,
    key_fn: Optional[Callable[[Any], Any]] = None,
    fingerprint_shuffle: bool = False,
    file_format: common_types.VocabularyFileFormatType = analyzers.
    DEFAULT_VOCABULARY_FILE_FORMAT,
    store_frequency: Optional[bool] = False,
    name: Optional[str] = None,
):

  return _ComputeAndApplyVocab(
      columns=columns,
      default_value=default_value,
      top_k=top_k,
      frequency_threshold=frequency_threshold,
      num_oov_buckets=num_oov_buckets,
      vocab_filename=vocab_filename,
      weights=weights,
      labels=labels,
      use_adjusted_mutual_info=use_adjusted_mutual_info,
      min_diff_from_avg=min_diff_from_avg,
      coverage_top_k=coverage_top_k,
      key_fn=key_fn,
      coverage_frequency_threshold=coverage_frequency_threshold,
      fingerprint_shuffle=fingerprint_shuffle,
      file_format=file_format,
      store_frequency=store_frequency,
      name=name)


def scale_to_z_score(
    columns: Optional[List[str]] = None,
    *,
    elementwise: bool = False,
    name: Optional[str] = None,
    output_dtype: Optional[tf.DType] = None):
  return _Scale_To_Z_Score(
      columns=columns,
      elementwise=elementwise,
      name=name,
      output_dtype=output_dtype)


# MLTransform - framework agnostic PTransform


class MLTransform(beam.PTransform):
  def __init__(self, transforms: Optional[List[_BaseTransform]] = None):
    self._transforms = transforms if transforms else []
    self._process_handler = TFTProcessHandler(self._transforms)

  def expand(self, pcoll):
    return (
        pcoll
        | beam.ParDo(_MLTransformDoFn(process_handler=self._process_handler)))

  def with_transform(self, transform: _BaseTransform):
    self._transforms.append(transform)
    return self


class _MLTransformDoFn(beam.DoFn):
  def __init__(self, process_handler):
    self._process_handler = process_handler

  def process(self, element):
    transformed_dataset, transform_fn = self._process_handler.process_data(element)
    yield transformed_dataset[0]


if __name__ == '__main__':
  # row-oriented instance dict dataset
  data = [[{
      'x': "hello", 'y': 1.0
  }, {
      'x': "world", 'y': 2.0
  }, {
      "x": "hello", 'y': 3.0
  }]]

  pipeline = beam.Pipeline()

  with pipeline as p:
    (
        p
        | beam.Create(data)
        | MLTransform().with_transform(
            compute_and_apply_vocabulary(columns=['x']))
        | beam.Map(print))
