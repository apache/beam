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

# TODO: Refactor file.

import logging
import typing
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

import apache_beam as beam
import numpy as np
import pyarrow as pa
import tensorflow as tf
import tensorflow_transform as tft
from apache_beam.ml.ml_transform.handlers import (
    TFTProcessHandler, TFTProcessHandlerDict)
from apache_beam.typehints.schemas import named_fields_from_element_type
from tensorflow_transform import analyzers, common_types, tf_utils
from apache_beam.ml.ml_transform.base import _BaseOperation, MLTransform

__all__ = ['compute_and_apply_vocabulary', 'scale_to_z_score', 'MLTransform']


class _TFTOperation(_BaseOperation):
  def __init__(self, columns, *args, **kwargs):
    """
    Constructor for the BaseTransform class. When subclassing this, please make sure
    positional arguments are part of the instance variables.
    """
    self.columns = columns
    self._args = args
    self._kwargs = kwargs
    self.has_artifacts = False

    if not columns:
      # (TODO): Shoud we apply the transform or skip the transform?
      logging.warning(
          "Columns are not specified. Ignoring the transform %s" % self)

  def apply(self, inputs, *args, **kwargs):
    raise NotImplementedError

  def validate_args(self):
    raise NotImplementedError

  def __call__(self, data):
    return self.apply(data, *self._args, **self._kwargs)

  def get_analyzer_artifacts(self, data, col_name):
    pass


class _ComputeAndApplyVocab(_TFTOperation):
  # TODO: Pending outputting artifact.
  def apply(self, data: common_types.ConsistentTensorType):
    return tft.compute_and_apply_vocabulary(x=data, *self._args, **self._kwargs)

  def __str__(self):
    return "compute_and_apply_vocabulary"


class _Scale_To_Z_Score(_TFTOperation):
  def __init__(self, columns, *args, **kwargs):
    super().__init__(columns, *args, **kwargs)
    self.has_artifacts = True

  def apply(self, data):
    return tft.scale_to_z_score(x=data, *self._args, **self._kwargs)

  def get_analyzer_artifacts(self, data, col_name):
    mean_var = tft.analyzers._mean_and_var(data)
    return {
        col_name + '_mean': tf.reshape(mean_var[0], [-1]),
        col_name + '_var': tf.reshape(mean_var[1], [-1])
    }

  def __str__(self):
    return "scale_to_z_score"


class _Scale_0_to_1(_TFTOperation):
  def __init__(self, columns, *args, **kwargs):
    super().__init__(columns, *args, **kwargs)
    self.has_artifacts = True

  def get_analyzer_artifacts(self, data, col_name) -> Dict[str, tf.Tensor]:
    return {
        col_name + '_min': tf.reshape(tft.min(data), [-1]),
        col_name + '_max': tf.reshape(tft.max(data), [-1])
    }

  def apply(self, data: tf.Tensor):
    return tft.scale_to_0_1(x=data, *self._args, **self._kwargs)

  def __str__(self):
    return 'scale_0_to_1'


class _ApplyBuckets(_TFTOperation):
  def __init__(self, columns, bucket_boundaries, *args, **kwargs):
    super().__init__(columns, *args, **kwargs)
    self.bucket_boundaries = bucket_boundaries

  def apply(self, data: tf.Tensor):
    return tft.apply_buckets(
        x=data,
        bucket_boundaries=self.bucket_boundaries * self._args,
        **self._kwargs)

  def __str__(self):
    return 'apply_buckets'


class _Bucketize(_TFTOperation):
  def __init__(self, columns: List[str], num_buckets: int, *args, **kwrags):
    super().__init__(columns, *args, **kwrags)
    self.num_buckets = num_buckets
    self.has_artifacts = True

  def get_analyzer_artifacts(self, data, col_name):
    num_buckets = self.num_buckets
    epsilon = self._kwargs['epsilon']
    weights = self._kwargs['weights']
    elementwise = self._kwargs['elementwise']

    if num_buckets < 1:
      raise ValueError('Invalid num_buckets %d' % num_buckets)

    if isinstance(data, (tf.SparseTensor, tf.RaggedTensor)) and elementwise:
      raise ValueError(
          'bucketize requires `x` to be dense if `elementwise=True`')

    x_values = tf_utils.get_values(data)

    if epsilon is None:
      # See explanation in args documentation for epsilon.
      epsilon = min(1.0 / num_buckets, 0.01)

    quantiles = analyzers.quantiles(
        x_values,
        num_buckets,
        epsilon,
        weights,
        reduce_instance_dims=not elementwise)

    return {col_name + '_quantiles': tf.reshape(quantiles, [-1])}

  def apply(self, data):
    return tft.bucketize(data, self.num_buckets, *self._args, **self._kwargs)


# API visible to the user.


def scale_0_to_1(
    columns, elementwise: bool = False, name: Optional[str] = None):
  return _Scale_0_to_1(columns=columns, elementwise=elementwise, name=name)


def apply_buckets(
    bucket_boundaries: Optional[common_types.BucketBoundariesType],
    columns: Optional[List[str]],
    *,
    name: str):
  return _ApplyBuckets(
      columns=columns, bucket_boundaries=bucket_boundaries, name=name)


def bucketize(
    columns: List[str],
    num_buckets: int,
    *,
    epsilon: Optional[float] = None,
    weights: Optional[tf.Tensor] = None,
    elementwise: bool = False,
    name: Optional[str] = None):
  return _Bucketize(
      columns=columns,
      num_buckets=num_buckets,
      epsilon=epsilon,
      weights=weights,
      elementwise=elementwise,
      name=name)


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
