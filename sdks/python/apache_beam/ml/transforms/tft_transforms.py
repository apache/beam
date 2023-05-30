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

from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from apache_beam.ml.transforms.base import _BaseOperation
import tensorflow as tf
import tensorflow_transform as tft
from tensorflow_transform import analyzers
from tensorflow_transform import common_types
from tensorflow_transform import tf_utils

__all__ = [
    'compute_and_apply_vocabulary',
    'scale_to_z_score',
    'scale_to_0_1',
    'apply_buckets',
    'bucketize'
]


class _TFTOperation(_BaseOperation):
  def __init__(
      self, columns, save_result=False, output_name=None, *args, **kwargs):
    """
    When subclassing _TFTOperation, please make sure
    positional arguments are part of the instance variables.
    """
    self.columns = columns
    self._args = args
    self._kwargs = kwargs
    self.has_artifacts = False

    self._save_result = save_result
    self._output_name = output_name
    if not columns:
      raise RuntimeError(
          "Columns are not specified. Please specify the column for the "
          " op %s" % self)

    if self._save_result and not self._output_name:
      raise RuntimeError(
          "Inplace is set to True. "
          "but output name in which transformed data is stored"
          " is not specified. Please specify the output name for "
          " the op %s" % self)

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
    shape = [tf.shape(data)[0], 1]
    return {
        col_name + '_mean': tf.broadcast_to(mean_var[0], shape),
        col_name + '_var': tf.broadcast_to(mean_var[1], shape),
    }

  def __str__(self):
    return "scale_to_z_score"


class _Scale_to_0_1(_TFTOperation):
  def __init__(self, columns, *args, **kwargs):
    super().__init__(columns, *args, **kwargs)
    self.has_artifacts = True

  def get_analyzer_artifacts(self, data, col_name) -> Dict[str, tf.Tensor]:
    shape = [tf.shape(data)[0], 1]
    return {
        col_name + '_min': tf.broadcast_to(tft.min(data), shape),
        col_name + '_max': tf.broadcast_to(tft.max(data), shape)
    }

  def apply(self, data: tf.Tensor):
    return tft.scale_to_0_1(x=data, *self._args, **self._kwargs)

  def __str__(self):
    return 'scale_to_0_1'


class _ApplyBuckets(_TFTOperation):
  def __init__(self, columns, bucket_boundaries, name=None, *args, **kwargs):
    super().__init__(columns, *args, **kwargs)
    self.bucket_boundaries = bucket_boundaries
    self.name = name

  def apply(self, data: tf.Tensor):
    return tft.apply_buckets(
        x=data, bucket_boundaries=self.bucket_boundaries, name=self.name)

  def __str__(self):
    return 'apply_buckets'


class _Bucketize(_TFTOperation):
  """
  class that bucketizes the input data using the apply method.
  """
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
    shape = [
        tf.shape(data)[0], num_buckets - 1 if num_buckets > 1 else num_buckets
    ]
    # These quantiles are used as the bucket boundaries in the later stages.
    # Should we change the prefix _quantiles to _bucket_boundaries?
    return {col_name + '_quantiles': tf.broadcast_to(quantiles, shape)}

  def apply(self, data):
    return tft.bucketize(data, self.num_buckets, *self._args, **self._kwargs)


def scale_to_0_1(
    columns, elementwise: bool = False, name: Optional[str] = None):
  return _Scale_to_0_1(columns=columns, elementwise=elementwise, name=name)


def apply_buckets(
    bucket_boundaries: Optional[common_types.BucketBoundariesType],
    columns: Optional[List[str]],
    *,
    name: Optional[str] = None):
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
  """
  This function applies a bucketizing transformation on the given columns
  of incoming data. The transformation splits the input data range into
  a set of consecutive bins/buckets, and converts the input values to
  bucket IDs (integers) where each ID corresponds to a particular bin.

  This operation is used within the beam.MLTransform process.

  Example usage:
  with beam.Pipeline() as p:
    data = <data_pcoll>
    data | beam.MLTransform(process_handler=<process_handler>
    ).with_transform(bucketize(columns=['col1'], num_buckets=10)

  Args:
    columns: List of column names to apply the transformation.
    num_buckets: Number of buckets to be created.
    epsilon: (Optional) A float number that specifies the error tolerance
      when computing quantiles, so that we guarantee that any value x will
      have a quantile q such that x is in the interval
      [q - epsilon, q + epsilon] (or the symmetric interval for even
      num_buckets). Must be greater than 0.0.
    weights: (Optional) A Tensor. The weight column to be used for
      quantile computation.
    elementwise: (Optional) A boolean that specifies whether the quantiles
      should be computed on an element-wise basis. If False, the quantiles
      are computed globally.
    name: (Optional) A string that specifies the name of the operation.
  """
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
    name: Optional[str] = None,
):
  """
  This function computes the vocabulary for the given columns of incoming data.
  The transformation converts the input values to indices of the
  vocabulary.

  Args:
    columns: List of column names to apply the transformation.
    default_value: (Optional) The value to use for out-of-vocabulary values.
    top_k: (Optional) The number of most frequent tokens to keep.
    frequency_threshold: (Optional) Limit the generated vocabulary only to
      elements whose absolute frequency is >= to the supplied threshold.
      If set to None, the full vocabulary is generated.
    num_oov_buckets:  Any lookup of an out-of-vocabulary token will return a
      bucket ID based on its hash if `num_oov_buckets` is greater than zero.
      Otherwise it is assigned the `default_value`.
    vocab_filename: The file name for the vocabulary file. If None, a name based
      on the scope name in the context of this graph will be used as the file
      name. If not None, should be unique within a given preprocessing function.
      NOTE in order to make your pipelines resilient to implementation details
      please set `vocab_filename` when you are using the vocab_filename on a
      downstream component.
    weights: (Optional) Weights for the vocabulary. It must have the same shape
      as the incoming data.
    """
  return _ComputeAndApplyVocab(
      columns=columns,
      default_value=default_value,
      top_k=top_k,
      frequency_threshold=frequency_threshold,
      num_oov_buckets=num_oov_buckets,
      vocab_filename=vocab_filename,
      weights=weights,
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
