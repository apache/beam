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
from typing import Iterable
from typing import List
from typing import Optional
from typing import Union

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
      self,
      columns: List[str],
      save_result: bool = False,
      output_name: str = '',
      *args,
      **kwargs):
    """
    When subclassing _TFTOperation, please make sure
    positional arguments are part of the instance variables.
    """
    self.columns = columns
    self._args = args
    self._kwargs = kwargs

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

  def validate_args(self):
    raise NotImplementedError

  def get_artifacts(self, data: common_types.TensorType,
                    col_name) -> Optional[Dict[str, tf.Tensor]]:
    return None


class _ComputeAndApplyVocab(_TFTOperation):
  # TODO: Pending outputting artifact.
  def apply(self, data: common_types.TensorType) -> common_types.TensorType:
    return tft.compute_and_apply_vocabulary(x=data, *self._args, **self._kwargs)

  def __str__(self):
    return "compute_and_apply_vocabulary"


class _Scale_To_Z_Score(_TFTOperation):
  def __init__(self, columns: List[str], *args, **kwargs):
    super().__init__(columns, *args, **kwargs)

  def apply(self, data: common_types.TensorType) -> common_types.TensorType:
    return tft.scale_to_z_score(x=data, *self._args, **self._kwargs)

  def get_artifacts(self, data: common_types.TensorType,
                    col_name: str) -> Dict[str, tf.Tensor]:
    mean_var = tft.analyzers._mean_and_var(data)
    shape = [tf.shape(data)[0], 1]
    return {
        col_name + '_mean': tf.broadcast_to(mean_var[0], shape),
        col_name + '_var': tf.broadcast_to(mean_var[1], shape),
    }

  def __str__(self):
    return "scale_to_z_score"


class _Scale_to_0_1(_TFTOperation):
  def __init__(self, columns: List[str], *args, **kwargs):
    super().__init__(columns, *args, **kwargs)

  def get_artifacts(self, data: common_types.TensorType,
                    col_name: str) -> Dict[str, tf.Tensor]:
    shape = [tf.shape(data)[0], 1]
    return {
        col_name + '_min': tf.broadcast_to(tft.min(data), shape),
        col_name + '_max': tf.broadcast_to(tft.max(data), shape)
    }

  def apply(self, data: common_types.TensorType) -> common_types.TensorType:
    return tft.scale_to_0_1(x=data, *self._args, **self._kwargs)

  def __str__(self):
    return 'scale_to_0_1'


class _ApplyBuckets(_TFTOperation):
  def __init__(
      self,
      columns: List[str],
      bucket_boundaries: Iterable[Union[int, float]],
      name: Optional[str] = None,
      *args,
      **kwargs):
    super().__init__(columns, *args, **kwargs)
    self.bucket_boundaries = [bucket_boundaries]
    self.name = name

  def apply(self, data: common_types.TensorType) -> common_types.TensorType:
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

  def get_artifacts(self, data: common_types.TensorType,
                    col_name: str) -> Dict[str, tf.Tensor]:
    num_buckets = self.num_buckets
    epsilon = self._kwargs['epsilon']
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
        x_values, num_buckets, epsilon, reduce_instance_dims=not elementwise)
    shape = [
        tf.shape(data)[0], num_buckets - 1 if num_buckets > 1 else num_buckets
    ]
    # These quantiles are used as the bucket boundaries in the later stages.
    # Should we change the prefix _quantiles to _bucket_boundaries?
    return {col_name + '_quantiles': tf.broadcast_to(quantiles, shape)}

  def apply(self, data: common_types.TensorType) -> common_types.TensorType:
    return tft.bucketize(data, self.num_buckets, *self._args, **self._kwargs)


def scale_to_0_1(
    columns: List[str],
    elementwise: bool = False,
    name: Optional[str] = None,
    *args,
    **kwargs):
  """
  This function applies a scaling transformation on the given columns
  of incoming data. The transformation scales the input values to the
  range [0, 1] by dividing each value by the maximum value in the
  column.

  Args:
    columns: A list of column names to apply the transformation on.
    elementwise: If True, the transformation is applied elementwise.
      Otherwise, the transformation is applied on the entire column.
    name: A name for the operation (optional).

  scale_to_0_1 also outputs additional artifacts. The artifacts are
  max, which is the maximum value in the column, and min, which is the
  minimum value in the column. The artifacts are stored in the column
  named with the suffix <original_col_name>_min and <original_col_name>_max
  respectively.

  """
  return _Scale_to_0_1(
      columns=columns, elementwise=elementwise, name=name, *args, **kwargs)


def apply_buckets(
    bucket_boundaries: Iterable[Union[int, float]],
    columns: List[str],
    *,
    name: Optional[str] = None):
  """
  This functions is used to map the element to a positive index i for
  which bucket_boundaries[i-1] <= element < bucket_boundaries[i], if it exists.
  If input < bucket_boundaries[0], then element is mapped to 0.
  If element >= bucket_boundaries[-1], then element is mapped to
  len(bucket_boundaries). NaNs are mapped to len(bucket_boundaries)

  Args:
    columns: A list of column names to apply the transformation on.
    bucket_boundaries: A rank 2 Tensor or list representing the bucket
      boundaries sorted in ascending order.
    name: (Optional) A string that specifies the name of the operation.
  """
  return _ApplyBuckets(
      columns=columns, bucket_boundaries=bucket_boundaries, name=name)


def bucketize(
    columns: List[str],
    num_buckets: int,
    *,
    epsilon: Optional[float] = None,
    elementwise: bool = False,
    name: Optional[str] = None):
  """
  This function applies a bucketizing transformation on the given columns
  of incoming data. The transformation splits the input data range into
  a set of consecutive bins/buckets, and converts the input values to
  bucket IDs (integers) where each ID corresponds to a particular bin.

  Args:
    columns: List of column names to apply the transformation.
    num_buckets: Number of buckets to be created.
    epsilon: (Optional) A float number that specifies the error tolerance
      when computing quantiles, so that we guarantee that any value x will
      have a quantile q such that x is in the interval
      [q - epsilon, q + epsilon] (or the symmetric interval for even
      num_buckets). Must be greater than 0.0.
    elementwise: (Optional) A boolean that specifies whether the quantiles
      should be computed on an element-wise basis. If False, the quantiles
      are computed globally.
    name: (Optional) A string that specifies the name of the operation.
  """
  return _Bucketize(
      columns=columns,
      num_buckets=num_buckets,
      epsilon=epsilon,
      elementwise=elementwise,
      name=name)


def compute_and_apply_vocabulary(
    columns: List[str],
    *,
    default_value: Any = -1,
    top_k: Optional[int] = None,
    frequency_threshold: Optional[int] = None,
    num_oov_buckets: int = 0,
    vocab_filename: Optional[str] = None,
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
    """
  return _ComputeAndApplyVocab(
      columns=columns,
      default_value=default_value,
      top_k=top_k,
      frequency_threshold=frequency_threshold,
      num_oov_buckets=num_oov_buckets,
      vocab_filename=vocab_filename,
      name=name)


def scale_to_z_score(
    columns: List[str],
    *,
    elementwise: bool = False,
    name: Optional[str] = None):
  """
  This function performs a scaling transformation on the specified columns of
  the incoming data. It processes the input tensor such that it's normalized
  to have a mean of 0 and a variance of 1. The transformation achieves this
  by subtracting the mean from the input tensor and then dividing it by the
  square root of the variance.

  Args:
    columns: A list of column names to apply the transformation on.
    elementwise: If True, the transformation is applied elementwise.
      Otherwise, the transformation is applied on the entire column.
    name: A name for the operation (optional).

  scale_to_z_score also outputs additional artifacts. The artifacts are
  mean, which is the mean value in the column, and var, which is the
  variance in the column. The artifacts are stored in the column
  named with the suffix <original_col_name>_mean and <original_col_name>_var
  respectively.
  """

  return _Scale_To_Z_Score(columns=columns, elementwise=elementwise, name=name)
