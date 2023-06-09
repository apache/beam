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

"""
This module defines a set of data processing transforms that can be used
to perform common data transformations on a dataset. These transforms are
implemented using the TensorFlow Transform (TFT) library. The transforms
in this module are intended to be used in conjunction with the
beam.ml.MLTransform class, which provides a convenient interface for
applying a sequence of data processing transforms to a dataset with the
help of the ProcessHandler class.

See the documentation for beam.ml.MLTransform for more details.

Since the transforms in this module are implemented using TFT, they
should be wrapped inside a TFTProcessHandler object before being passed
to the beam.ml.MLTransform class. The ProcessHandler will let MLTransform
know which type of input is expected and infers the relevant schema required
for the TFT library.

Note: The data processing transforms defined in this module don't
perform the transformation immediately. Instead, it returns a
configured operation object, which encapsulates the details of the
transformation. The actual computation takes place later in the Apache Beam
pipeline, after all transformations are set up and the pipeline is run.
"""

import logging
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Union

from apache_beam.ml.transforms.base import BaseOperation
import tensorflow as tf
import tensorflow_transform as tft
from tensorflow_transform import analyzers
from tensorflow_transform import common_types
from tensorflow_transform import tf_utils

__all__ = [
    'ComputeAndApplyVocabulary',
    'Scale_To_ZScore',
    'Scale_To_0_1',
    'ApplyBuckets',
    'Bucketize'
]


class TFTOperation(BaseOperation):
  def __init__(self, columns: List[str], **kwargs):
    """
    Base Opertation class for all the TFT operations.
    """
    self.columns = columns
    self._kwargs = kwargs

    if not columns:
      raise RuntimeError(
          "Columns are not specified. Please specify the column for the "
          " op %s" % self)

  def validate_args(self):
    raise NotImplementedError

  def get_artifacts(self, data: common_types.TensorType,
                    col_name) -> Optional[Dict[str, tf.Tensor]]:
    return None


class ComputeAndApplyVocabulary(TFTOperation):
  def __init__(
      self,
      columns: List[str],
      *,
      default_value: Any = -1,
      top_k: Optional[int] = None,
      frequency_threshold: Optional[int] = None,
      num_oov_buckets: int = 0,
      vocab_filename: Optional[str] = None,
      name: Optional[str] = None,
      **kwargs):
    """
    This function computes the vocabulary for the given columns of incoming
    data. The transformation converts the input values to indices of the
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
      vocab_filename: The file name for the vocabulary file. If None,
        a name based on the scope name in the context of this graph will
        be used as the file name. If not None, should be unique within
        a given preprocessing function.
        NOTE in order to make your pipelines resilient to implementation
        details please set `vocab_filename` when you are using
        the vocab_filename on a downstream component.
    """
    super().__init__(columns, **kwargs)
    self._default_value = default_value
    self._top_k = top_k
    self._frequency_threshold = frequency_threshold
    self._num_oov_buckets = num_oov_buckets
    self._vocab_filename = vocab_filename
    self._name = name

  def apply(self, data: common_types.TensorType,
            output_column_name: str) -> Dict[str, common_types.TensorType]:
    # TODO: Pending outputting artifact.
    return {
        output_column_name: tft.compute_and_apply_vocabulary(
            x=data, **self._kwargs)
    }

  def __str__(self):
    return "compute_and_apply_vocabulary"


class Scale_To_ZScore(TFTOperation):
  def __init__(
      self,
      columns: List[str],
      *,
      elementwise: bool = False,
      name: Optional[str] = None,
      **kwargs):
    """
    This function performs a scaling transformation on the specified columns of
    the incoming data. It processes the input data such that it's normalized
    to have a mean of 0 and a variance of 1. The transformation achieves this
    by subtracting the mean from the input data and then dividing it by the
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
    super().__init__(columns, **kwargs)
    self.elementwise = elementwise
    self.name = name

  def apply(self, data: common_types.TensorType,
            output_column_name: str) -> Dict[str, common_types.TensorType]:
    artifacts = self.get_artifacts(data, output_column_name)
    output = {output_column_name: tft.scale_to_z_score(x=data, **self._kwargs)}
    output_dict = {output_column_name: output}
    if artifacts is not None:
      output_dict.update(artifacts)
    return output_dict

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


class Scale_To_0_1(TFTOperation):
  def __init__(
      self,
      columns: List[str],
      elementwise: bool = False,
      name: Optional[str] = None,
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
    super().__init__(columns, **kwargs)
    self.elementwise = elementwise
    self.name = name

  def get_artifacts(self, data: common_types.TensorType,
                    col_name: str) -> Dict[str, tf.Tensor]:
    shape = [tf.shape(data)[0], 1]
    return {
        col_name + '_min': tf.broadcast_to(tft.min(data), shape),
        col_name + '_max': tf.broadcast_to(tft.max(data), shape)
    }

  def apply(self, data: common_types.TensorType,
            output_column_name: str) -> Dict[str, common_types.TensorType]:
    artifacts = self.get_artifacts(data, output_column_name)
    output = tft.scale_to_0_1(x=data, **self._kwargs)

    output_dict = {output_column_name: output}
    if artifacts is not None:
      output_dict.update(artifacts)
    return output_dict

  def __str__(self):
    return 'scale_to_0_1'


class ApplyBuckets(TFTOperation):
  def __init__(
      self,
      columns: List[str],
      bucket_boundaries: Iterable[Union[int, float]],
      name: Optional[str] = None,
      **kwargs):
    """
    This functions is used to map the element to a positive index i for
    which bucket_boundaries[i-1] <= element < bucket_boundaries[i],
    if it exists. If input < bucket_boundaries[0], then element is
    mapped to 0. If element >= bucket_boundaries[-1], then element is
    mapped to len(bucket_boundaries). NaNs are mapped to
    len(bucket_boundaries).

    Args:
      columns: A list of column names to apply the transformation on.
      bucket_boundaries: A rank 2 Tensor or list representing the bucket
        boundaries sorted in ascending order.
      name: (Optional) A string that specifies the name of the operation.
    """
    super().__init__(columns, **kwargs)
    self.bucket_boundaries = [bucket_boundaries]
    self.name = name

  def apply(self, data: common_types.TensorType,
            output_column_name: str) -> Dict[str, common_types.TensorType]:
    output = {
        output_column_name: tft.apply_buckets(
            x=data, bucket_boundaries=self.bucket_boundaries, **self._kwargs)
    }
    return output

  def __str__(self):
    return 'apply_buckets'


class Bucketize(TFTOperation):
  def __init__(
      self,
      columns: List[str],
      num_buckets: int,
      *,
      epsilon: Optional[float] = None,
      elementwise: bool = False,
      name: Optional[str] = None,
      **kwargs):
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
    super().__init__(columns, **kwargs)
    self.num_buckets = num_buckets
    self.epsilon = epsilon
    self.elementwise = elementwise
    self.name = name

  def get_artifacts(self, data: common_types.TensorType,
                    col_name: str) -> Dict[str, tf.Tensor]:
    num_buckets = self.num_buckets
    epsilon = self.epsilon
    elementwise = self.elementwise

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

  def apply(self, data: common_types.TensorType,
            output_column_name: str) -> Dict[str, common_types.TensorType]:
    artifacts = self.get_artifacts(data, output_column_name)
    output = {
        output_column_name: tft.bucketize(
            x=data, num_buckets=self.num_buckets, **self._kwargs)
    }
    output.update(artifacts)
    return output


class TFIDF(TFTOperation):
  def __init__(
      self,
      columns: List[str],
      vocab_size: Optional[int] = None,
      smooth: bool = True,
      name: Optional[str] = None,
      **kwargs):
    """
    This function applies a tf-idf transformation on the given columns
    of incoming data. The transformation computes the tf-idf score for
    each element in the input data.

    Args:
      columns: List of column names to apply the transformation.
      vocab_size: (Optional) An integer that specifies the size of the
        vocabulary. Defaults to None.

        If vocab_size is None, then the size of the vocabulary is
        determined by `tft.get_num_buckets_for_transformed_feature`.
      smooth: (Optional) A boolean that specifies whether to apply
        smoothing to the tf-idf score. Defaults to True.
      name: (Optional) A string that specifies the name of the operation.
    """
    super().__init__(columns, **kwargs)
    self.vocab_size = vocab_size
    self.smooth = smooth
    self.name = name
    self.tfidf_weight = None

  def apply(
      self, data: tf.SparseTensor, output_column_name: str) -> tf.SparseTensor:

    if self.vocab_size is None:
      try:
        logging.info(
            'vocab_size is not specified. Trying to infer vocab_size '
            'from the input data using '
            'tft.get_num_buckets_for_transformed_feature.')
        vocab_size = tft.get_num_buckets_for_transformed_feature(data)
      except RuntimeError:
        raise RuntimeError(
            'vocab_size is not specified. Tried to infer vocab_size from the '
            'input data using tft.get_num_buckets_for_transformed_feature, but '
            'failed. Please specify vocab_size explicitly.')
    else:
      vocab_size = self.vocab_size

    vocab_index, tfidf_weight = tft.tfidf(
      data,
      vocab_size,
      self.smooth,
      self.name
    )

    output = {
        output_column_name + '_vocab_index': vocab_index,
        output_column_name + '_tfidf_weight': tfidf_weight
    }
    return output
