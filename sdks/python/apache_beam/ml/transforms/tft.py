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
MLTransform class, which provides a convenient interface for
applying a sequence of data processing transforms to a dataset.

See the documentation for MLTransform for more details.

Note: The data processing transforms defined in this module don't
perform the transformation immediately. Instead, it returns a
configured operation object, which encapsulates the details of the
transformation. The actual computation takes place later in the Apache Beam
pipeline, after all transformations are set up and the pipeline is run.
"""

# pytype: skip-file

import logging
from collections.abc import Iterable
from typing import Any
from typing import Optional
from typing import Union

import apache_beam as beam
import tensorflow as tf
import tensorflow_transform as tft
from apache_beam.ml.transforms.base import BaseOperation
from tensorflow_transform import common_types

__all__ = [
    'ComputeAndApplyVocabulary',
    'ScaleToZScore',
    'ScaleTo01',
    'ScaleToGaussian',
    'ApplyBuckets',
    'ApplyBucketsWithInterpolation',
    'Bucketize',
    'TFIDF',
    'TFTOperation',
    'ScaleByMinMax',
    'NGrams',
    'BagOfWords',
    'HashStrings',
    'DeduplicateTensorPerRow',
]

# Register the expected input types for each operation
# this will be used to determine schema for the tft.AnalyzeDataset
_EXPECTED_TYPES: dict[str, Union[int, str, float]] = {}

_LOGGER = logging.getLogger(__name__)


def register_input_dtype(type):
  def wrapper(fn):
    _EXPECTED_TYPES[fn.__name__] = type
    return fn

  return wrapper


# TODO: https://github.com/apache/beam/pull/29016
# Add support for outputting artifacts to a text file in human readable form.
class TFTOperation(BaseOperation[common_types.TensorType,
                                 common_types.TensorType]):
  def __init__(self, columns: list[str]) -> None:
    """
    Base Operation class for TFT data processing transformations.
    Processing logic for the transformation is defined in the
    apply_transform() method. If you have a custom transformation that is not
    supported by the existing transforms, you can extend this class
    and implement the apply_transform() method.
    Args:
      columns: List of column names to apply the transformation.
    """
    super().__init__(columns)
    if not columns:
      raise RuntimeError(
          "Columns are not specified. Please specify the column for the "
          " op %s" % self.__class__.__name__)

  def get_ptransform_for_processing(self, **kwargs) -> beam.PTransform:
    from apache_beam.ml.transforms.handlers import TFTProcessHandler
    params = {}
    artifact_location = kwargs.get('artifact_location')
    if not artifact_location:
      raise RuntimeError(
          "artifact_location is not specified. Please specify the "
          "artifact_location for the op %s" % self.__class__.__name__)

    artifact_mode = kwargs.get('artifact_mode')
    if artifact_mode:
      params['artifact_mode'] = artifact_mode
    return TFTProcessHandler(artifact_location=artifact_location, **params)

  @tf.function
  def _split_string_with_delimiter(self, data, delimiter):
    """
    only applicable to string columns.
    """
    data = tf.sparse.to_dense(data)
    # this method acts differently compared to tf.strings.split
    # this will split the string based on multiple delimiters while
    # the latter will split the string based on a single delimiter.
    fn = lambda data: tf.compat.v1.string_split(
        data, delimiter, result_type='RaggedTensor')
    # tf.compat.v1.string_split works on a single string. Use tf.map_fn
    # to apply the function on each element of the input data.
    data = tf.map_fn(
        fn,
        data,
        fn_output_signature=tf.RaggedTensorSpec(
            tf.TensorShape([None, None]), tf.string))
    data = data.values.to_sparse()
    # the columns of the sparse tensor are suffixed with $indices, $values
    # related to sparse tensor. Create a new sparse tensor by extracting
    # the indices, values and dense_shape from the original sparse tensor
    # to preserve the original column name.
    data = tf.sparse.SparseTensor(
        indices=data.indices, values=data.values, dense_shape=data.dense_shape)
    # for list of string, batch dimensions becomes inverted after tf.map_fn,
    #  transpose the data to get the original shape.
    if tf.shape(data)[1] == 1:
      data = tf.sparse.transpose(data)
    return data


@register_input_dtype(str)
class ComputeAndApplyVocabulary(TFTOperation):
  def __init__(
      self,
      columns: list[str],
      split_string_by_delimiter: Optional[str] = None,
      *,
      default_value: Any = -1,
      top_k: Optional[int] = None,
      frequency_threshold: Optional[int] = None,
      num_oov_buckets: int = 0,
      vocab_filename: Optional[str] = None,
      name: Optional[str] = None):
    """
    This function computes the vocabulary for the given columns of incoming
    data. The transformation converts the input values to indices of the
    vocabulary.

    Args:
      columns: List of column names to apply the transformation.
      split_string_by_delimiter: (Optional) A string that specifies the
        delimiter to split strings.
      default_value: (Optional) The value to use for out-of-vocabulary values.
      top_k: (Optional) The number of most frequent tokens to keep.
      frequency_threshold: (Optional) Limit the generated vocabulary only to
        elements whose absolute frequency is >= to the supplied threshold.
        If set to None, the full vocabulary is generated.
      num_oov_buckets:  Any lookup of an out-of-vocabulary token will return a
        bucket ID based on its hash if `num_oov_buckets` is greater than zero.
        Otherwise it is assigned the `default_value`.
      vocab_filename: The file name for the vocabulary file. The vocab file
        will be suffixed with the column name.
        NOTE in order to make your pipelines resilient to implementation
        details please set `vocab_filename` when you are using
        the vocab_filename on a downstream component.
    """
    super().__init__(columns)
    self._default_value = default_value
    self._top_k = top_k
    self._frequency_threshold = frequency_threshold
    self._num_oov_buckets = num_oov_buckets
    self._vocab_filename = vocab_filename
    self._name = name
    self.split_string_by_delimiter = split_string_by_delimiter

  def apply_transform(
      self, data: common_types.TensorType,
      output_column_name: str) -> dict[str, common_types.TensorType]:

    if self.split_string_by_delimiter:
      data = self._split_string_with_delimiter(
          data, self.split_string_by_delimiter)

    vocab_filename = self._vocab_filename
    if vocab_filename:
      vocab_filename = vocab_filename + f'_{output_column_name}'
    return {
        output_column_name: tft.compute_and_apply_vocabulary(
            x=data,
            default_value=self._default_value,
            top_k=self._top_k,
            frequency_threshold=self._frequency_threshold,
            num_oov_buckets=self._num_oov_buckets,
            vocab_filename=vocab_filename,
            name=self._name)
    }


@register_input_dtype(float)
class ScaleToZScore(TFTOperation):
  def __init__(
      self,
      columns: list[str],
      *,
      elementwise: bool = False,
      name: Optional[str] = None):
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
    super().__init__(columns)
    self.elementwise = elementwise
    self.name = name

  def apply_transform(
      self, data: common_types.TensorType,
      output_column_name: str) -> dict[str, common_types.TensorType]:
    output_dict = {
        output_column_name: tft.scale_to_z_score(
            x=data, elementwise=self.elementwise, name=self.name)
    }
    return output_dict


@register_input_dtype(float)
class ScaleTo01(TFTOperation):
  def __init__(
      self,
      columns: list[str],
      elementwise: bool = False,
      name: Optional[str] = None):
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

    ScaleTo01 also outputs additional artifacts. The artifacts are
    max, which is the maximum value in the column, and min, which is the
    minimum value in the column. The artifacts are stored in the column
    named with the suffix <original_col_name>_min and <original_col_name>_max
    respectively.

    """
    super().__init__(columns)
    self.elementwise = elementwise
    self.name = name

  def apply_transform(
      self, data: common_types.TensorType,
      output_column_name: str) -> dict[str, common_types.TensorType]:
    output = tft.scale_to_0_1(
        x=data, elementwise=self.elementwise, name=self.name)

    output_dict = {output_column_name: output}
    return output_dict


@register_input_dtype(float)
class ScaleToGaussian(TFTOperation):
  def __init__(
      self,
      columns: list[str],
      elementwise: bool = False,
      name: Optional[str] = None):
    """
    This operation scales the given input column values to an approximately 
    normal distribution with mean 0 and variance of 1. The Gaussian
    transformation is only applied if the column has long tails;
    otherwise, the transformation is the same as normalizing to z scores.

    For more information, see: 
    https://www.tensorflow.org/tfx/transform/api_docs/python/tft/scale_to_gaussian

    Args:
      columns: A list of column names to apply the transformation on.
      elementwise: If True, the transformation is applied elementwise.
        Otherwise, the transformation is applied on the entire column.
      name: A name for the operation (optional).

    """
    super().__init__(columns)
    self.elementwise = elementwise
    self.name = name

  def apply_transform(
      self, data: common_types.TensorType,
      output_column_name: str) -> dict[str, common_types.TensorType]:
    output_dict = {
        output_column_name: tft.scale_to_gaussian(
            x=data, elementwise=self.elementwise, name=self.name)
    }
    return output_dict


@register_input_dtype(float)
class ApplyBuckets(TFTOperation):
  def __init__(
      self,
      columns: list[str],
      bucket_boundaries: Iterable[Union[int, float]],
      name: Optional[str] = None):
    """
    This functions is used to map the element to a positive index i for
    which `bucket_boundaries[i-1] <= element < bucket_boundaries[i]`,
    if it exists. If `input < bucket_boundaries[0]`, then element is
    mapped to 0. If `element >= bucket_boundaries[-1]`, then element is
    mapped to len(bucket_boundaries). NaNs are mapped to
    len(bucket_boundaries).

    Args:
      columns: A list of column names to apply the transformation on.
      bucket_boundaries: An iterable of ints or floats representing the bucket
        boundaries. Must be sorted in ascending order.
      name: (Optional) A string that specifies the name of the operation.
    """
    super().__init__(columns)
    self.bucket_boundaries = [bucket_boundaries]
    self.name = name

  def apply_transform(
      self, data: common_types.TensorType,
      output_column_name: str) -> dict[str, common_types.TensorType]:
    output = {
        output_column_name: tft.apply_buckets(
            x=data, bucket_boundaries=self.bucket_boundaries, name=self.name)
    }
    return output


@register_input_dtype(float)
class ApplyBucketsWithInterpolation(TFTOperation):
  def __init__(
      self,
      columns: list[str],
      bucket_boundaries: Iterable[Union[int, float]],
      name: Optional[str] = None):
    """Interpolates values within the provided buckets and then normalizes to
    [0, 1].
    
    Input values are bucketized based on the provided boundaries such that the
    input is mapped to a positive index i for which `bucket_boundaries[i-1] <=
    element < bucket_boundaries[i]`, if it exists. The values are then
    normalized to the range [0,1] within the bucket, with NaN values being
    mapped to 0.5.

    For more information, see:
    https://www.tensorflow.org/tfx/transform/api_docs/python/tft/apply_buckets_with_interpolation

    Args:
      columns: A list of column names to apply the transformation on.
      bucket_boundaries: An iterable of ints or floats representing the bucket
        boundaries sorted in ascending order.
      name: (Optional) A string that specifies the name of the operation.
    """
    super().__init__(columns)
    self.bucket_boundaries = [bucket_boundaries]
    self.name = name

  def apply_transform(
      self, data: common_types.TensorType,
      output_column_name: str) -> dict[str, common_types.TensorType]:
    output = {
        output_column_name: tft.apply_buckets_with_interpolation(
            x=data, bucket_boundaries=self.bucket_boundaries, name=self.name)
    }
    return output


@register_input_dtype(float)
class Bucketize(TFTOperation):
  def __init__(
      self,
      columns: list[str],
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
    super().__init__(columns)
    self.num_buckets = num_buckets
    self.epsilon = epsilon
    self.elementwise = elementwise
    self.name = name

  def apply_transform(
      self, data: common_types.TensorType,
      output_column_name: str) -> dict[str, common_types.TensorType]:
    output = {
        output_column_name: tft.bucketize(
            x=data,
            num_buckets=self.num_buckets,
            epsilon=self.epsilon,
            elementwise=self.elementwise,
            name=self.name)
    }
    return output


@register_input_dtype(float)
class TFIDF(TFTOperation):
  def __init__(
      self,
      columns: list[str],
      vocab_size: Optional[int] = None,
      smooth: bool = True,
      name: Optional[str] = None,
  ):
    """
    This function applies a tf-idf transformation on the given columns
    of incoming data.

    TFIDF outputs two artifacts for each column: the vocabulary index and
    the tfidf weight. The vocabulary index is a mapping from the original
    vocabulary to the new vocabulary. The tfidf weight is a mapping
    from the original vocabulary to the tfidf score.

    Input passed to the TFIDF is not modified and used to calculate the
    required artifacts.

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
    super().__init__(columns)
    self.vocab_size = vocab_size
    self.smooth = smooth
    self.name = name
    self.tfidf_weight = None

  def apply_transform(
      self, data: common_types.TensorType,
      output_column_name: str) -> common_types.TensorType:

    if self.vocab_size is None:
      try:
        _LOGGER.info(
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


@register_input_dtype(float)
class ScaleByMinMax(TFTOperation):
  def __init__(
      self,
      columns: list[str],
      min_value: float = 0.0,
      max_value: float = 1.0,
      name: Optional[str] = None):
    """
    This function applies a scaling transformation on the given columns
    of incoming data. The transformation scales the input values to the
    range [min_value, max_value].

    Args:
      columns: A list of column names to apply the transformation on.
      min_value: The minimum value of the output range.
      max_value: The maximum value of the output range.
      name: A name for the operation (optional).
    """
    super().__init__(columns)
    self.min_value = min_value
    self.max_value = max_value
    self.name = name

    if self.max_value <= self.min_value:
      raise ValueError('max_value must be greater than min_value')

  def apply_transform(
      self, data: common_types.TensorType,
      output_column_name: str) -> common_types.TensorType:

    output = tft.scale_by_min_max(
        x=data, output_min=self.min_value, output_max=self.max_value)
    return {output_column_name: output}


@register_input_dtype(str)
class NGrams(TFTOperation):
  def __init__(
      self,
      columns: list[str],
      split_string_by_delimiter: Optional[str] = None,
      *,
      ngram_range: tuple[int, int] = (1, 1),
      ngrams_separator: Optional[str] = None,
      name: Optional[str] = None):
    """
    An n-gram is a contiguous sequence of n items from a given sample of text
    or speech. This operation applies an n-gram transformation to
    specified columns of incoming data, splitting the input data into a
    set of consecutive n-grams.

    Args:
      columns: A list of column names to apply the transformation on.
      split_string_by_delimiter: (Optional) A string that specifies the
        delimiter to split the input strings before computing ngrams.
      ngram_range: A tuple of integers(inclusive) specifying the range of
        n-gram sizes.
      ngrams_separator: A string that will be inserted between each ngram.
      name: A name for the operation (optional).
    """
    super().__init__(columns)
    self.ngram_range = ngram_range
    self.ngrams_separator = ngrams_separator
    self.name = name
    self.split_string_by_delimiter = split_string_by_delimiter

    if ngram_range != (1, 1) and not ngrams_separator:
      raise ValueError(
          'ngrams_separator must be specified when ngram_range is not (1, 1)')

  def apply_transform(
      self, data: common_types.TensorType,
      output_column_name: str) -> dict[str, common_types.TensorType]:
    if self.split_string_by_delimiter:
      data = self._split_string_with_delimiter(
          data, self.split_string_by_delimiter)
    output = tft.ngrams(data, self.ngram_range, self.ngrams_separator)
    return {output_column_name: output}


@register_input_dtype(str)
class BagOfWords(TFTOperation):
  def __init__(
      self,
      columns: list[str],
      split_string_by_delimiter: Optional[str] = None,
      *,
      ngram_range: tuple[int, int] = (1, 1),
      ngrams_separator: Optional[str] = None,
      compute_word_count: bool = False,
      key_vocab_filename: Optional[str] = None,
      name: Optional[str] = None,
  ):
    """
    Bag of words contains the unique words present in the input text.
    This operation applies a bag of words transformation to specified
    columns of incoming data. Also, the transformation accepts a Tuple of
    integers specifying the range of n-gram sizes. The transformation
    splits the input data into a set of consecutive n-grams if ngram_range
    is specified. The n-grams are then converted to a bag of words.
    Also, you can specify a seperator string that will be inserted between
    each ngram.

    Args:
      columns: A list of column names to apply the transformation on.
      split_string_by_delimiter: (Optional) A string that specifies the
        delimiter to split the input strings before computing ngrams.
      ngram_range: A tuple of integers(inclusive) specifying the range of
        n-gram sizes.
      seperator: A string that will be inserted between each ngram.
      compute_word_count: A boolean that specifies whether to compute
        the unique word count over the entire dataset. Defaults to False.
      key_vocab_filename: The file name for the key vocabulary file when
        compute_word_count is True. If empty, a file name
        will be chosen based on the current scope. If provided, the vocab
        file will be suffixed with the column name.
      name: A name for the operation (optional).

    Note that original order of the input may not be preserved.
    """

    self.columns = columns
    self.ngram_range = ngram_range
    self.ngrams_separator = ngrams_separator
    self.name = name
    self.split_string_by_delimiter = split_string_by_delimiter
    self.key_vocab_filename = key_vocab_filename
    if compute_word_count:
      self.compute_word_count_fn = count_unique_words
    else:
      self.compute_word_count_fn = lambda *args, **kwargs: None

    if ngram_range != (1, 1) and not ngrams_separator:
      raise ValueError(
          'ngrams_separator must be specified when ngram_range is not (1, 1)')

  def apply_transform(self, data: tf.SparseTensor, output_col_name: str):
    if self.split_string_by_delimiter:
      data = self._split_string_with_delimiter(
          data, self.split_string_by_delimiter)
    output = tft.bag_of_words(
        data, self.ngram_range, self.ngrams_separator, self.name)
    # word counts are written to the file only if compute_word_count is True
    key_vocab_filename = self.key_vocab_filename
    if key_vocab_filename:
      key_vocab_filename = key_vocab_filename + f'_{output_col_name}'
    self.compute_word_count_fn(data, key_vocab_filename)
    return {output_col_name: output}


def count_unique_words(
    data: tf.SparseTensor, output_vocab_name: Optional[str]) -> None:
  tft.count_per_key(data, key_vocabulary_filename=output_vocab_name)


@register_input_dtype(str)
class HashStrings(TFTOperation):
  def __init__(
      self,
      columns: list[str],
      hash_buckets: int,
      key: Optional[tuple[int, int]] = None,
      name: Optional[str] = None):
    '''Hashes strings into the provided number of buckets.
    
    Args:
      columns: A list of the column names to apply the transformation on.
      hash_buckets: the number of buckets to hash the strings into.
      key: optional. An array of two Python `uint64`. If passed, output will be
        a deterministic function of `strings` and `key`. Note that hashing will
        be slower if this value is specified.
      name: optional. A name for this operation.

    Raises:
      ValueError if `hash_buckets` is not a positive and non-zero integer.
    '''
    self.hash_buckets = hash_buckets
    self.key = key
    self.name = name

    if hash_buckets < 1:
      raise ValueError(
          'number of hash buckets must be positive, got ', hash_buckets)

    super().__init__(columns)

  def apply_transform(
      self, data: common_types.TensorType,
      output_col_name: str) -> dict[str, common_types.TensorType]:
    output_dict = {
        output_col_name: tft.hash_strings(
            strings=data,
            hash_buckets=self.hash_buckets,
            key=self.key,
            name=self.name)
    }
    return output_dict


@register_input_dtype(str)
class DeduplicateTensorPerRow(TFTOperation):
  def __init__(self, columns: list[str], name: Optional[str] = None):
    """ Deduplicates each row (0th dimension) of the provided tensor.

    Args:
      columns: A list of the columns to apply the transformation on.
      name: optional. A name for this operation. 
    """
    self.name = name
    super().__init__(columns)

  def apply_transform(
      self, data: common_types.TensorType,
      output_col_name: str) -> dict[str, common_types.TensorType]:
    output_dict = {
        output_col_name: tft.deduplicate_tensor_per_row(
            input_tensor=data, name=self.name)
    }
    return output_dict
