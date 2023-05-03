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

"""
This file contains the methods that wrapped around Tensorflow transform
processing ops such as compute_and_apply_vocabulary, bucketize etc.
"""
# pylint: skip-file
import typing

import tensorflow_transform as tft


class _TFTOperation:
  """
    Interface used for the preprocessing operations.

    Assume the the data in apply method is not indexed/keyed.
    """
  def __init__(
      self,
      columns: typing.Optional[typing.List[str]] = None,
      include_output: bool = False,
      out_name: str = '',
      **kwargs):
    self.columns = columns
    self.kwargs = kwargs
    self.include_output = include_output
    self.out_name = out_name

  def _validate_args(self):
    if (self.include_output and not self.out_name) or (self.out_name and
                                                       not self.include_output):
      raise NotImplemented

  def apply(self, data):
    raise NotImplementedError

  def __call__(self, data):
    self.apply(data)


class _Bucketize(_TFTOperation):
  def apply(self, data):
    return tft.bucketize(x=data, **self.kwargs)


class _ApplyBuckets(_TFTOperation):
  def apply(self, data):
    return tft.apply_buckets(data, **self.kwargs)


import tensorflow as tf


class _ComputeAndApplyVocabulary(_TFTOperation):
  name = 'compute_and_apply'

  def apply(self, data):
    return tft.compute_and_apply_vocabulary(data)


    # return tf.convert_to_tensor([1], dtype=tf.int64)
class _ScaleToZscore(_TFTOperation):
  def apply(self, data):
    return tft.scale_to_z_score(data, self.kwargs)


class _GetNumBucketsForTransformedFeatures(_TFTOperation):
  name = 'get_num_buckets'

  def apply(self, data):
    return tft.get_num_buckets_for_transformed_feature(data)


class _CustomWrapper(_TFTOperation):
  def __init__(self, custom_fn, columns, **kwargs):
    super().__init__(columns=columns, kwargs=kwargs)
    self.custom_fn = custom_fn

  def apply(self, input_tesor):
    return self.custom_fn(input_tesor, self.kwargs)


# Wrappers visible to the public APIs.


def bucketize(columns, **kwargs):
  return _Bucketize(columns=columns, **kwargs)


def apply_buckets(columns, **kwargs):
  return _ApplyBuckets(columns, **kwargs)


def compute_and_apply_vocabulary(columns, **kwargs):
  return _ComputeAndApplyVocabulary(columns, **kwargs)


def scale_to_z_score(columns, **kwargs):
  return _ScaleToZscore(columns=columns, **kwargs)


def get_num_buckets_after_transformation(columns, **kwargs):
  return _GetNumBucketsForTransformedFeatures(columns=columns)


class _TFIDF(_TFTOperation):
  name = 'TFIDF'

  def apply(self, data, **kwargs):
    integerized = tft.compute_and_apply_vocabulary(data)
    vocab_size = tft.get_num_buckets_for_transformed_feature(integerized)
    vocab_index, tfidf_weight = tft.tfidf(integerized, vocab_size)
    return vocab_index


def tfidf(columns, **kwargs):
  return _TFIDF(columns, **kwargs)
