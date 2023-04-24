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

import typing

import tensorflow_transform as tft


class _TFTOperation:
  """
    Interface used for the preprocessing operations.

    Assume the the data in apply method is not indexed/keyed.
    """
  def __init__(
      self, columns: typing.Optional[typing.List[str]] = None, **kwargs):
    self.columns = columns
    self.kwargs = kwargs

  def apply(self, data):
    pass


class _Bucketize(_TFTOperation):
  def apply(self, data):
    return tft.bucketize(x=data, **self.kwargs)


class _ApplyBuckets(_TFTOperation):
  def apply(self, data):
    return tft.apply_buckets(data, **self.kwargs)


class _ComputeAndApplyVocabulary(_TFTOperation):
  def apply(self, data):
    return tft.compute_and_apply_vocabulary(data)


class _ScaleToZscore(_TFTOperation):
  def apply(self, data):
    return tft.scale_to_z_score(data, self.kwargs)


# Wrappers visible to the public APIs.


def bucketize(columns, **kwargs):
  return _Bucketize(columns=columns, **kwargs)


def apply_buckets(columns, **kwargs):
  return _ApplyBuckets(columns, **kwargs)


def compute_and_apply_vocabulary(columns, **kwargs):
  return _ComputeAndApplyVocabulary(columns, **kwargs)


def scale_to_z_score(columns, **kwargs):
  return _ScaleToZscore(columns=columns, **kwargs)
