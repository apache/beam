# coding=utf-8
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


def mltransform_scale_to_0_1(test=None):
  # [START mltransform_scale_to_0_1]
  import apache_beam as beam
  # TODO: refactor this import
  from apache_beam.ml.transforms.base import MLTransform
  from apache_beam.ml.transforms.tft import ScaleTo01
  import tempfile

  data = [
      {
          'x': [1, 2, 3]
      },
      {
          'x': [4, 5, 6]
      },
      {
          'x': [7, 8, 9]
      },
  ]

  artifact_location = tempfile.mkdtemp()
  scale_to_0_1_fn = ScaleTo01(columns=['x'])

  with beam.Pipeline() as p:
    transformed_data = (
        p
        | beam.Create(data)
        | MLTransform(artifact_location=artifact_location).with_transform(
            scale_to_0_1_fn)
        | beam.Map(print))


# [END mltransform_scale_to_0_1]
  if test:
    test(transformed_data)


def ml_transform_compute_and_apply_vocabulary(test=None):
  # [START ml_transform_compute_and_apply_vocabulary]
  import apache_beam as beam
  from apache_beam.ml.transforms.base import MLTransform
  from apache_beam.ml.transforms.tft import ComputeAndApplyVocabulary
  from apache_beam.ml.transforms.utils import ArtifactsFetcher
  import tempfile

  artifact_location = tempfile.mkdtemp()
  data = [
      {
          'x': ['a', 'b', 'c']
      },
      {
          'x': ['d', 'e', 'f']
      },
      {
          'x': ['g', 'h', 'i']
      },
  ]

  compute_and_apply_vocabulary_fn = ComputeAndApplyVocabulary(columns=['x'])

  with beam.Pipeline() as p:
    transformed_data = (
        p
        | beam.Create(data)
        | MLTransform(artifact_location=artifact_location).with_transform(
            compute_and_apply_vocabulary_fn)
        | beam.Map(print))

  # fetching artifacts, vocabulary file, produced by ComputeAndApplyVocabulary.
  artifacts_fetcher = ArtifactsFetcher(artifact_location)
  vocab_list = artifacts_fetcher.get_vocab_list()
  print(vocab_list)
  # [END ml_transform_compute_and_apply_vocabulary]
  if test:
    test(transformed_data)


def mltransform_multiple_transforms(test=None):
  # [START mltransform_multiple_transforms]
  import apache_beam as beam
  from apache_beam.ml.transforms.base import MLTransform
  from apache_beam.ml.transforms.tft import ScaleTo01
  from apache_beam.ml.transforms.tft import ComputeAndApplyVocabulary
  import tempfile

  data = [
      {
          'x': [1, 2, 3], 'y': ['a', 'b', 'c']
      },
      {
          'x': [4, 5, 6], 'y': ['d', 'e', 'f']
      },
      {
          'x': [7, 8, 9], 'y': ['g', 'h', 'i']
      },
  ]

  artifact_location = tempfile.mkdtemp()

  scale_to_0_1_fn = ScaleTo01(columns=['x'])
  compute_and_apply_vocabulary_fn = ComputeAndApplyVocabulary(columns=['y'])

  transforms = [scale_to_0_1_fn, compute_and_apply_vocabulary_fn]

  with beam.Pipeline() as p:
    transformed_data = (
        p
        | beam.Create(data)
        # you can specify multiple transforms in a list or
        # you can chain them using with_transform method
        | MLTransform(
            artifact_location=artifact_location, transforms=transforms)
        | beam.Map(print))


# [END mltransform_multiple_transforms]

  if test:
    test(transformed_data)
