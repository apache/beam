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

# pytype: skip-file
# pylint: disable=reimported
# pylint:disable=line-too-long


def mltransform_scale_to_0_1(test=None):
  # [START mltransform_scale_to_0_1]
  import apache_beam as beam
  from apache_beam.ml.transforms.base import MLTransform
  from apache_beam.ml.transforms.tft import ScaleTo01
  import tempfile

  data = [
      {
          'x': [1, 5, 3]
      },
      {
          'x': [4, 2, 8]
      },
  ]

  artifact_location = tempfile.mkdtemp()
  scale_to_0_1_fn = ScaleTo01(columns=['x'])

  with beam.Pipeline() as p:
    transformed_data = (
        p
        | beam.Create(data)
        | MLTransform(write_artifact_location=artifact_location).with_transform(
            scale_to_0_1_fn)
        | beam.Map(print))
    # [END mltransform_scale_to_0_1]
    if test:
      test(transformed_data)


def mltransform_compute_and_apply_vocabulary(test=None):
  # [START mltransform_compute_and_apply_vocabulary]
  import apache_beam as beam
  from apache_beam.ml.transforms.base import MLTransform
  from apache_beam.ml.transforms.tft import ComputeAndApplyVocabulary
  import tempfile

  artifact_location = tempfile.mkdtemp()
  data = [
      {
          'x': ['I', 'love', 'Beam']
      },
      {
          'x': ['Beam', 'is', 'awesome']
      },
  ]
  compute_and_apply_vocabulary_fn = ComputeAndApplyVocabulary(columns=['x'])
  with beam.Pipeline() as p:
    transformed_data = (
        p
        | beam.Create(data)
        | MLTransform(write_artifact_location=artifact_location).with_transform(
            compute_and_apply_vocabulary_fn)
        | beam.Map(print))
    # [END mltransform_compute_and_apply_vocabulary]
    if test:
      test(transformed_data)


def mltransform_compute_and_apply_vocabulary_with_scalar(test=None):
  # [START mltransform_compute_and_apply_vocabulary_with_scalar]
  import apache_beam as beam
  from apache_beam.ml.transforms.base import MLTransform
  from apache_beam.ml.transforms.tft import ComputeAndApplyVocabulary
  import tempfile
  data = [
      {
          'x': 'I'
      },
      {
          'x': 'love'
      },
      {
          'x': 'Beam'
      },
      {
          'x': 'Beam'
      },
      {
          'x': 'is'
      },
      {
          'x': 'awesome'
      },
  ]
  artifact_location = tempfile.mkdtemp()
  compute_and_apply_vocabulary_fn = ComputeAndApplyVocabulary(columns=['x'])
  with beam.Pipeline() as p:
    transformed_data = (
        p
        | beam.Create(data)
        | MLTransform(write_artifact_location=artifact_location).with_transform(
            compute_and_apply_vocabulary_fn)
        | beam.Map(print))
    # [END mltransform_compute_and_apply_vocabulary_with_scalar]
    if test:
      test(transformed_data)
