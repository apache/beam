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
This example demonstrates how to use MLTransform.
MLTransform is a PTransform that applies multiple data transformations on the
incoming data.

This example computes the vocabulary on the incoming data. Then, it computes
the TF-IDF of the incoming data using the vocabulary computed in the previous
step.

1. ComputeAndApplyVocabulary computes the vocabulary on the incoming data and
   overrides the incoming data with the vocabulary indices.
2. TFIDF computes the TF-IDF of the incoming data using the vocabulary and
    provides vocab_index and tf-idf weights. vocab_index is suffixed with
    '_vocab_index' and tf-idf weights are suffixed with '_tfidf' to the
    original column name(which is the output of ComputeAndApplyVocabulary).

MLTransform produces artifacts, for example: ComputeAndApplyVocabulary produces
a text file that contains vocabulary which is saved in `artifact_location`.
ComputeAndApplyVocabulary outputs vocab indices associated with the saved vocab
file. This mode of MLTransform is called artifact `produce` mode.
This will be useful when the data is preprocessed before ML model training.

The second mode of MLTransform is artifact `consume` mode. In this mode, the
transformations are applied on the incoming data using the artifacts produced
by the previous run of MLTransform. This mode will be useful when the data is
preprocessed before ML model inference.
"""

import argparse
import logging
import tempfile

import apache_beam as beam
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.ml.transforms.tft import TFIDF
from apache_beam.ml.transforms.tft import ComputeAndApplyVocabulary
from apache_beam.ml.transforms.utils import ArtifactsFetcher


def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--artifact_location', type=str, default='')
  return parser.parse_known_args()


def preprocess_data_for_ml_training(train_data, args):
  """
  Preprocess the data for ML training. This method runs a pipeline to
  preprocess the data needed for ML training. It produces artifacts that can
  be used for ML inference later.
  """

  with beam.Pipeline() as p:
    train_data_pcoll = (p | "CreateData" >> beam.Create(train_data))

    # When using write_artifact_location, the ComputeAndApplyVocabulary
    # function generates a vocabulary file. This file, stored in
    # 'write_artifact_location', contains the vocabulary of the entire dataset.
    # This is considered as an artifact of ComputeAndApplyVocabulary transform.
    # The indices of the vocabulary in this file are returned as
    # the output of MLTransform.
    transformed_data_pcoll = (
        train_data_pcoll
        | 'MLTransform' >> MLTransform(
            write_artifact_location=args.artifact_location,
        ).with_transform(ComputeAndApplyVocabulary(
            columns=['x'])).with_transform(TFIDF(columns=['x'])))

    _ = transformed_data_pcoll | beam.Map(logging.info)
    # output for the element dict(x=["Let's", "go", "to", "the", "park"])
    # will be:
    # Row(x=array([21,  5,  0,  2,  3]),
    # x_tfidf_weight=array([0.28109303, 0.36218604, 0.36218604, 0.41972247,
    # 0.5008155 ], dtype=float32), x_vocab_index=array([ 0,  2,  3,  5, 21]))


def preprocess_data_for_ml_inference(test_data, args):
  """
  Preprocess the data for ML inference. This method runs a pipeline to
  preprocess the data needed for ML inference. It consumes the artifacts
  produced during the preprocessing stage for ML training.
  """
  with beam.Pipeline() as p:

    test_data_pcoll = (p | beam.Create(test_data))
    # Here, the previously saved vocabulary from an MLTransform run is used by
    # ComputeAndApplyVocabulary to access and apply the stored artifacts to the
    # test data.
    transformed_data_pcoll = (
        test_data_pcoll
        | "MLTransformOnTestData" >> MLTransform(
            read_artifact_location=args.artifact_location,
            # ww don't need to specify transforms as they are already saved in
            # in the artifacts.
        ))
    _ = transformed_data_pcoll | beam.Map(logging.info)
    # output for dict(x=['I', 'love', 'books']) will be:
    # Row(x=array([1, 4, 7]),
    # x_tfidf_weight=array([0.4684884 , 0.6036434 , 0.69953746], dtype=float32)
    # , x_vocab_index=array([1, 4, 7]))


def run(args):
  """
  This example demonstrates how to use MLTransform in ML workflow.
  1. Preprocess the data for ML training.
  2. Do some ML model training.
  3. Preprocess the data for ML inference.

  training and inference on ML modes are not shown in this example.
  This example only shows how to use MLTransform for preparing data for ML
  training and inference.
  """

  train_data = [
      dict(x=["Let's", "go", "to", "the", "park"]),
      dict(x=["I", "enjoy", "going", "to", "the", "park"]),
      dict(x=["I", "enjoy", "reading", "books"]),
      dict(x=["Beam", "can", "be", "fun"]),
      dict(x=["The", "weather", "is", "really", "nice", "today"]),
      dict(x=["I", "love", "to", "go", "to", "the", "park"]),
      dict(x=["I", "love", "to", "read", "books"]),
      dict(x=["I", "love", "to", "program"]),
  ]

  test_data = [
      dict(x=['I', 'love', 'books']), dict(x=['I', 'love', 'Apache', 'Beam'])
  ]

  # Preprocess the data for ML training.
  # For the data going into the ML model training, we want to produce the
  # artifacts.
  preprocess_data_for_ml_training(train_data, args=args)

  # Do some ML model training here.

  # Preprocess the data for ML inference.
  # For the data going into the ML model inference, we want to consume the
  # artifacts produced during the stage where we preprocessed the data for ML
  # training.
  preprocess_data_for_ml_inference(test_data, args=args)

  # To fetch the artifacts produced in MLTransform, you can use
  # ArtifactsFetcher for fetching vocab related artifacts. For
  # others such as TFIDF weight, they can be accessed directly
  # from the output of MLTransform.
  artifacts_fetcher = ArtifactsFetcher(artifact_location=args.artifact_location)
  vocab_list = artifacts_fetcher.get_vocab_list()
  assert vocab_list[22] == 'Beam'


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  args, pipeline_args = parse_args()
  # for this example, create a temp artifact location if not provided.
  if args.artifact_location == '':
    args.artifact_location = tempfile.mkdtemp()
  run(args)
