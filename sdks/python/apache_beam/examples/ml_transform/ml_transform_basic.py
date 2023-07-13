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
from apache_beam.ml.transforms.base import ArtifactMode
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.ml.transforms.tft import TFIDF
from apache_beam.ml.transforms.tft import ComputeAndApplyVocabulary
from apache_beam.ml.transforms.utils import ArtifactsFetcher


def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--artifact_location', type=str, default='')
  return parser.parse_known_args()


def run(args):
  data = [
      dict(x=["Let's", "go", "to", "the", "park"]),
      dict(x=["I", "enjoy", "going", "to", "the", "park"]),
      dict(x=["I", "enjoy", "reading", "books"]),
      dict(x=["Beam", "can", "be", "fun"]),
      dict(x=["The", "weather", "is", "really", "nice", "today"]),
      dict(x=["I", "love", "to", "go", "to", "the", "park"]),
      dict(x=["I", "love", "to", "read", "books"]),
      dict(x=["I", "love", "to", "program"]),
  ]

  with beam.Pipeline() as p:
    input_data = p | beam.Create(data)

    # arfifacts produce mode.
    input_data |= (
        'MLTransform' >> MLTransform(
            artifact_location=args.artifact_location,
            artifact_mode=ArtifactMode.PRODUCE,
        ).with_transform(ComputeAndApplyVocabulary(
            columns=['x'])).with_transform(TFIDF(columns=['x'])))

    # _ =  input_data | beam.Map(logging.info)

  with beam.Pipeline() as p:
    input_data = [
        dict(x=['I', 'love', 'books']), dict(x=['I', 'love', 'Apache', 'Beam'])
    ]
    input_data = p | beam.Create(input_data)

    # artifacts consume mode.
    input_data |= (
        MLTransform(
            artifact_location=args.artifact_location,
            artifact_mode=ArtifactMode.CONSUME,
            # you don't need to specify transforms as they are already saved in
            # in the artifacts.
        ))

    _ = input_data | beam.Map(logging.info)

  # To fetch the artifacts after the pipeline is run
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
