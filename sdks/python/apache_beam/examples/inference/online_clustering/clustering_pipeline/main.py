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

"""This file contains the pipeline for doing online clustering."""
import argparse
import sys

import apache_beam as beam
import config as cfg
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerKeyedTensor
from pipeline.options import get_pipeline_options
from pipeline.transformations import Decode
from pipeline.transformations import GetUpdates
from pipeline.transformations import ModelWrapper
from pipeline.transformations import NormalizeEmbedding
from pipeline.transformations import StatefulOnlineClustering
from pipeline.transformations import tokenize_sentence
from transformers import AutoConfig


def parse_arguments(argv):
  """
    It parses the arguments passed to the command line and returns them as an object

    Args:
      argv: The arguments passed to the command line.

    Returns:
      The arguments that are being passed in.
    """
  parser = argparse.ArgumentParser(description="online-clustering")

  parser.add_argument(
      "-m",
      "--mode",
      help="Mode to run pipeline in.",
      choices=["local", "cloud"],
      default="local",
  )
  parser.add_argument(
      "-p",
      "--project",
      help="GCP project to run pipeline on.",
      default=cfg.PROJECT_ID,
  )

  args, _ = parser.parse_known_args(args=argv)
  return args


# Can be removed once: https://github.com/apache/beam/issues/21863 is fixed
class PytorchNoBatchModelHandler(PytorchModelHandlerKeyedTensor):
  """Wrapper to PytorchModelHandler to limit batch size to 1.
    The tokenized strings generated from BertTokenizer may have different
    lengths, which doesn't work with torch.stack() in current RunInference
    implementation since stack() requires tensors to be the same size.
    Restricting max_batch_size to 1 means there is only 1 example per `batch`
    in the run_inference() call.
    """
  def batch_elements_kwargs(self):
    return {"max_batch_size": 1}


def run():
  """
    It runs the pipeline.  It read from PubSub, decode the message,
    tokenize the text, get the embedding, normalize the embedding,
    map the document to a key, and then perform stateful clustering using Birch
    """
  args = parse_arguments(sys.argv)
  pipeline_options = get_pipeline_options(
      job_name=cfg.JOB_NAME,
      num_workers=cfg.NUM_WORKERS,
      project=args.project,
      mode=args.mode,
  )

  model_handler = PytorchNoBatchModelHandler(
      state_dict_path=cfg.MODEL_STATE_DICT_PATH,
      model_class=ModelWrapper,
      model_params={
          "config": AutoConfig.from_pretrained(cfg.MODEL_CONFIG_PATH)
      },
      device="cpu",
  )

  with beam.Pipeline(options=pipeline_options) as pipeline:
    docs = (
        pipeline | "Read from PubSub" >> ReadFromPubSub(
            subscription=cfg.SUBSCRIPTION_ID, with_attributes=True)
        | "Decode PubSubMessage" >> beam.ParDo(Decode()))
    normalized_embedding = (
        docs | "Tokenize Text" >> beam.Map(tokenize_sentence)
        | "Get Embedding" >> RunInference(KeyedModelHandler(model_handler))
        | "Normalize Embedding" >> beam.ParDo(NormalizeEmbedding()))
    clustering = (
        normalized_embedding | "Map doc to key" >> beam.Map(lambda x: (1, x))
        | "StatefulClustering using Birch" >> beam.ParDo(
            StatefulOnlineClustering()))

    _ = clustering | "Format Update" >> beam.ParDo(GetUpdates())


if __name__ == "__main__":
  run()
