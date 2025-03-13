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

"""This file contains the pipeline for loading a ML model, and exploring
the different RunInference metrics."""
import argparse
import logging
import sys

import apache_beam as beam
import config as cfg
from apache_beam.ml.inference import RunInference
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerKeyedTensor
from pipeline.options import get_pipeline_options
from pipeline.transformations import CustomPytorchModelHandlerKeyedTensor
from pipeline.transformations import HuggingFaceStripBatchingWrapper
from pipeline.transformations import PostProcessor
from pipeline.transformations import Tokenize
from transformers import DistilBertConfig


def parse_arguments(argv):
  """
    Parses the arguments passed to the command line and
    returns them as an object
    Args:
      argv: The arguments passed to the command line.
    Returns:
      The arguments that are being passed in.
    """
  parser = argparse.ArgumentParser(description="benchmark-runinference")

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
  parser.add_argument(
      "-d",
      "--device",
      help="Device to run the dataflow job on",
      choices=["CPU", "GPU"],
      default="CPU",
  )

  args, _ = parser.parse_known_args(args=argv)
  return args


def run():
  """
    Runs the pipeline that loads a transformer based text classification model
    and does inference on a list of sentences.
    At the end of pipeline, different metrics like latency,
    throughput and others are printed.
    """
  args = parse_arguments(sys.argv)

  inputs = [
      "This is the worst food I have ever eaten",
      "In my soul and in my heart, I’m convinced I’m wrong!",
      "Be with me always—take any form—drive me mad!"\
      "only do not leave me in this abyss, where I cannot find you!",
      "Do I want to live? Would you like to live with your soul in the grave?",
      "Honest people don’t hide their deeds.",
      "Nelly, I am Heathcliff!  He’s always,"\
      "always in my mind: not as a pleasure,"\
      "any more than I am always a pleasure to myself, but as my own being.",
  ] * 1000

  pipeline_options = get_pipeline_options(
      job_name=cfg.JOB_NAME,
      num_workers=cfg.NUM_WORKERS,
      project=args.project,
      mode=args.mode,
      device=args.device,
  )
  model_handler_class = (
      PytorchModelHandlerKeyedTensor
      if args.device == "GPU" else CustomPytorchModelHandlerKeyedTensor)
  device = "cuda:0" if args.device == "GPU" else args.device
  model_handler = model_handler_class(
      state_dict_path=cfg.MODEL_STATE_DICT_PATH,
      model_class=HuggingFaceStripBatchingWrapper,
      model_params={
          "config": DistilBertConfig.from_pretrained(cfg.MODEL_CONFIG_PATH)
      },
      device=device,
  )

  with beam.Pipeline(options=pipeline_options) as pipeline:
    _ = (
        pipeline
        | "Create inputs" >> beam.Create(inputs)
        | "Tokenize" >> beam.ParDo(Tokenize(cfg.TOKENIZER_NAME))
        | "Inference" >>
        RunInference(model_handler=KeyedModelHandler(model_handler))
        | "Decode Predictions" >> beam.ParDo(PostProcessor()))
  metrics = pipeline.result.metrics().query(beam.metrics.MetricsFilter())
  logging.info(metrics)


if __name__ == "__main__":
  run()
