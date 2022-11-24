#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License

""""A pipeline that uses RunInference to perform Translation
with T5 language model.

This pipeline takes a list of english sentences and then uses
the T5ForConditionalGeneration from Hugging Face to translate the
english sentence into german.
"""
import argparse
import sys

import apache_beam as beam
from apache_beam.ml.inference.base import RunInference
from apache_beam.options.pipeline_options import PipelineOptions
from pipeline_utils.utils import ModelHandlerWrapper
from pipeline_utils.utils import ModelWrapper
from pipeline_utils.utils import Postprocess
from pipeline_utils.utils import Preprocess
from transformers import AutoConfig
from transformers import AutoTokenizer


def parse_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      "--model_state_dict_path",
      required=True,
      help="Path to the model's state_dict.",
  )
  parser.add_argument(
      "--model_name",
      required=True,
      help="Path to the model's state_dict.",
      default="t5-small",
  )
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
      help="GCP project ID to run pipeline on.",
      default="apache-beam-testing",
  )
  parser.add_argument(
      "-r",
      "--region",
      help="Default region for Dataflow Runner",
      default="us-central1",
  )

  parser.add_argument(
      "--machine_type",
      help="Dataflow Worker machine type",
      default="n1-standard-4",
  )
  parser.add_argument(
      "--setup_file", help="Pipeline requirements", default="./setup.py")
  parser.add_argument(
      "--job_name", help="Dataflow Job Name", default="large-language-modeling")

  args, _ = parser.parse_known_args(args=argv)
  return args


def run():
  """
    Runs the interjector pipeline which translates english sentences
    into german using the RunInference API. """

  args = parse_args(sys.argv)
  runner = "DirectRunner" if args.mode == "local" else "DataflowRunner"
  pipeline_options_dict = vars(args)
  pipeline_options_dict.update({"runner": runner})
  pipeline_options = PipelineOptions(flags=[], **pipeline_options_dict)

  model_handler = ModelHandlerWrapper(
      state_dict_path=args.model_state_dict_path,
      model_class=ModelWrapper,
      model_params={"config": AutoConfig.from_pretrained(args.model_name)},
      device="cpu",
  )

  eng_sentences = [
      "The house is wonderful.",
      "I like to work in NYC.",
      "My name is Shubham.",
      "I want to work for Google.",
      "I am from India."
  ]
  task_prefix = "translate English to German: "
  task_sentences = [task_prefix + sentence for sentence in eng_sentences]
  tokenizer = AutoTokenizer.from_pretrained(args.model_name)

  # [START Pipeline]
  with beam.Pipeline(options=pipeline_options) as pipeline:
    _ = (
        pipeline
        | "Create Inputs" >> beam.Create(task_sentences)
        | "Preprocess" >> beam.ParDo(Preprocess(tokenizer=tokenizer))
        | "RunInference" >> RunInference(model_handler=model_handler)
        | "PostProcess" >> beam.ParDo(Postprocess(tokenizer=tokenizer)))
  # [END Pipeline]


if __name__ == "__main__":
  run()
