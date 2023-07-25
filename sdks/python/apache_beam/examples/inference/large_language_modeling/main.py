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

""""A pipeline that uses RunInference to perform translation
with a T5 language model.

This pipeline takes a list of english sentences and then uses
the T5ForConditionalGeneration from Hugging Face to translate the
english sentence into german.
"""
import argparse
import sys

import apache_beam as beam
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from apache_beam.ml.inference.pytorch_inference import make_tensor_model_fn
from apache_beam.options.pipeline_options import PipelineOptions
from transformers import AutoConfig
from transformers import AutoTokenizer
from transformers import T5ForConditionalGeneration


class Preprocess(beam.DoFn):
  def __init__(self, tokenizer: AutoTokenizer):
    self._tokenizer = tokenizer

  def process(self, element):
    """
        Process the raw text input to a format suitable for
        T5ForConditionalGeneration model inference

        Args:
          element: A string of text

        Returns:
          A tokenized example that can be read by the
          T5ForConditionalGeneration
        """
    input_ids = self._tokenizer(
        element, return_tensors="pt", padding="max_length",
        max_length=512).input_ids
    return input_ids


class Postprocess(beam.DoFn):
  def __init__(self, tokenizer: AutoTokenizer):
    self._tokenizer = tokenizer

  def process(self, element):
    """
        Process the PredictionResult to print the translated texts

        Args:
          element: The RunInference output to be processed.
        """
    decoded_inputs = self._tokenizer.decode(
        element.example, skip_special_tokens=True)
    decoded_outputs = self._tokenizer.decode(
        element.inference, skip_special_tokens=True)
    print(f"{decoded_inputs} \t Output: {decoded_outputs}")


def parse_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      "--model_state_dict_path",
      dest="model_state_dict_path",
      required=True,
      help="Path to the model's state_dict.",
  )
  parser.add_argument(
      "--model_name",
      dest="model_name",
      required=False,
      help="Path to the model's state_dict.",
      default="t5-11b",
  )

  return parser.parse_known_args(args=argv)


def run():
  """
    Runs the interjector pipeline which translates English sentences
    into German using the RunInference API. """

  known_args, pipeline_args = parse_args(sys.argv)
  pipeline_options = PipelineOptions(pipeline_args)

  gen_fn = make_tensor_model_fn('generate')
  model_handler = PytorchModelHandlerTensor(
      state_dict_path=known_args.model_state_dict_path,
      model_class=T5ForConditionalGeneration,
      model_params={
          "config": AutoConfig.from_pretrained(known_args.model_name)
      },
      device="cpu",
      inference_fn=gen_fn)

  eng_sentences = [
      "The house is wonderful.",
      "I like to work in NYC.",
      "My name is Shubham.",
      "I want to work for Google.",
      "I am from India."
  ]
  task_prefix = "translate English to German: "
  task_sentences = [task_prefix + sentence for sentence in eng_sentences]
  tokenizer = AutoTokenizer.from_pretrained(known_args.model_name)

  # [START Pipeline]
  with beam.Pipeline(options=pipeline_options) as pipeline:
    _ = (
        pipeline
        | "CreateInputs" >> beam.Create(task_sentences)
        | "Preprocess" >> beam.ParDo(Preprocess(tokenizer=tokenizer))
        | "RunInference" >> RunInference(model_handler=model_handler)
        | "PostProcess" >> beam.ParDo(Postprocess(tokenizer=tokenizer)))
  # [END Pipeline]


if __name__ == "__main__":
  run()
