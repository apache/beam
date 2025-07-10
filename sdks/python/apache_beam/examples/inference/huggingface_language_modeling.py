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

"""A pipeline that uses RunInference to perform Language Modeling with
masked language model from Hugging Face.

This pipeline takes sentences from a custom text file, converts the last word
of the sentence into a <mask> token, and then uses the AutoModelForMaskedLM from
Hugging Face to predict the best word for the masked token given all the words
already in the sentence. The pipeline then writes the prediction to an output
file in which users can then compare against the original sentence.
"""

import argparse
import logging
from collections.abc import Iterable
from collections.abc import Iterator

import apache_beam as beam
import torch
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.huggingface_inference import HuggingFaceModelHandlerKeyedTensor
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult
from transformers import AutoModelForMaskedLM
from transformers import AutoTokenizer


def add_mask_to_last_word(text: str) -> tuple[str, str]:
  text_list = text.split()
  return text, ' '.join(text_list[:-2] + ['<mask>', text_list[-1]])


def tokenize_sentence(
    text_and_mask: tuple[str, str],
    tokenizer: AutoTokenizer) -> tuple[str, dict[str, torch.Tensor]]:
  text, masked_text = text_and_mask
  tokenized_sentence = tokenizer.encode_plus(masked_text, return_tensors="pt")

  # Workaround to manually remove batch dim until we have the feature to
  # add optional batching flag.
  # TODO(https://github.com/apache/beam/issues/21863): Remove once optional
  # batching flag added
  return text, {
      k: torch.squeeze(v)
      for k, v in dict(tokenized_sentence).items()
  }


def filter_empty_lines(text: str) -> Iterator[str]:
  if len(text.strip()) > 0:
    yield text


class PostProcessor(beam.DoFn):
  """Processes the PredictionResult to get the predicted word.

  The logits are the output of the Model. We can get the word with the highest
  probability of being a candidate replacement word by taking the argmax.
  """
  def __init__(self, tokenizer: AutoTokenizer):
    super().__init__()
    self.tokenizer = tokenizer

  def process(self, element: tuple[str, PredictionResult]) -> Iterable[str]:
    text, prediction_result = element
    inputs = prediction_result.example
    logits = prediction_result.inference['logits']
    mask_token_index = torch.where(
        inputs["input_ids"] == self.tokenizer.mask_token_id)[0]
    predicted_token_id = logits[mask_token_index].argmax(axis=-1)
    decoded_word = self.tokenizer.decode(predicted_token_id)
    yield text + ';' + decoded_word


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      help='Path to the text file containing sentences.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Path of file in which to save the output predictions.')
  parser.add_argument(
      '--model_name',
      dest='model_name',
      required=True,
      help='bert uncased model. This can be base model or large model')
  parser.add_argument(
      '--model_class',
      dest='model_class',
      default=AutoModelForMaskedLM,
      help='Name of the model from Hugging Face')
  parser.add_argument(
      '--large_model',
      action='store_true',
      dest='large_model',
      default=False,
      help='Set to true if your model is large enough to run into memory '
      'pressure if you load multiple copies.')
  return parser.parse_known_args(argv)


def run(
    argv=None, save_main_session=True, test_pipeline=None) -> PipelineResult:
  """
  Args:
    argv: Command line arguments defined for this example.
    save_main_session: Used for internal testing.
    test_pipeline: Used for internal testing.
  """
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  pipeline = test_pipeline
  if not test_pipeline:
    pipeline = beam.Pipeline(options=pipeline_options)

  tokenizer = AutoTokenizer.from_pretrained(known_args.model_name)

  model_handler = HuggingFaceModelHandlerKeyedTensor(
      model_uri=known_args.model_name,
      model_class=known_args.model_class,
      framework='pt',
      max_batch_size=1,
      large_model=known_args.large_model)
  if not known_args.input:
    text = (
        pipeline | 'CreateSentences' >> beam.Create([
            'The capital of France is Paris .',
            'It is raining cats and dogs .',
            'Today is Monday and tomorrow is Tuesday .',
            'There are 5 coconuts on this palm tree .',
            'The strongest person in the world is not famous .',
            'The secret ingredient to his wonderful life was gratitude .',
            'The biggest animal in the world is the whale .',
        ]))
  else:
    text = (
        pipeline | 'ReadSentences' >> beam.io.ReadFromText(known_args.input))
  text_and_tokenized_text_tuple = (
      text
      | 'FilterEmptyLines' >> beam.ParDo(filter_empty_lines)
      | 'AddMask' >> beam.Map(add_mask_to_last_word)
      |
      'TokenizeSentence' >> beam.Map(lambda x: tokenize_sentence(x, tokenizer)))
  output = (
      text_and_tokenized_text_tuple
      | 'RunInference' >> RunInference(KeyedModelHandler(model_handler))
      | 'ProcessOutput' >> beam.ParDo(PostProcessor(tokenizer=tokenizer)))
  _ = output | "WriteOutput" >> beam.io.WriteToText(
      known_args.output, shard_name_template='', append_trailing_newlines=True)

  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
