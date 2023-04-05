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

""""A pipeline that uses RunInference to perform Language Modeling with Bert.

This pipeline takes sentences from a custom text file, converts the last word
of the sentence into a [MASK] token, and then uses the BertForMaskedLM from
Hugging Face to predict the best word for the masked token given all the words
already in the sentence. The pipeline then writes the prediction to an output
file in which users can then compare against the original sentence.
"""

import argparse
import logging
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import Tuple

import apache_beam as beam
import torch
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerKeyedTensor
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult
from transformers import BertConfig
from transformers import BertForMaskedLM
from transformers import BertTokenizer


def add_mask_to_last_word(text: str) -> Tuple[str, str]:
  text_list = text.split()
  return text, ' '.join(text_list[:-2] + ['[MASK]', text_list[-1]])


def tokenize_sentence(
    text_and_mask: Tuple[str, str],
    bert_tokenizer: BertTokenizer) -> Tuple[str, Dict[str, torch.Tensor]]:
  text, masked_text = text_and_mask
  tokenized_sentence = bert_tokenizer.encode_plus(
      masked_text, return_tensors="pt")

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

  The logits are the output of the BERT Model. After applying a softmax
  activation function to the logits, we get probabilistic distributions for each
  of the words in BERT’s vocabulary. We can get the word with the highest
  probability of being a candidate replacement word by taking the argmax.
  """
  def __init__(self, bert_tokenizer: BertTokenizer):
    super().__init__()
    self.bert_tokenizer = bert_tokenizer

  def process(self, element: Tuple[str, PredictionResult]) -> Iterable[str]:
    text, prediction_result = element
    inputs = prediction_result.example
    logits = prediction_result.inference['logits']
    mask_token_index = (
        inputs['input_ids'] == self.bert_tokenizer.mask_token_id).nonzero(
            as_tuple=True)[0]
    predicted_token_id = logits[mask_token_index].argmax(axis=-1)
    decoded_word = self.bert_tokenizer.decode(predicted_token_id)
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
      '--bert_tokenizer',
      dest='bert_tokenizer',
      default='bert-base-uncased',
      help='bert uncased model. This can be base model or large model')
  parser.add_argument(
      '--model_state_dict_path',
      dest='model_state_dict_path',
      required=True,
      help="Path to the model's state_dict.")
  return parser.parse_known_args(argv)


def run(
    argv=None,
    model_class=None,
    model_params=None,
    save_main_session=True,
    test_pipeline=None) -> PipelineResult:
  """
  Args:
    argv: Command line arguments defined for this example.
    model_class: Reference to the class definition of the model.
                If None, BertForMaskedLM will be used as default .
    model_params: Parameters passed to the constructor of the model_class.
                  These will be used to instantiate the model object in the
                  RunInference API.
    save_main_session: Used for internal testing.
    test_pipeline: Used for internal testing.
  """
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  if not model_class:
    model_config = BertConfig.from_pretrained(
        known_args.bert_tokenizer, is_decoder=False, return_dict=True)
    model_class = BertForMaskedLM
    model_params = {'config': model_config}

  # TODO: Remove once nested tensors https://github.com/pytorch/nestedtensor
  # is officially released.
  class PytorchNoBatchModelHandler(PytorchModelHandlerKeyedTensor):
    """Wrapper to PytorchModelHandler to limit batch size to 1.

    The tokenized strings generated from BertTokenizer may have different
    lengths, which doesn't work with torch.stack() in current RunInference
    implementation since stack() requires tensors to be the same size.

    Restricting max_batch_size to 1 means there is only 1 example per `batch`
    in the run_inference() call.
    """
    def batch_elements_kwargs(self):
      return {'max_batch_size': 1}

  model_handler = PytorchNoBatchModelHandler(
      state_dict_path=known_args.model_state_dict_path,
      model_class=model_class,
      model_params=model_params)

  pipeline = test_pipeline
  if not test_pipeline:
    pipeline = beam.Pipeline(options=pipeline_options)

  bert_tokenizer = BertTokenizer.from_pretrained(known_args.bert_tokenizer)

  if not known_args.input:
    text = (pipeline | 'CreateSentences' >> beam.Create([
      'The capital of France is Paris .',
      'It is raining cats and dogs .',
      'He looked up and saw the sun and stars .',
      'Today is Monday and tomorrow is Tuesday .',
      'There are 5 coconuts on this palm tree .',
      'The richest person in the world is not here .',
      'Malls are amazing places to shop because you can find everything you need under one roof .', # pylint: disable=line-too-long
      'This audiobook is sure to liquefy your brain .',
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
      | 'TokenizeSentence' >>
      beam.Map(lambda x: tokenize_sentence(x, bert_tokenizer)))
  output = (
      text_and_tokenized_text_tuple
      | 'PyTorchRunInference' >> RunInference(KeyedModelHandler(model_handler))
      | 'ProcessOutput' >> beam.ParDo(
          PostProcessor(bert_tokenizer=bert_tokenizer)))
  output | "WriteOutput" >> beam.io.WriteToText( # pylint: disable=expression-not-assigned
    known_args.output,
    shard_name_template='',
    append_trailing_newlines=True)

  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
