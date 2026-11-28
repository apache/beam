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

"""A pipeline that uses RunInference to perform Question Answering using a
model from Hugging Face Models Hub.

This pipeline takes questions and context from a custom text file separated by
a semicolon, tokenizes each `(question, context)` pair with `AutoTokenizer`,
and runs `AutoModelForQuestionAnswering` via
`HuggingFaceModelHandlerKeyedTensor`. The pipeline then writes the predicted
answer span to an output file in which users can compare against the original
context.
"""

import argparse
import logging
from collections.abc import Iterable

import torch
from transformers import AutoModelForQuestionAnswering
from transformers import AutoTokenizer

import apache_beam as beam
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.huggingface_inference import HuggingFaceModelHandlerKeyedTensor
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult

_MAX_ANSWER_TOKENS = 15


class PostProcessor(beam.DoFn):
  """Processes the PredictionResult to decode the highest-scoring answer span."""
  def __init__(self, tokenizer: AutoTokenizer):
    super().__init__()
    self.tokenizer = tokenizer

  def process(
      self, result: tuple[tuple[str, int, int],
                          PredictionResult]) -> Iterable[str]:
    (question, context_start, context_end), prediction = result
    input_ids = prediction.example['input_ids']
    if context_end < context_start:
      yield question + ';'
      return

    start_logits = prediction.inference['start_logits'][
        context_start:context_end + 1]
    end_logits = prediction.inference['end_logits'][context_start:context_end +
                                                    1]
    span_scores = start_logits.unsqueeze(1) + end_logits.unsqueeze(0)
    valid_mask = torch.triu(
        torch.ones_like(span_scores, dtype=torch.bool),
        diagonal=0) & torch.tril(
            torch.ones_like(span_scores, dtype=torch.bool),
            diagonal=_MAX_ANSWER_TOKENS - 1)
    span_scores = span_scores.masked_fill(~valid_mask, float('-inf'))
    best_flat_idx = int(span_scores.argmax().item())
    context_len = context_end - context_start + 1
    rel_start, rel_end = divmod(best_flat_idx, context_len)
    answer_ids = input_ids[context_start + rel_start:context_start + rel_end +
                           1]
    predicted_answer = self.tokenizer.decode(
        answer_ids, skip_special_tokens=True).strip()
    yield question + ';' + predicted_answer


def preprocess(text):
  """
  preprocess separates the text into question and context
  by splitting on semi-colon.

  Args:
      text (str): string with question and context separated by semi-colon.

  Yields:
      (str, str): yields question and context from text.
  """
  if len(text.strip()) > 0:
    question, context = text.split(';')
    yield (question, context)


def tokenize_question_context(
    text: tuple[str, str], tokenizer: AutoTokenizer
) -> tuple[tuple[str, int, int], dict[str, torch.Tensor]]:
  """Tokenizes a `(question, context)` pair and records context token bounds."""
  question, context = text
  encoded = tokenizer(
      question, context, truncation='only_second', return_tensors='pt')
  sequence_ids = encoded.sequence_ids(0)
  context_indices = [idx for idx, sid in enumerate(sequence_ids) if sid == 1]
  if context_indices:
    context_start, context_end = context_indices[0], context_indices[-1]
  else:
    context_start, context_end = 0, -1
  tensors = {k: torch.squeeze(v, dim=0) for k, v in dict(encoded).items()}
  return (question, context_start, context_end), tensors


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      help='Path of file containing question and context separated by semicolon'
  )
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Path of file in which to save the output predictions.')
  parser.add_argument(
      '--model_name',
      dest='model_name',
      default="deepset/roberta-base-squad2",
      help='Model repository-id from Hugging Face Models Hub.')
  parser.add_argument(
      '--revision',
      dest='revision',
      help=
      'Specific model version to use - branch name, tag name, or a commit-id.')
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

  load_args = {'revision': known_args.revision} if known_args.revision else {}
  tokenizer = AutoTokenizer.from_pretrained(known_args.model_name, **load_args)
  model_handler = HuggingFaceModelHandlerKeyedTensor(
      model_uri=known_args.model_name,
      model_class=AutoModelForQuestionAnswering,
      framework='pt',
      load_model_args=load_args or None,
      max_batch_size=1)
  if not known_args.input:
    text = (
        pipeline | 'CreateSentences' >> beam.Create([
            "What does Apache Beam do?;"
            "Apache Beam enables batch and streaming data processing.",
            "What is the capital of France?;The capital of France is Paris .",
            "Where was beam summit?;Apache Beam Summit 2023 was in NYC.",
        ]))
  else:
    text = (
        pipeline | 'ReadSentences' >> beam.io.ReadFromText(known_args.input))
  processed_text = (
      text
      | 'PreProcess' >> beam.ParDo(preprocess)
      | 'TokenizeQuestionContext' >>
      beam.Map(lambda x: tokenize_question_context(x, tokenizer)))
  output = (
      processed_text
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
