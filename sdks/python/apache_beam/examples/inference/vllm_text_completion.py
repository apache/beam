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

""" A sample pipeline using the RunInference API to interface with an LLM using
vLLM. Takes in a set of prompts or lists of previous messages and produces
responses using a model of choice.

Requires a GPU runtime with vllm, openai, and apache-beam installed to run
correctly.
"""

import argparse
import logging
from typing import Iterable

import apache_beam as beam
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.vllm_inference import OpenAIChatMessage, VLLMCompletionsModelHandler, VLLMChatModelHandler
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult

COMPLETION_EXAMPLES = [
    "Hello, my name is",
    "The president of the United States is",
    "The capital of France is",
    "The future of AI is",
    "John cena is",
]

CHAT_EXAMPLES = [
    [
        OpenAIChatMessage(
            role='user', content='What is an example of a type of penguin?'),
        OpenAIChatMessage(
            role='system', content='An emperor penguin is a type of penguin.'),
        OpenAIChatMessage(role='user', content='Tell me about them')
    ],
    [
        OpenAIChatMessage(
            role='user', content='What colors are in the rainbow?'),
        OpenAIChatMessage(
            role='system',
            content='Red, orange, yellow, green, blue, indigo, and violet.'),
        OpenAIChatMessage(role='user', content='Do other colors ever appear?')
    ],
    [
        OpenAIChatMessage(
            role='user', content='Who is the president of the United States?')
    ],
    [
        OpenAIChatMessage(role='user', content='What state is Fargo in?'),
        OpenAIChatMessage(role='system', content='Fargo is in North Dakota.'),
        OpenAIChatMessage(role='user', content='How many people live there?'),
        OpenAIChatMessage(
            role='system',
            content='Approximately 130,000 people live in Fargo, North Dakota.'
        ),
        OpenAIChatMessage(role='user', content='What is Fargo known for?'),
    ],
    [
        OpenAIChatMessage(
            role='user', content='How many fish are in the ocean?'),
    ],
]


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--model',
      dest='model',
      type=str,
      required=False,
      default='facebook/opt-125m',
      help='LLM to use for task')
  parser.add_argument(
      '--output',
      dest='output',
      type=str,
      required=True,
      help='Path to save output predictions.')
  parser.add_argument(
      '--chat',
      dest='chat',
      type=bool,
      required=False,
      default=False,
      help='Whether to use chat model handler and examples')
  return parser.parse_known_args(argv)


class PostProcessor(beam.DoFn):
  def process(self, element: PredictionResult) -> Iterable[str]:
    yield element.example + ": " + str(element.inference)


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

  model_handler = VLLMCompletionsModelHandler(model_name=known_args.model)
  input_examples = COMPLETION_EXAMPLES

  if known_args.chat:
    model_handler = VLLMChatModelHandler(model_name=known_args.model)
    input_examples = CHAT_EXAMPLES

  pipeline = test_pipeline
  if not test_pipeline:
    pipeline = beam.Pipeline(options=pipeline_options)

  examples = pipeline | "Create examples" >> beam.Create(input_examples)
  predictions = examples | "RunInference" >> RunInference(model_handler)
  process_output = predictions | "Process Predictions" >> beam.ParDo(
      PostProcessor())
  _ = process_output | "WriteOutput" >> beam.io.WriteToText(
      known_args.output, shard_name_template='', append_trailing_newlines=True)

  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
