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

""""A pipeline that uses RunInference to perform Question Answering using the
model from Hugging Face Models Hub.

This pipeline takes questions and context from a custom text file separated by
a semicolon. These are converted to SquadExamples by using the utility provided
by transformers.QuestionAnsweringPipeline and passed to the model handler.
We just provide the model name here because the model repository specifies the
task that it will do. The pipeline then writes the prediction to an output
file in which users can then compare against the original context.
"""

import argparse
import logging
from collections.abc import Iterable

import apache_beam as beam
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.huggingface_inference import HuggingFacePipelineModelHandler
from apache_beam.ml.inference.huggingface_inference import PipelineTask
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult
from transformers import QuestionAnsweringPipeline


class PostProcessor(beam.DoFn):
  """Processes the PredictionResult to get the predicted answer.

  Hugging Face Pipeline for Question Answering returns a dictionary
  with score, start and end index of answer and the answer.
  """
  def process(self, result: tuple[str, PredictionResult]) -> Iterable[str]:
    text, prediction = result
    predicted_answer = prediction.inference['answer']
    yield text + ';' + predicted_answer


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


def create_squad_example(text):
  """Creates SquadExample objects to be fed to QuestionAnsweringPipeline
  supported by Hugging Face.

  Check out https://huggingface.co/docs/transformers/main_classes/pipelines#transformers.QuestionAnsweringPipeline.__call__.X #pylint: disable=line-too-long
  to learn about valid input types for QuestionAnswering Pipeline.
  Args:
      text (Tuple[str,str]): a tuple of question and context.
  """
  question, context = text
  yield question, QuestionAnsweringPipeline.create_sample(question, context)


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

  model_handler = HuggingFacePipelineModelHandler(
      task=PipelineTask.QuestionAnswering,
      model=known_args.model_name,
      load_model_args={
          'framework': 'pt', 'revision': known_args.revision
      })
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
      | 'SquadExample' >> beam.ParDo(create_squad_example))
  output = (
      processed_text
      | 'RunInference' >> RunInference(KeyedModelHandler(model_handler))
      | 'ProcessOutput' >> beam.ParDo(PostProcessor()))
  _ = output | "WriteOutput" >> beam.io.WriteToText(
      known_args.output, shard_name_template='', append_trailing_newlines=True)

  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
