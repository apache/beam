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

""""A pipeline that uses RunInference to perform Language Modeling with
model from Hugging Face.

This pipeline takes sentences from a custom text file, converts the last word
of the sentence into a <mask> token, and then uses the AutoModelForMaskedLM from
Hugging Face to predict the best word for the masked token given all the words
already in the sentence. The pipeline then writes the prediction to an output
file in which users can then compare against the original sentence.
"""

import argparse
import logging
from typing import Iterable
from typing import Tuple

import apache_beam as beam
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.huggingface_inference import HuggingFacePipelineModelHandler
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult
from transformers import QuestionAnsweringPipeline


class PostProcessor(beam.DoFn):
  """Processes the PredictionResult to get the predicted answer.

  Hugging Face Pipeline for Question Answering returns a dictionary
  with score, start and end index of answer and the answer.
  """
  def process(self, result: Tuple[str, PredictionResult]) -> Iterable[str]:
    text, prediction = result
    predicted_answer = prediction.inference['answer']
    yield text + ';' + predicted_answer


def preprocess(text):
  if len(text.strip()) > 0:
    question, context = text.split(';')
    yield (question, context)


def create_squad_example(text):
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

  model_handler = HuggingFacePipelineModelHandler(model=known_args.model_name, )
  if not known_args.input:
    text = (
        pipeline | 'CreateSentences' >> beam.Create([
            "What does Apache Beam do?;"
            "Apache Beam enables batch and streaming data processing."
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
