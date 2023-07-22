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

"""A pipeline to demonstrate usage of TensorRT with RunInference
for a text classification model. This pipeline reads data from a text
file, preprocesses the data, and then uses RunInference to generate
predictions from the text classification TensorRT engine. Next,
it postprocesses the RunInference outputs to print the input and
the predicted class label.
It also prints metrics provided by RunInference.
"""

import argparse
import logging

import numpy as np

import apache_beam as beam
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.tensorrt_inference import TensorRTEngineHandlerNumPy
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from transformers import AutoTokenizer


class Preprocess(beam.DoFn):
  """Processes the input sentences to tokenize them.

  The input sentences are tokenized because the
  model is expecting tokens.
  """
  def __init__(self, tokenizer: AutoTokenizer):
    self._tokenizer = tokenizer

  def process(self, element):
    inputs = self._tokenizer(
        element, return_tensors="np", padding="max_length", max_length=128)
    return inputs.input_ids


class Postprocess(beam.DoFn):
  """Processes the PredictionResult to get the predicted class.

  The logits are the output of the TensorRT engine.
  We can get the class label by getting the index of
  maximum logit using argmax.
  """
  def __init__(self, tokenizer: AutoTokenizer):
    self._tokenizer = tokenizer

  def process(self, element):
    decoded_input = self._tokenizer.decode(
        element.example, skip_special_tokens=True)
    logits = element.inference[0]
    argmax = np.argmax(logits)
    output = "Positive" if argmax == 1 else "Negative"
    yield decoded_input, output


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='Path to the text file containing sentences.')
  parser.add_argument(
      '--trt_model_path',
      dest='trt_model_path',
      required=True,
      help='Path to the pre-built textattack/bert-base-uncased-SST-2'
      'TensorRT engine.')
  parser.add_argument(
      '--model_id',
      dest='model_id',
      default="textattack/bert-base-uncased-SST-2",
      help="name of model.")
  return parser.parse_known_args(argv)


def run(
    argv=None,
    save_main_session=True,
):
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  model_handler = TensorRTEngineHandlerNumPy(
      min_batch_size=1,
      max_batch_size=1,
      engine_path=known_args.trt_model_path,
  )

  tokenizer = AutoTokenizer.from_pretrained(known_args.model_id)

  with beam.Pipeline(options=pipeline_options) as pipeline:
    _ = (
        pipeline
        | "ReadSentences" >> beam.io.ReadFromText(known_args.input)
        | "Preprocess" >> beam.ParDo(Preprocess(tokenizer=tokenizer))
        | "RunInference" >> RunInference(model_handler=model_handler)
        | "PostProcess" >> beam.ParDo(Postprocess(tokenizer=tokenizer))
        | "LogResult" >> beam.Map(logging.info))
  metrics = pipeline.result.metrics().query(beam.metrics.MetricsFilter())
  logging.info(metrics)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
