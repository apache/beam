import argparse
import logging
import numpy as np

import apache_beam as beam

from apache_beam.ml.inference.base import RunInference
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.ml.inference.tensorrt_inference import TensorRTEngineHandlerNumPy
from transformers import AutoTokenizer


class Preprocess(beam.DoFn):

  def __init__(self, tokenizer: AutoTokenizer):
    self._tokenizer = tokenizer

  def process(self, element):
    inputs = self._tokenizer(
        element, return_tensors="np", padding="max_length", max_length=128)
    return inputs.input_ids


class Postprocess(beam.DoFn):

  def __init__(self, tokenizer: AutoTokenizer):
    self._tokenizer = tokenizer

  def process(self, element):
    decoded_input = self._tokenizer.decode(
        element.example, skip_special_tokens=True)
    logits = element.inference[0]
    argmax = np.argmax(logits)
    output = "Positive" if argmax == 1 else "Negative"
    print(f"Input: {decoded_input}, \t Sentiment: {output}")


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--trt-model-path',
      dest='trt_model_path',
      required=True,
      help='Path to the TensorRT engine.')
  parser.add_argument(
      '--model-id',
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

  task_sentences = [
      "Hello, my dog is cute",
      "I hate you",
      "Shubham Krishna is a good coder",
  ] * 4000

  tokenizer = AutoTokenizer.from_pretrained(known_args.model_id)

  with beam.Pipeline(options=pipeline_options) as pipeline:
    _ = (
        pipeline
        | "CreateInputs" >> beam.Create(task_sentences)
        | "Preprocess" >> beam.ParDo(Preprocess(tokenizer=tokenizer))
        | "RunInference" >> RunInference(model_handler=model_handler)
        | "PostProcess" >> beam.ParDo(Postprocess(tokenizer=tokenizer)))
  metrics = pipeline.result.metrics().query(beam.metrics.MetricsFilter())
  print(metrics)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
