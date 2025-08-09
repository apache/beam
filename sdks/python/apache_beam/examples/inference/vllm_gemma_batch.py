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

from __future__ import annotations

import logging
import os
import tempfile

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.vllm_inference import VLLMCompletionsModelHandler
from apache_beam.ml.inference.vllm_inference import _VLLMModelServer
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class GemmaVLLMOptions(PipelineOptions):
  """Custom pipeline options for the Gemma vLLM batch inference job."""
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        "--input",
        dest="input_file",
        required=True,
        help="Input file gs://path containing prompts.",
    )
    parser.add_argument(
        "--output_table",
        required=True,
        help="BigQuery table to write to in the form project:dataset.table.",
    )
    parser.add_argument(
        "--model_gcs_path",
        required=True,
        help="GCS path to the directory containing model files.",
    )


class FormatOutput(beam.DoFn):
  def process(self, element):
    prompt = element.example
    comp = element.inference

    if hasattr(comp, 'choices'):
      completion = comp.choices[0].text
    # fallback to a single .text field
    elif hasattr(comp, 'text'):
      completion = comp.text
    # final fallback
    else:
      completion = str(comp)

    yield {'prompt': prompt, 'completion': completion}


class GcsVLLMCompletionsModelHandler(VLLMCompletionsModelHandler):
  def __init__(self, model_name, vllm_server_kwargs=None):
    super().__init__(model_name, vllm_server_kwargs)
    self._local_model_dir = None

  def _download_gcs_directory(self, gcs_path: str, local_path: str):
    logging.info("Downloading model from %s to %s…", gcs_path, local_path)
    matches = FileSystems.match([os.path.join(gcs_path, "**")])[0].metadata_list
    for md in matches:
      rel = os.path.relpath(md.path, gcs_path)
      dst = os.path.join(local_path, rel)
      os.makedirs(os.path.dirname(dst), exist_ok=True)
      with FileSystems.open(md.path) as src, open(dst, "wb") as dstf:
        dstf.write(src.read())
    logging.info("Download complete.")

  def load_model(self) -> _VLLMModelServer:
    uri = self._model_name
    if uri.startswith("gs://"):
      self._local_model_dir = tempfile.mkdtemp(prefix="vllm_model_")
      self._download_gcs_directory(uri, self._local_model_dir)
      logging.info("Loading vLLM from local dir %s", self._local_model_dir)
      return _VLLMModelServer(self._local_model_dir, self._vllm_server_kwargs)
    else:
      logging.info("Loading vLLM from HF hub: %s", uri)
      return super().load_model()


def run(argv=None, save_main_session=True, test_pipeline=None):
  # Build pipeline options
  opts = PipelineOptions(argv)

  gem = opts.view_as(GemmaVLLMOptions)
  opts.view_as(SetupOptions).save_main_session = save_main_session

  logging.info("Pipeline starting with model path: %s", gem.model_gcs_path)
  handler = GcsVLLMCompletionsModelHandler(
      model_name=gem.model_gcs_path,
      vllm_server_kwargs={"served-model-name": gem.model_gcs_path})

  with (test_pipeline or beam.Pipeline(options=opts)) as p:
    _ = (
        p
        | "Read" >> beam.io.ReadFromText(gem.input_file)
        | "InferBatch" >> RunInference(handler, inference_batch_size=32)
        | "FormatForBQ" >> beam.ParDo(FormatOutput())
        | "WriteToBQ" >> beam.io.WriteToBigQuery(
            gem.output_table,
            schema="prompt:STRING,completion:STRING",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
        ))
  return p.result


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
