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

"""Functionality for the data preprocessing step."""

import re
import json
import io
import argparse
import time
from pathlib import Path

import requests
from PIL import Image, UnidentifiedImageError
import numpy as np
import torch
import torchvision.transforms as T
import torchvision.transforms.functional as TF
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# [START preprocess_component_argparse]
def parse_args():
  """Parse preprocessing arguments."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      "--ingested-dataset-path",
      type=str,
      help="Path to the ingested dataset",
      required=True)
  parser.add_argument(
      "--preprocessed-dataset-path",
      type=str,
      help="The target directory for the ingested dataset.",
      required=True)
  parser.add_argument(
      "--base-artifact-path",
      type=str,
      help="Base path to store pipeline artifacts.",
      required=True)
  parser.add_argument(
      "--gcp-project-id",
      type=str,
      help="ID for the google cloud project to deploy the pipeline to.",
      required=True)
  parser.add_argument(
      "--region",
      type=str,
      help="Region in which to deploy the pipeline.",
      required=True)
  parser.add_argument(
      "--dataflow-staging-root",
      type=str,
      help="Path to staging directory for dataflow.",
      required=True)
  parser.add_argument(
      "--beam-runner",
      type=str,
      help="Beam runner: DataflowRunner or DirectRunner.",
      default="DirectRunner")

  return parser.parse_args()


# [END preprocess_component_argparse]


def preprocess_dataset(
    ingested_dataset_path: str,
    preprocessed_dataset_path: str,
    base_artifact_path: str,
    gcp_project_id: str,
    region: str,
    dataflow_staging_root: str,
    beam_runner: str):
  """Preprocess the ingested raw dataset and write the result to avro format.

  Args:
    ingested_dataset_path (str): Path to the ingested dataset
    preprocessed_dataset_path (str): Path to where the preprocessed dataset will be saved
    base_artifact_path (str): path to the base directory of where artifacts can be stored for
      this component.
    gcp_project_id (str): ID for the google cloud project to deploy the pipeline to.
    region (str): Region in which to deploy the pipeline.
    dataflow_staging_root (str): Path to staging directory for the dataflow runner.
    beam_runner (str): Beam runner: DataflowRunner or DirectRunner.
  """
  # [START kfp_component_input_output]
  timestamp = time.time()
  target_path = f"{base_artifact_path}/preprocessing/preprocessed_dataset_{timestamp}"

  # the directory where the output file is created may or may not exists
  # so we have to create it.
  Path(preprocessed_dataset_path).parent.mkdir(parents=True, exist_ok=True)
  with open(preprocessed_dataset_path, 'w') as f:
    f.write(target_path)
  # [END kfp_component_input_output]

  # [START deploy_preprocessing_beam_pipeline]
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(
      runner=beam_runner,
      project=gcp_project_id,
      job_name=f'preprocessing-{int(time.time())}',
      temp_location=dataflow_staging_root,
      region=region,
      requirements_file="/requirements.txt",
      save_main_session=True,
  )

  with beam.Pipeline(options=pipeline_options) as pipeline:
    (
        pipeline
        |
        "Read input jsonl file" >> beam.io.ReadFromText(ingested_dataset_path)
        | "Load json" >> beam.Map(json.loads)
        | "Filter licenses" >> beam.Filter(valid_license)
        | "Download image from URL" >> beam.ParDo(DownloadImageFromURL())
        | "Filter on valid images" >>
        beam.Filter(lambda el: el['image'] is not None)
        | "Resize image" >> beam.ParDo(ResizeImage(size=(224, 224)))
        | "Clean Text" >> beam.ParDo(CleanText())
        | "Serialize Example" >> beam.ParDo(SerializeExample())
        | "Write to Avro files" >> beam.io.WriteToAvro(
            file_path_prefix=target_path,
            schema={
                "namespace": "preprocessing.example",
                "type": "record",
                "name": "Sample",
                "fields": [{
                    "name": "id", "type": "int"
                }, {
                    "name": "caption", "type": "string"
                }, {
                    "name": "image", "type": "bytes"
                }]
            },
            file_name_suffix=".avro"))
  # [END deploy_preprocessing_beam_pipeline]


class DownloadImageFromURL(beam.DoFn):
  """DoFn to download the images from their uri."""
  def process(self, element):
    response = requests.get(element['image_url'])
    try:
      image = Image.open(io.BytesIO(response.content))
      image = T.ToTensor()(image)
      element['image'] = image
    except UnidentifiedImageError:
      element['image'] = None
    return [element]


class ResizeImage(beam.DoFn):
  "DoFn to resize the elememt's PIL image to the target resolution."
  def process(self, element, size=(256, 256)):
    element['image'] = TF.resize(element['image'], size)
    return [element]


class CleanText(beam.DoFn):
  """Dofn to perform a series of string cleaning operations."""

  def process(self, element):
    text = element['caption']
    text = text.lower()  # lower case
    text = re.sub(r"http\S+", "", text)  # remove urls
    text = re.sub("\s+", " ", text)  # remove extra spaces (including \n and \t)
    text = re.sub(
        "[()[\].,|:;?!=+~\-\/{}]", ",",
        text)  # all puncutation are replace w commas
    text = f" {text}"  # always start with a space
    text = text.strip(',')  #  remove commas at the start or end of the caption
    text = text[:-1] if text and text[-1] == "," else text
    text = text[1:] if text and text[0] == "," else text

    element["preprocessed_caption"] = text
    return [element]


def valid_license(element):
  """Checks whether an element's image has the correct license for our use case."""
  license = element['image_license']
  return license in ["Attribution License", "No known copyright restrictions"]


class SerializeExample(beam.DoFn):
  """DoFn to serialize an elements image."""
  def process(self, element):
    buffer = io.BytesIO()
    torch.save(element['image'], buffer)
    buffer.seek(0)
    element['image'] = buffer.read()
    return [element]


if __name__ == "__main__":
  args = parse_args()
  preprocess_dataset(**vars(args))
