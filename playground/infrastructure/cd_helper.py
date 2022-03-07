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

"""
  Helper for CD step.

  It is used to save beam examples/katas/tests and their output on the GCS.
"""
import asyncio
import json
import logging
import os
import shutil
from pathlib import Path
from typing import List

from tqdm import tqdm
from google.cloud import storage

from api.v1.api_pb2 import Sdk, SDK_PYTHON, SDK_JAVA, PrecompiledObjectType
from config import Config, PrecompiledExample
from grpc_client import GRPCClient
from helper import Example, get_statuses


class CDHelper:
  """
  Helper for CD step.

  It is used to save beam examples/katas/tests and their output on the GCS.
  """
  def store_examples(self, examples: List[Example]):
    """
    Store beam examples and their output in the Google Cloud.

    Outputs for multifile examples are left empty.
    """
    single_file_examples = list(filter(
      lambda example: example.tag.multifile is False, examples))
    logging.info("Start of executing only single-file Playground examples ...")
    asyncio.run(self._get_outputs(single_file_examples))
    logging.info("Finish of executing single-file Playground examples")

    logging.info("Start of sending Playground examples to the bucket ...")
    self._save_to_cloud_storage(examples)
    logging.info("Finish of sending Playground examples to the bucket")
    self._clear_temp_folder()

  async def _get_outputs(self, examples: List[Example]):
    """
    Run beam examples and keep their output.

    Call the backend to start code processing for the examples.
    Then receive code output.

    Args:
        examples: beam examples that should be run
    """
    await get_statuses(
        examples)  # run examples code and wait until all are executed
    client = GRPCClient()
    tasks = [client.get_run_output(example.pipeline_id) for example in examples]
    outputs = await asyncio.gather(*tasks)

    tasks = [client.get_log(example.pipeline_id) for example in examples]
    logs = await asyncio.gather(*tasks)

    if len(examples) > 0 and (examples[0].sdk is SDK_PYTHON or
                              examples[0].sdk is SDK_JAVA):
      tasks = [
          client.get_graph(example.pipeline_id, example.filepath)
          for example in examples
      ]
      graphs = await asyncio.gather(*tasks)

      for graph, example in zip(graphs, examples):
        example.graph = graph

    for output, example in zip(outputs, examples):
      example.output = output

    for log, example in zip(logs, examples):
      example.logs = log

  def _save_to_cloud_storage(self, examples: List[Example]):
    """
    Save examples, outputs and meta to bucket

    Args:
        examples: precompiled examples
    """
    self._storage_client = storage.Client()
    self._bucket = self._storage_client.bucket(Config.BUCKET_NAME)

    for example in tqdm(examples):
      file_names = self._write_to_local_fs(example)

      if example.tag.default_example:
        default_example_path = str(Path([*file_names].pop()).parent)
        cloud_path = self._write_default_example_path_to_local_fs(
          default_example_path)

        self._upload_blob(
          source_file=os.path.join(Config.TEMP_FOLDER, cloud_path),
          destination_blob_name=cloud_path)

      for cloud_file_name, local_file_name in file_names.items():
        self._upload_blob(
            source_file=local_file_name, destination_blob_name=cloud_file_name)

  def _write_default_example_path_to_local_fs(self, path: str) -> str:
    """
    Write default example path to the file (in temp folder)

    Args:
        path: path of the default example

    Returns: name of the file

    """
    sdk = Path(path).parts[0]
    cloud_path = os.path.join(sdk, Config.DEFAULT_PRECOMPILED_OBJECT)

    path_to_file = os.path.join(Config.TEMP_FOLDER, sdk)
    Path(path_to_file).mkdir(parents=True, exist_ok=True)

    local_path = os.path.join(path_to_file, Config.DEFAULT_PRECOMPILED_OBJECT)

    content = json.dumps({sdk: path})
    with open(local_path, "w", encoding="utf-8") as file:
      file.write(content)

    return cloud_path

  def _write_to_local_fs(self, example: Example):
    """
    Write code of an example, output and meta info
    to the filesystem (in temp folder)

    Args:
        example: example object

    Returns: dict {path_at_the_bucket:path_at_the_os}

    """
    path_to_object_folder = os.path.join(
        Config.TEMP_FOLDER,
        example.pipeline_id,
        Sdk.Name(example.sdk),
        PrecompiledObjectType.Name(example.type),
        example.tag.name)
    Path(path_to_object_folder).mkdir(parents=True, exist_ok=True)

    file_names = {}
    code_path = self._get_gcs_object_name(
        sdk=example.sdk,
        type=example.type,
        base_folder_name=example.tag.name,
        file_name=example.tag.name)
    output_path = self._get_gcs_object_name(
        sdk=example.sdk,
        type=example.type,
        base_folder_name=example.tag.name,
        file_name=example.tag.name,
        extension=PrecompiledExample.OUTPUT_EXTENSION)
    log_path = self._get_gcs_object_name(
        sdk=example.sdk,
        type=example.type,
        base_folder_name=example.tag.name,
        file_name=example.tag.name,
        extension=PrecompiledExample.LOG_EXTENSION)
    graph_path = self._get_gcs_object_name(
        sdk=example.sdk,
        type=example.type,
        base_folder_name=example.tag.name,
        file_name=example.tag.name,
        extension=PrecompiledExample.GRAPH_EXTENSION)
    meta_path = self._get_gcs_object_name(
        sdk=example.sdk,
        type=example.type,
        base_folder_name=example.tag.name,
        file_name=PrecompiledExample.META_NAME,
        extension=PrecompiledExample.META_EXTENSION)
    file_names[code_path] = example.code
    file_names[output_path] = example.output
    meta = example.tag._asdict()
    meta["link"] = example.link
    file_names[meta_path] = json.dumps(meta)
    file_names[log_path] = example.logs
    if example.sdk == SDK_PYTHON or example.sdk == SDK_JAVA:
      file_names[graph_path] = example.graph
    for file_name, file_content in file_names.items():
      local_file_path = os.path.join(
          Config.TEMP_FOLDER, example.pipeline_id, file_name)
      with open(local_file_path, "w", encoding="utf-8") as file:
        file.write(file_content)
      # don't need content anymore, instead save the local path
      file_names[file_name] = local_file_path
    return file_names

  def _get_gcs_object_name(
      self,
      sdk: Sdk,
      type: PrecompiledObjectType,
      base_folder_name: str,
      file_name: str,
      extension: str = None):
    """
    Get the path where file will be stored at the bucket.

    Args:
      sdk: sdk of the example
      type: type of the example
      file_name: name of the example
      base_folder_name: name of the folder where example is stored
        (eq. to example name)
      extension: extension of the file

    Returns: file name
    """
    if extension is None:
      extension = Config.SDK_TO_EXTENSION[sdk]
    return os.path.join(
        Sdk.Name(sdk),
        PrecompiledObjectType.Name(type),
        base_folder_name,
        f"{file_name}.{extension}")

  def _upload_blob(self, source_file: str, destination_blob_name: str):
    """
    Upload a file to the bucket.

    Args:
        source_file: name of the file to be stored
        destination_blob_name: "storage-object-name"
    """

    blob = self._bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file)
    # change caching to no caching
    blob.cache_control = Config.NO_STORE
    blob.patch()
    logging.info("File uploaded to %s", destination_blob_name)

  def _clear_temp_folder(self):
    """
    Remove temporary folder with source files.
    """
    if os.path.exists(Config.TEMP_FOLDER):
      shutil.rmtree(Config.TEMP_FOLDER)
