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

__all__ = ['ArtifactsFetcher']

import os
import tempfile
import typing

from google.cloud.storage import Client
from google.cloud.storage import transfer_manager

import tensorflow_transform as tft
from apache_beam.ml.transforms import base


def download_artifacts_from_gcs(bucket_name, prefix, local_path):
  """Downloads artifacts from GCS to the local file system.
    Args:
        bucket_name: The name of the GCS bucket to download from.
        prefix: Prefix of GCS objects to download.
        local_path: The local path to download the folder to.
  """
  client = Client()
  bucket = client.get_bucket(bucket_name)
  blobs = [blob.name for blob in bucket.list_blobs(prefix=prefix)]
  _ = transfer_manager.download_many_to_path(
      bucket, blobs, destination_directory=local_path)


class ArtifactsFetcher:
  """
  Utility class used to fetch artifacts from the artifact_location passed
  to the TFTProcessHandlers in MLTransform.

  This is intended to be used for testing purposes only.
  """
  def __init__(self, artifact_location: str):
    tempdir = tempfile.mkdtemp()
    if artifact_location.startswith('gs://'):
      parts = artifact_location[5:].split('/')
      bucket_name = parts[0]
      prefix = '/'.join(parts[1:])
      download_artifacts_from_gcs(bucket_name, prefix, tempdir)

    assert os.listdir(tempdir), f"No files found in {artifact_location}"
    artifact_location = os.path.join(tempdir, prefix)
    files = os.listdir(artifact_location)
    files.remove(base._ATTRIBUTE_FILE_NAME)
    # TODO: https://github.com/apache/beam/issues/29356
    #  Integrate ArtifactFetcher into MLTransform.
    if len(files) > 1:
      raise NotImplementedError(
          "MLTransform may have been utilized alongside transforms written "
          "in TensorFlow Transform, in conjunction with those from different "
          "frameworks. Currently, retrieving artifacts from this "
          "multi-framework setup is not supported.")
    self._artifact_location = os.path.join(artifact_location, files[0])
    self.transform_output = tft.TFTransformOutput(self._artifact_location)

  def get_vocab_list(self, vocab_filename: str) -> typing.List[bytes]:
    """
    Returns list of vocabulary terms created during MLTransform.
    """
    try:
      vocab_list = self.transform_output.vocabulary_by_name(vocab_filename)
    except ValueError as e:
      raise ValueError(
          'Vocabulary file {} not found in artifact location'.format(
              vocab_filename)) from e
    return [x.decode('utf-8') for x in vocab_list]

  def get_vocab_filepath(self, vocab_filename: str) -> str:
    """
    Return the path to the vocabulary file created during MLTransform.
    """
    return self.transform_output.vocabulary_file_by_name(vocab_filename)

  def get_vocab_size(self, vocab_filename: str) -> int:
    return self.transform_output.vocabulary_size_by_name(vocab_filename)
