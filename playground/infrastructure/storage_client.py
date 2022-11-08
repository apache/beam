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
Module contains the client to communicate with Google Cloud Storage
"""
import logging
import os
from typing import List

from google.cloud import storage

from config import StorageProps

# Google Storage documentation link: https://cloud.google.com/storage/docs/introduction
from helper import Example


class StorageClient:
    """StorageClient is a storage client for sending a request to the Google."""
    _storage_client: storage.Client
    bucket_key = "DATASET_BUCKET_NAME"

    def __init__(self):
        self._storage_client = storage.Client()

    def set_dataset_path_for_examples(self, examples: List[Example]):
        for example in examples:
            if example.datasets:
                dataset_tag = example.datasets[0]
                file_name = f"{dataset_tag.name}.{dataset_tag.format}"
                path = self._upload_dataset(file_name)
                dataset_tag.path = path
                example.datasets[0] = dataset_tag

    def _upload_dataset(self, dataset_file_name: str) -> str:
        bucket_name = os.getenv(self.bucket_key)
        if not bucket_name:
            logging.error("Environment variable %s not found", self.bucket_key)
            raise KeyError(f"{self.bucket_key} environment variable should be specified in os")
        bucket = self._storage_client.bucket(bucket_name)
        blob = bucket.blob(f"{StorageProps.DATASET_GCS_ROOT}/{dataset_file_name}")
        dataset_path = f"{StorageProps.DATASET_REP_ROOT}/{dataset_file_name}"
        if not os.path.isfile(dataset_path):
            logging.error("File not found at the specified path: %s", dataset_path)
            raise FileNotFoundError
        blob.upload_from_filename(dataset_path)
        blob.cache_control = "no-store"
        blob.patch()
        logging.info("Dataset uploaded to %s bucket", bucket_name)
        return blob.public_url
