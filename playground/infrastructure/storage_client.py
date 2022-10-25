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

from google.cloud import storage

from config import StorageProps


# Google Storage documentation link: https://cloud.google.com/storage/docs/introduction
class StorageClient:
    """StorageClient is a storage client for sending a request to the Google."""
    _storage_client: storage.Client

    def __init__(self):
        self._storage_client = storage.Client()

    def upload_dataset(self, dataset_file_name: str) -> str:
        bucket_name = os.getenv("DATASET_BUCKET_NAME")
        if not bucket_name:
            raise KeyError("DATASET_BUCKET_NAME environment variable should be specified in os")
        bucket = self._storage_client.bucket(bucket_name)
        blob = bucket.blob(f"{StorageProps.DATASET_GCS_ROOT}/{dataset_file_name}")
        blob.upload_from_filename(f"{StorageProps.DATASET_REP_ROOT}/{dataset_file_name}")
        logging.info("Dataset uploaded to %s bucket", bucket_name)
        return blob.public_url
