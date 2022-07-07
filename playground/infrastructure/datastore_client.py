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
Module contains the client to communicate with Google Cloud Datastore
"""
from typing import List

from google.cloud import datastore
from tqdm import tqdm

from config import Config
from helper import Example

namespace = "Playground"
exampleKind = "pg_examples"
snippetKind = "pg_snippets"
schemaVersionKind = "pg_schema_versions"
precompiledObjectKind = "pg_pc_objects"
fileKind = "pg_files"


class DatastoreClient:
    """DatastoreClient is a datastore client for sending a request to the Google."""

    def __init__(self):
        self._datastore_client = datastore.Client(
            namespace=namespace,
            project=Config.GOOGLE_CLOUD_PROJECT
        )

    def save_to_cloud_datastore(self, examples: List[Example]):
        """
        Save examples, output and meta to datastore

        Args:
            examples: precompiled examples
        """

        with self._datastore_client.transaction():
            for example in tqdm(examples):
                exampleEntity = datastore.Entity(self._datastore_client.key(""))

            self._datastore_client.put_multi()
