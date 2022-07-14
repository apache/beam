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
import string
from datetime import datetime
from typing import List

from google.cloud import datastore
from tqdm import tqdm

from config import Config, PrecompiledExample, DatastoreProps
from helper import Example

from api.v1.api_pb2 import Sdk


# https://cloud.google.com/datastore/docs/concepts/entities
class DatastoreClient:
    """DatastoreClient is a datastore client for sending a request to the Google."""

    def __init__(self):
        self._datastore_client = datastore.Client(namespace=DatastoreProps.NAMESPACE, project=Config.GOOGLE_CLOUD_PROJECT)

    def save_to_cloud_datastore(self, examples_from_rep: List[Example]):
        """
        Save examples, output and meta to datastore

        Args:
            :param examples_from_rep: examples from the repository for saving to the Cloud Datastore
        """

        snippets = []
        examples = []
        pc_objects = []
        files = []
        now = datetime.today()
        last_schema_version_query = self._datastore_client.query(kind=DatastoreProps.SCHEMA_KIND)
        schema_iterator = last_schema_version_query.fetch()
        schema_names = []
        for schema in schema_iterator:
            schema_names.append(schema.key.name)
        actual_schema_version_key = self._get_actual_schema_version(schema_names)
        with self._datastore_client.transaction():
            for example in tqdm(examples_from_rep):
                sdk_key = self._get_key(DatastoreProps.SDK_KIND, Sdk.Name(example.sdk))
                example_id = f"{example.name.strip()}_{Sdk.Name(example.sdk)}"
                self._to_example_entities(example, example_id, sdk_key, actual_schema_version_key, examples)
                self._to_snippet_entities(example, example_id, sdk_key, now, actual_schema_version_key, snippets)
                self._to_pc_object_entities(example, example_id, pc_objects)
                self._to_file_entities(example, example_id, files)

            self._datastore_client.put_multi(examples)
            self._datastore_client.put_multi(snippets)
            self._datastore_client.put_multi(pc_objects)
            self._datastore_client.put_multi(files)

    def _get_actual_schema_version(self, schema_names: List[str]) -> datastore.Key:
        schema_names.sort()
        return self._get_key(DatastoreProps.SCHEMA_KIND, schema_names[0])

    def _get_key_name(self, key: datastore.Key):
        return key.name

    def _get_key(self, kind, identifier: str) -> datastore.Key:
        return self._datastore_client.key(kind, identifier)

    def _to_snippet_entities(self, example: Example, snp_id: string, sdk_key: datastore.Key, now: datetime, schema_key: datastore.Key, snippets: list):
        snippet_entity = datastore.Entity(self._get_key(DatastoreProps.SNIPPET_KIND, snp_id))
        snippet_entity.update(
            {
                "sdk": sdk_key,
                "pipeOpts": example.tag.pipeline_options,
                "created": now,
                "lVisited": now,
                "origin": DatastoreProps.ORIGIN_PROPERTY_VALUE,
                "numberOfFiles": 1,
                "schVer": schema_key
            }
        )
        snippets.append(snippet_entity)

    def _to_example_entities(self, example: Example, example_id: str, sdk_key: datastore.Key, schema_key: datastore.Key, examples: list):
        example_entity = datastore.Entity(self._get_key(DatastoreProps.EXAMPLE_KIND, example_id))
        example_entity.update(
            {
                "name": example.name,
                "sdk": sdk_key,
                "descr": example.tag.description,
                "cats": example.tag.categories,
                "path": example.link,
                "origin": DatastoreProps.ORIGIN_PROPERTY_VALUE,
                "schVer": schema_key
            }
        )
        examples.append(example_entity)

    def _to_pc_object_entities(self, example: Example, snp_id: string, pc_objects: list):
        if example.graph != (None, ""):
            self._append_pc_obj_entity(snp_id, example.graph, PrecompiledExample.GRAPH_EXTENSION.upper(), pc_objects)
        if example.output != (None, ""):
            self._append_pc_obj_entity(snp_id, example.output, PrecompiledExample.OUTPUT_EXTENSION.upper(), pc_objects)
        if example.logs != (None, ""):
            self._append_pc_obj_entity(snp_id, example.logs, PrecompiledExample.LOG_EXTENSION.upper(), pc_objects)

    def _append_pc_obj_entity(self, snp_id: str, content: str, pc_obj_type: str, pc_objects: list):
        pc_obj_entity = datastore.Entity(self._get_key(DatastoreProps.PRECOMPILED_OBJECT_KIND, f"{snp_id}_{pc_obj_type}"), exclude_from_indexes=('content',))
        pc_obj_entity.update({"content": content})
        pc_objects.append(pc_obj_entity)

    def _to_file_entities(self, example: Example, snp_id: str, files: list):
        file_entity = datastore.Entity(self._get_key(DatastoreProps.FILED_KIND, f"{snp_id}_{0}"), exclude_from_indexes=('content',))
        file_entity.update(
            {
                "name": example.name,
                "content": example.code,
                "cntxLine": example.tag.context_line,
                "isMain": True
            }
        )
        files.append(file_entity)
