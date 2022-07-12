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

from config import Config, PrecompiledExample
from helper import Example

import constant
from hashlib import sha256
from base64 import urlsafe_b64encode

from property import Properties
from util import StringBuilder


# https://cloud.google.com/datastore/docs/concepts/entities
class DatastoreClient:
    """DatastoreClient is a datastore client for sending a request to the Google."""

    def __init__(self):
        self._datastore_client = datastore.Client(namespace=constant.NAMESPACE, project=Config.GOOGLE_CLOUD_PROJECT)
        self._properties = Properties()

    def save_to_cloud_datastore(self, examples_from_rep: List[Example]):
        """
        Save examples, output and meta to datastore

        Args:
            :param examples_from_rep:
        """

        snippets = []
        examples = []
        pc_objects = []
        files = []
        now = datetime.today()
        last_schema_version_query = self._datastore_client.query(kind=constant.SCHEMA_KIND)
        schema_keys = last_schema_version_query.fetch()
        actual_schema_version_key = self._get_actual_schema_version(schema_keys)
        with self._datastore_client.transaction():
            for example in tqdm(examples_from_rep):
                content = self._merge_content(example)
                snp_id = self._generate_id(self._properties.get_salt(), content, self._properties.get_id_length())
                sdk_key = self._get_key(constant.SDK_KIND, example.sdk)
                self._to_snippet_entities(snp_id, sdk_key, now, actual_schema_version_key, snippets)
                self._to_example_entities(example, snp_id, sdk_key, actual_schema_version_key, examples)
                self._to_pc_object_entities(example, snp_id, pc_objects)
                self._to_file_entities(files)

            self._datastore_client.put_multi(snippets)
            self._datastore_client.put_multi(examples)
            self._datastore_client.put_multi(pc_objects)
            self._datastore_client.put_multi(files)

    def _get_actual_schema_version(self, schema_keys: List[datastore.key]) -> datastore.key:
        schema_keys.sort(key=self._get_key_name)
        return schema_keys[0]

    def _get_key_name(self, key: datastore.key):
        return key["arg_1"]

    def _merge_content(self, example: Example) -> string:
        content = StringBuilder()
        content.add(example.code.strip())
        content.add(example.name.strip())
        content.add(example.sdk)
        content.add(example.tag.pipeline_options.strip())
        return content

    def _generate_id(self, salt, content: string, length: int) -> string:
        hash_init = sha256()
        hash_init.update(salt + content)
        return urlsafe_b64encode(hash_init.digest())[:length]

    def _get_key(self, kind, identifier: str) -> datastore.key:
        return self._datastore_client.key(kind, identifier)

    def _to_snippet_entities(self, snp_id: string, sdk_key: datastore.key, now: datetime, schema_key: datastore.key, snippets: list):
        snippet_entity = datastore.Entity(self._get_key(constant.SNIPPET_KIND, snp_id))
        snippet_entity.update(
            {
                "sdk": sdk_key,
                "created": now,
                "lVisited": now,
                "origin": constant.ORIGIN_PROPERTY_VALUE,
                "numberOfFiles": 1,
                "schVer": schema_key
            }
        )
        snippets.append(snippet_entity)

    def _to_example_entities(self, example: Example, snp_id: string, sdk_key: datastore.key, schema_key: datastore.key, examples: list):
        snp_key = self._get_key(constant.SNIPPET_KIND, snp_id)
        example_entity = datastore.Entity(self._get_key(constant.EXAMPLE_KIND, f"${example.name.strip()}_${example.sdk}"))
        example_entity.update(
            {
                "name": example.name,
                "sdk": sdk_key,
                "descr": example.tag.description,
                "cats": example.tag.categories,
                "path": example.link,
                "origin": constant.ORIGIN_PROPERTY_VALUE,
                "snpId": snp_key,
                "schVer": schema_key
            }
        )
        examples.append(example_entity)

    def _to_pc_object_entities(self, example: Example, snp_id: string, pc_objects: list):
        if example.graph is not (None, ""):
            self._append_pc_obj_entity(snp_id, example.graph, PrecompiledExample.GRAPH_EXTENSION.upper(), pc_objects)
        if example.output is not (None, ""):
            self._append_pc_obj_entity(snp_id, example.output, PrecompiledExample.OUTPUT_EXTENSION.upper(), pc_objects)
        if example.logs is not (None, ""):
            self._append_pc_obj_entity(snp_id, example.logs, PrecompiledExample.LOG_EXTENSION.upper(), pc_objects)

    def _append_pc_obj_entity(self, snp_id: str, content: str, pc_obj_type: str, pc_objects: list):
        pc_obj_entity = datastore.Entity(self._get_key(constant.PRECOMPILED_OBJECT_KIND, f"${snp_id}_${pc_obj_type}"), exclude_from_indexes=tuple('content'))
        pc_obj_entity.update({"content": content})
        pc_objects.append(pc_obj_entity)

    def _to_file_entities(self, snp_id: str, example: Example, files: list):
        file_entity = datastore.Entity(self._get_key(constant.FILED_KIND, f"${snp_id}_${0}"), exclude_from_indexes=tuple('content'))
        file_entity.update(
            {
                "name": example.name,
                "content": example.code,
                "cntxLine": example.tag.context_line,
                "isMain": True
            }
        )
        files.append(file_entity)
