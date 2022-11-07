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
import logging
import os.path
from datetime import datetime
from pathlib import Path
from typing import List

import yaml
from google.cloud import datastore
from tqdm import tqdm

import config
from config import Config, Origin, PrecompiledExample, DatastoreProps
from helper import Example

from api.v1.api_pb2 import Sdk, PrecompiledObjectType


class DatastoreException(Exception):
    def __init__(self, error: str):
        super().__init__()
        self.msg = error

    def __str__(self):
        return self.msg


# Google Datastore documentation link: https://cloud.google.com/datastore/docs/concepts
class DatastoreClient:
    """DatastoreClient is a datastore client for sending a request to the Google."""
    _datastore_client: datastore.Client

    def __init__(self):
        self._check_envs()
        self._datastore_client = datastore.Client(
            namespace=DatastoreProps.NAMESPACE,
            project=Config.GOOGLE_CLOUD_PROJECT
        )

    def _check_envs(self):
        if Config.GOOGLE_CLOUD_PROJECT is None:
            raise KeyError("GOOGLE_CLOUD_PROJECT environment variable should be specified in os")
        if Config.SDK_CONFIG is None:
            raise KeyError("SDK_CONFIG environment variable should be specified in os")

    def save_to_cloud_datastore(self, examples_from_rep: List[Example], sdk: Sdk, origin: Origin):
        """
        Save examples, output and meta to datastore
        Args:
            :param examples_from_rep: examples from the repository for saving to the Cloud Datastore
            :param sdk: sdk from parameters
            :param origin: typed origin const PG_EXAMPLES | TB_EXAMPLES
        """
        # initialise data
        snippets = []
        examples = []
        pc_objects = []
        files = []
        updated_example_ids = []
        now = datetime.today()

        # retrieve the last schema version
        actual_schema_version_key = self._get_actual_schema_version_key()

        # retrieve all example keys before updating
        examples_ids_before_updating = self._get_all_examples(sdk, origin)

        # loop through every example to save them to the Cloud Datastore
        with self._datastore_client.transaction():
            for example in tqdm(examples_from_rep):
                sdk_key = self._get_key(DatastoreProps.SDK_KIND, Sdk.Name(example.sdk))
                example_id = self._make_example_id(origin, sdk, example.name)
                updated_example_ids.append(example_id)
                examples.append(
                    self._to_example_entity(example, example_id, sdk_key, actual_schema_version_key, origin)
                )
                snippets.append(
                    self._to_snippet_entity(example, example_id, sdk_key, now, actual_schema_version_key, origin)
                )
                pc_objects.extend(self._pc_object_entities(example, example_id))
                files.append(self._to_file_entity(example, example_id))

            self._datastore_client.put_multi(examples)
            self._datastore_client.put_multi(snippets)
            self._datastore_client.put_multi(pc_objects)
            self._datastore_client.put_multi(files)

            # delete examples from the Cloud Datastore that are not in the repository
            examples_ids_for_removing = list(filter(lambda key: key not in updated_example_ids, examples_ids_before_updating))
            if len(examples_ids_for_removing) != 0:
                logging.info("Start of deleting extra playground examples ...")
                examples_keys_for_removing = list(
                    map(lambda ex_id: self._get_key(DatastoreProps.EXAMPLE_KIND, ex_id), examples_ids_for_removing)
                )
                snippets_keys_for_removing = list(
                    map(lambda ex_id: self._get_key(DatastoreProps.SNIPPET_KIND, ex_id), examples_ids_for_removing)
                )
                file_keys_for_removing = list(
                    map(self._get_files_key, examples_ids_for_removing)
                )
                pc_objs_keys_for_removing = []
                for example_id_item in examples_ids_for_removing:
                    for example_type in [
                        PrecompiledExample.GRAPH_EXTENSION.upper(),
                        PrecompiledExample.OUTPUT_EXTENSION.upper(),
                        PrecompiledExample.LOG_EXTENSION.upper()
                    ]:
                        pc_objs_keys_for_removing.append(
                            self._get_key(DatastoreProps.PRECOMPILED_OBJECT_KIND,
                                f"{example_id_item}{config.DatastoreProps.KEY_NAME_DELIMITER}{example_type}")
                        )
                self._datastore_client.delete_multi(examples_keys_for_removing)
                self._datastore_client.delete_multi(snippets_keys_for_removing)
                self._datastore_client.delete_multi(file_keys_for_removing)
                self._datastore_client.delete_multi(pc_objs_keys_for_removing)
                logging.info("Finish of deleting extra playground examples ...")

    def save_catalogs(self):
        """
        Save catalogs to the Cloud Datastore
        """
        # save a schema version entity
        schema_entity = datastore.Entity(
            self._get_key(DatastoreProps.SCHEMA_KIND, "0.0.1"),
            exclude_from_indexes=('descr',)
        )
        schema_entity.update(
            {
                "descr": "Data initialization: a schema version, SDKs"
            }
        )
        self._datastore_client.put(schema_entity)

        # save a sdk catalog
        sdk_objs: any = None
        with open(Config.SDK_CONFIG, encoding="utf-8") as sdks:
            sdk_objs = yaml.load(sdks.read(), Loader=yaml.SafeLoader)

        sdk_entities = []
        file_name = Path(Config.SDK_CONFIG).stem
        for key in sdk_objs[file_name]:
            default_example = sdk_objs[file_name][key]["default-example"]
            sdk_entity = datastore.Entity(self._get_key(DatastoreProps.SDK_KIND, key))
            sdk_entity.update(
                {
                    "defaultExample": default_example
                }
            )
            sdk_entities.append(sdk_entity)

        self._datastore_client.put_multi(sdk_entities)

    def _get_actual_schema_version_key(self) -> datastore.Key:
        schema_names = []
        last_schema_version_query = self._datastore_client.query(kind=DatastoreProps.SCHEMA_KIND)
        last_schema_version_query.keys_only()
        schema_iterator = last_schema_version_query.fetch()
        schemas = list(schema_iterator)
        if len(schemas) == 0:
            logging.error("Schema versions not found")
            raise DatastoreException("Schema versions not found. Schema versions must be downloaded during application startup")
        for schema in schemas:
            schema_names.append(schema.key.name)
        schema_names.sort(reverse=True)
        return self._get_key(DatastoreProps.SCHEMA_KIND, schema_names[0])

    def _get_all_examples(self, sdk: Sdk, origin: Origin) -> List[str]:
        examples_ids_before_updating = []
        all_examples_query = self._datastore_client.query(kind=DatastoreProps.EXAMPLE_KIND)
        all_examples_query.add_filter("sdk", "=", self._get_key(DatastoreProps.SDK_KIND, Sdk.Name(sdk)))
        all_examples_query.add_filter("origin", "=", origin)
        all_examples_query.keys_only()
        examples_iterator = all_examples_query.fetch()
        for example_item in examples_iterator:
            examples_ids_before_updating.append(example_item.key.name)
        return examples_ids_before_updating

    def _get_key(self, kind: str, identifier: str) -> datastore.Key:
        return self._datastore_client.key(kind, identifier)
    
    def _get_snippet_key(self, snippet_id: str):
        return self._get_key(DatastoreProps.SNIPPET_KIND, snippet_id)

    def _get_example_key(self, example_id: str):
        return self._get_key(DatastoreProps.EXAMPLE_KIND, example_id)
    
    def _make_example_id(self, origin: Origin, sdk: Sdk, name: str):
        # ToB examples (and other related entities: snippets, files, pc_objects)
        # have origin prefix in a key
        if origin == Origin.TB_EXAMPLES:
            return config.DatastoreProps.KEY_NAME_DELIMITER.join([
                origin,
                Sdk.Name(sdk),
                name,
            ])
        return config.DatastoreProps.KEY_NAME_DELIMITER.join([
            Sdk.Name(sdk),
            name,
        ])


    def _get_files_key(self, snippet_id: str):
        name = config.DatastoreProps.KEY_NAME_DELIMITER.join(
            [snippet_id, '0']
        )
        return self._get_key(DatastoreProps.FILES_KIND, name)

    def _get_pc_objects_key(self, snp_id: str, pc_obj_type: str):
        return self._get_key(
            DatastoreProps.PRECOMPILED_OBJECT_KIND,
            config.DatastoreProps.KEY_NAME_DELIMITER.join([snp_id, pc_obj_type])
        )

    def _to_snippet_entity(self,
            example: Example,
            snp_id: str,
            sdk_key: datastore.Key,
            now: datetime,
            schema_key: datastore.Key,
            origin: Origin,
    ) -> datastore.Entity:
        snippet_entity = datastore.Entity(self._get_snippet_key(snp_id))
        snippet_entity.update(
            {
                "sdk": sdk_key,
                "pipeOpts": self._get_pipeline_options(example),
                "created": now,
                "origin": origin,
                "numberOfFiles": 1,
                "schVer": schema_key,
                "complexity": f"COMPLEXITY_{example.complexity}"
            }
        )
        return snippet_entity

    def _get_pipeline_options(self, example: Example):
        pip_opts = example.tag.pipeline_options
        if pip_opts is not None:
            return pip_opts
        return ""

    def _to_example_entity(self,
        example: Example,
        example_id: str,
        sdk_key: datastore.Key,
        schema_key: datastore.Key,
        origin: Origin,
    ) -> datastore.Entity:
        example_entity = datastore.Entity(self._get_example_key(example_id))
        example_entity.update(
            {
                "name": example.name,
                "sdk": sdk_key,
                "descr": example.tag.description,
                "tags": example.tag.tags,
                "cats": example.tag.categories,
                "path": example.link,
                "type": PrecompiledObjectType.Name(example.type),
                "origin": origin,
                "schVer": schema_key
            }
        )
        return example_entity

    def _pc_object_entities(self, example: Example, snp_id: str) -> List[datastore.Entity]:
        entities = []
        if len(example.graph) != 0:
            entities.append(
                self._pc_obj_entity(snp_id, example.graph, PrecompiledExample.GRAPH_EXTENSION.upper())
            )
        if len(example.output) != 0:
            entities.append(
                self._pc_obj_entity(snp_id, example.output, PrecompiledExample.OUTPUT_EXTENSION.upper())
            )
        if len(example.logs) != 0:
            entities.append(
                self._pc_obj_entity(snp_id, example.logs, PrecompiledExample.LOG_EXTENSION.upper())
            )
        return entities

    def _pc_obj_entity(self, snp_id: str, content: str, pc_obj_type: str) -> datastore.Entity:
        pc_obj_entity = datastore.Entity(
            self._get_pc_objects_key(snp_id, pc_obj_type),
            exclude_from_indexes=('content',)
        )
        pc_obj_entity.update({"content": content})
        return pc_obj_entity

    def _to_file_entity(self, example: Example, snp_id: str):
        file_entity = datastore.Entity(
            self._get_files_key(snp_id),
            exclude_from_indexes=('content',)
        )
        file_entity.update(
            {
                "name": self._get_file_name_with_extension(example.name, example.sdk),
                "content": example.code,
                "cntxLine": example.tag.context_line,
                "isMain": True
            }
        )
        return file_entity

    def _get_file_name_with_extension(self, name: str, sdk: Sdk) -> str:
        filename, file_extension = os.path.splitext(name)
        if len(file_extension) == 0:
            extension = Config.SDK_TO_EXTENSION[sdk]
            return f"{filename}.{extension}"
        return name
