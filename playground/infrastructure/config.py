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
Configuration for CI/CD steps
"""

import os
from dataclasses import dataclass
from enum import Enum
from typing import Literal

from api.v1.api_pb2 import STATUS_VALIDATION_ERROR, STATUS_ERROR, \
    STATUS_PREPARATION_ERROR, STATUS_COMPILE_ERROR, \
    STATUS_RUN_TIMEOUT, STATUS_RUN_ERROR, SDK_JAVA, SDK_GO, SDK_PYTHON, \
    SDK_SCIO, Sdk
from constants import SERVER_ADDRESS_ENV_VAR_KEY, SDK_CONFIG_ENV_VAR_KEY


@dataclass(frozen=True)
class Config:
    """
    General configuration for CI/CD steps
    """
    SERVER_ADDRESS = os.getenv(SERVER_ADDRESS_ENV_VAR_KEY, "localhost:8080")
    EXTENSION_TO_SDK = {
        "java": SDK_JAVA, "go": SDK_GO, "py": SDK_PYTHON, "scala": SDK_SCIO
    }
    SUPPORTED_SDK = (
        Sdk.Name(SDK_JAVA),
        Sdk.Name(SDK_GO),
        Sdk.Name(SDK_PYTHON),
        Sdk.Name(SDK_SCIO))
    SDK_TO_EXTENSION = {
        SDK_JAVA: "java", SDK_GO: "go", SDK_PYTHON: "py", SDK_SCIO: "scala"
    }
    ERROR_STATUSES = [
        STATUS_VALIDATION_ERROR,
        STATUS_ERROR,
        STATUS_PREPARATION_ERROR,
        STATUS_COMPILE_ERROR,
        STATUS_RUN_TIMEOUT,
        STATUS_RUN_ERROR
    ]
    BEAM_PLAYGROUND_TITLE = "beam-playground:\n"
    BEAM_PLAYGROUND = "beam-playground"
    PAUSE_DELAY = 1
    CI_STEP_NAME = "CI"
    CD_STEP_NAME = "CD"
    CI_CD_LITERAL = Literal["CI", "CD"]
    URL_VCS_PREFIX = "https://github.com/apache/beam/blob/master"
    SDK_CONFIG = os.getenv(SDK_CONFIG_ENV_VAR_KEY, "../../playground/sdks.yaml")
    DEFAULT_NAMESPACE = "Playground"


@dataclass(frozen=True)
class TagFields:
    name: str = "name"
    description: str = "description"
    multifile: str = "multifile"
    categories: str = "categories"
    pipeline_options: str = "pipeline_options"
    default_example: str = "default_example"
    context_line: str = "context_line"
    complexity: str = "complexity"
    tags: str = "tags"
    emulators: str = "emulators"
    datasets: str = "datasets"
    url_notebook: str = "url_notebook"


@dataclass(frozen=True)
class PrecompiledExample:
    OUTPUT_EXTENSION = "output"
    LOG_EXTENSION = "log"
    GRAPH_EXTENSION = "graph"
    META_NAME = "meta"
    META_EXTENSION = "info"


@dataclass(frozen=True)
class PrecompiledExampleType:
    examples = "examples"
    katas = "katas"
    test_ends = ("test", "it")


@dataclass(frozen=True)
class DatastoreProps:
    NAMESPACE = "Playground"
    KEY_NAME_DELIMITER = "_"
    EXAMPLE_KIND = "pg_examples"
    SNIPPET_KIND = "pg_snippets"
    SCHEMA_KIND = "pg_schema_versions"
    PRECOMPILED_OBJECT_KIND = "pg_pc_objects"
    FILES_KIND = "pg_files"
    SDK_KIND = "pg_sdks"
    DATASET_KIND = "pg_datasets"


@dataclass(frozen=True)
class RepoProps:
    REPO_DATASETS_PATH = "../backend/datasets"

class Origin(str, Enum):
    PG_EXAMPLES = 'PG_EXAMPLES'
    PG_USER = 'PG_USER'
    PG_BEAMDOC = 'PG_BEAMDOC'
    TB_EXAMPLES = 'TB_EXAMPLES'
    TB_USER = 'TB_USER'

