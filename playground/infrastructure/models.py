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
import logging
import os.path
import pathlib

from enum import Enum, IntEnum
from typing import List, Optional, Dict
from api.v1 import api_pb2

from pydantic import BaseModel, Extra, Field, validator, root_validator, HttpUrl

from config import RepoProps


class ComplexityEnum(str, Enum):
    BASIC = "BASIC"
    MEDIUM = "MEDIUM"
    ADVANCED = "ADVANCED"


class DatasetFormat(str, Enum):
    JSON = "json"
    AVRO = "avro"


class DatasetLocation(str, Enum):
    LOCAL = "local"


class Dataset(BaseModel):
    format: DatasetFormat
    location: DatasetLocation
    file_name: str = ""


class Topic(BaseModel):
    id: str
    source_dataset: str


class EmulatorType(str, Enum):
    KAFKA = "kafka"


class Emulator(BaseModel):
    type: EmulatorType
    topic: Topic


class ImportFile(BaseModel):
    name: str = Field(..., min_length=1)
    context_line: int = 0
    content: str = ""


class Tag(BaseModel):
    """
    Tag represents the beam-playground embedded yaml content
    """

    # These parameters are parsed from YAML and are required:

    categories: List[str] = []
    """
    Titles of categories to list this snippet in. Non-existent categories will be created.
    """

    complexity: ComplexityEnum
    """
    How hard is the snippet to understand.
    """

    context_line: int = 1
    """
    The line number to scroll to when the snippet is loaded.
    This applies after the metadata block is removed from the file, so discount for those lines.
    Integer, 1-based.
    """

    description: str
    """
    Description text to show on mouse over.
    """

    name: str = Field(..., min_length=1)
    """
    Name to show in the dropdown and to compose snippet ID from.
    """

    # These parameters are parsed from YAML and are optional:

    datasets: Dict[str, Dataset] = {}
    """
    Datasets which will be used by emulators. Example:
    datasets:
      CountWords:
        location: local
        format: json
    """

    default_example: bool = False
    """
    Whether this is the default example in its SDK.
    If multiple snippets set this to `true` the behavior is undefined.
    """

    emulators: List[Emulator] = []
    """
    List of emulators to start during pipeline execution. Currently only `kafka` type is supported.
    Example:
    emulators:
      - type: kafka
        topic:
          id: dataset
          source_dataset: CountWords
    """

    always_run: bool = False
    """
    The example output will not be cached and playground will always run its code when user clicks 'Run' if this is set to 'True'
    """

    never_run: bool = False
    """
    If 'True', it will not be possible to run this example from Playground UI. It will be read-only.
    """


    multifile: bool = False
    """
    Whether this is a file of a multi-file example.
    """

    pipeline_options: str = ""
    """
    Pipeline options. Example:
    pipeline_options: --name1 value1 --name2 value2
    """

    tags: List[str] = []
    """
    Tags by which this snippet can be found in the dropdown.
    """

    url_notebook: Optional[HttpUrl] = None
    """
    The URL of the Colab notebook that is based on this snippet.
    """

    # These parameters are NOT parsed from YAML but are added at construction:

    filepath: str = Field(..., min_length=1)
    files: List[ImportFile] = []
    line_start: int
    line_finish: int

    class Config:
        supported_categories = []
        extra = Extra.forbid

    @root_validator(skip_on_failure=True)
    def lines_order(cls, values):
        assert (
            (0 <= values["line_start"] < values["line_finish"]) and
              (values["context_line"] <= values["line_start"] or values["context_line"] > values["line_finish"])
        ), f"line ordering error: {values}"
        return values

    @root_validator(skip_on_failure=True)
    def datasets_with_emulators(csl, values):
        if values.get("datasets") and not values.get("emulators"):
            raise ValueError("datasets w/o emulators")
        return values

    @validator("emulators", each_item=True)
    def dataset_defined(cls, v, values, **kwargs):
        if "datasets" not in values:
            raise ValueError("datasets not defined")
        for dataset_id in values["datasets"]:
            if dataset_id == v.topic.source_dataset:
                return v
        raise ValueError(
            f"Emulator topic {v.topic.id} has undefined dataset {v.topic.source_dataset}"
        )

    @validator("datasets")
    def dataset_file_name(cls, datasets):
        for dataset_id, dataset in datasets.items():
            dataset.file_name = f"{dataset_id}.{dataset.format}"
            if dataset.location == DatasetLocation.LOCAL:
                dataset_path = os.path.join(
                    RepoProps.REPO_DATASETS_PATH, dataset.file_name
                )
                if not os.path.isfile(dataset_path):
                    logging.error(
                        "File not found at the specified path: %s", dataset_path
                    )
                    raise FileNotFoundError
        return datasets

    @validator("categories", each_item=True)
    def category_supported(cls, v, values, config, **kwargs):
        if v not in config.supported_categories:
            raise ValueError(f"Category {v} not in {config.supported_categories}")
        return v

    @root_validator
    def multifile_files(cls, values):
        if values.get('multifile', False) and not values.get('files', []):
            raise ValueError('multifile is True but no files defined')
        return values

    @validator("filepath")
    def check_filepath_exists(cls, v: str):
        if not os.path.isfile(v):
            logging.error("Example file not found: %s", v)
            raise FileNotFoundError(v)
        return v

    @validator("files", each_item=True)
    def check_files(cls, v: ImportFile, values):
        local_path = os.path.join(os.path.dirname(values["filepath"]), v.name)
        if not os.path.isfile(local_path):
            logging.error("Import file not found: %s", local_path)
            raise FileNotFoundError(local_path)
        v.content = open(local_path).read()
        return v


class SdkEnum(IntEnum):
    JAVA = api_pb2.SDK_JAVA
    GO = api_pb2.SDK_GO
    PYTHON = api_pb2.SDK_PYTHON
    SCIO = api_pb2.SDK_SCIO


def StringToSdkEnum(s: str) -> SdkEnum:
    parsed: int = api_pb2.Sdk.Value(s)
    return SdkEnum(parsed)


class Example(BaseModel):
    """
    Class which contains all information about beam example
    """

    sdk: SdkEnum
    tag: Tag
    filepath: str = Field(..., min_length=1)
    code: str = Field(..., min_length=1)
    url_vcs: HttpUrl
    type: int = api_pb2.PRECOMPILED_OBJECT_TYPE_UNSPECIFIED
    context_line: int = Field(..., gt=0)

    status: int = api_pb2.STATUS_UNSPECIFIED
    logs: str = ""
    pipeline_id: str = ""
    output: str = ""
    compile_output: str = ""
    graph: str = ""
