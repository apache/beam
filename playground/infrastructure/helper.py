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
Common helper module for CI/CD Steps
"""
import asyncio
import logging
import os
import urllib.parse
from pathlib import PurePath
from typing import List, Optional, Dict
from api.v1 import api_pb2

import pydantic
import yaml

from api.v1.api_pb2 import (
    SDK_UNSPECIFIED,
    STATUS_UNSPECIFIED,
    Sdk,
    STATUS_VALIDATING,
    STATUS_PREPARING,
    STATUS_COMPILING,
    STATUS_EXECUTING,
    PRECOMPILED_OBJECT_TYPE_UNIT_TEST,
    PRECOMPILED_OBJECT_TYPE_KATA,
    PRECOMPILED_OBJECT_TYPE_UNSPECIFIED,
    PRECOMPILED_OBJECT_TYPE_EXAMPLE,
    PrecompiledObjectType,
)
from config import Config, TagFields, PrecompiledExampleType
from grpc_client import GRPCClient
from constants import BEAM_ROOT_DIR_ENV_VAR_KEY

from models import Example, Tag, SdkEnum, Dataset


def _check_no_nested(subdirs: List[str]):
    """
    Check there're no nested subdirs

    Sort alphabetically and compare the pairs of adjacent items
    using pathlib.PurePath: we don't want fs calls in this check
    """
    sorted_subdirs = sorted(PurePath(s) for s in subdirs)
    for dir1, dir2 in zip(sorted_subdirs, sorted_subdirs[1:]):
        if dir1 in [dir2, *dir2.parents]:
            raise ValueError(f"{dir2} is a subdirectory of {dir1}")


def find_examples(root_dir: str, subdirs: List[str], sdk: SdkEnum) -> List[Example]:
    """
    Find and return beam examples.

    Search throws all child files of work_dir directory files with beam tag:
    Beam-playground:
        name: NameOfExample
        description: Description of NameOfExample.
        multifile: false
        default_example: false
        context_line: 10
        categories:
            - category-1
            - category-2
        pipeline_options: --inputFile your_file --outputFile your_output_file
        complexity: MEDIUM
        tags:
            - example
    If some example contains beam tag with incorrect format raise an error.

    Args:
        root_dir: project root dir
        subdirs: sub-directories where to search examples.
        sdk: sdk that using to find examples for the specific sdk.

    Returns:
        List of Examples.
    """
    has_errors = False
    examples = []
    _check_no_nested(subdirs)
    for subdir in subdirs:
        subdir = os.path.join(root_dir, subdir)
        logging.info("subdir: %s", subdir)
        for root, _, files in os.walk(subdir):
            for filename in files:
                filepath = os.path.join(root, filename)
                try:
                    try:
                        example = _load_example(
                            filename=filename, filepath=filepath, sdk=sdk
                        )
                        if example is not None:
                            examples.append(example)
                    except pydantic.ValidationError as err:
                        if len(err.errors()) > 1:
                            raise
                        if err.errors()[0]["msg"] == "multifile is True but no files defined":
                            logging.warning("incomplete multifile example ignored %s", filepath)
                            continue
                        raise
                except Exception:
                    logging.exception("error loading example at %s", filepath)
                    has_errors = True
    if has_errors:
        raise ValueError(
            "Some of the beam examples contain beam playground tag with "
            "an incorrect format"
        )
    return examples


def get_tag(filepath: PurePath) -> Optional[Tag]:
    """
    Parse file by filepath and find beam tag

    Args:
        filepath: path of the file

    Returns:
        If file contains tag, returns Tag object
        If file doesn't contain tag, returns None
    """
    with open(filepath, encoding="utf-8") as parsed_file:
        lines = parsed_file.readlines()

    line_start: Optional[int] = None
    line_finish: Optional[int] = None
    tag_prefix: Optional[str] = ""
    for idx, line in enumerate(lines):
        if line_start is None and line.endswith(Config.BEAM_PLAYGROUND_TITLE):
            line_start = idx
            prefix_len = len(line) - len(Config.BEAM_PLAYGROUND_TITLE)
            tag_prefix = line[:prefix_len]
        elif line_start and not line.startswith(tag_prefix):
            line_finish = idx
            break

    if not line_start or not line_finish:
        return None

    embdedded_yaml_content = "".join(
        line[len(tag_prefix) :] for line in lines[line_start:line_finish]
    )
    yml = yaml.load(embdedded_yaml_content, Loader=yaml.SafeLoader)

    try:
        return Tag(
            filepath=str(filepath),
            line_start=line_start,
            line_finish=line_finish,
            **yml[Config.BEAM_PLAYGROUND],
            )
    except pydantic.ValidationError as err:
        if len(err.errors()) == 1 and err.errors()[0]["msg"] == "multifile is True but no files defined":
            logging.warning("incomplete multifile example ignored %s", filepath)
            return None
        raise

def _load_example(filename, filepath, sdk: SdkEnum) -> Optional[Example]:
    """
    Check file by filepath for matching to beam example. If file is beam example,

    Args:
        filename: name of the file.
        filepath: path to the file.
        sdk: sdk that using to find examples for the specific sdk.

    Returns:
        If the file is an example, return Example object
        If it's not, return None
        In case of error, raise Exception
    """
    logging.debug("inspecting file %s", filepath)
    extension = filepath.split(os.extsep)[-1]
    if extension == Config.SDK_TO_EXTENSION[sdk]:
        logging.debug("sdk %s matched extension %s", api_pb2.Sdk.Name(sdk), extension)
        tag = get_tag(filepath)
        if tag is not None:
            logging.debug("playground-beam tag found")
            return _get_example(filepath, filename, tag, sdk)
    return None


# Make load_supported_categories called only once
# to make testing easier
_load_supported_categories = False


def load_supported_categories(categories_path: str):
    """
    Load the list of supported categories from categories_path file
    into Tag model config

    Args:
        categories_path: path to the file with categories.
    """
    global _load_supported_categories
    if _load_supported_categories:
        return
    with open(categories_path, encoding="utf-8") as supported_categories:
        yaml_object = yaml.load(supported_categories.read(), Loader=yaml.SafeLoader)

    Tag.Config.supported_categories = yaml_object[TagFields.categories]
    _load_supported_categories = True


def _get_content(filepath: str, tag_start_line: int, tag_finish_line) -> str:
    with open(filepath, encoding="utf-8") as parsed_file:
        lines = parsed_file.readlines()
        lines = lines[:tag_start_line] + lines[tag_finish_line:]
    return "".join(lines)


def _get_url_vcs(filepath: str) -> str:
    """
    Construct VCS URL from example's filepath
    """
    root_dir = os.getenv(BEAM_ROOT_DIR_ENV_VAR_KEY, "../..")
    rel_path = os.path.relpath(filepath, root_dir)
    url_vcs = "{}/{}".format(Config.URL_VCS_PREFIX, urllib.parse.quote(rel_path))
    return url_vcs


def _get_example(filepath: str, filename: str, tag: Tag, sdk: int) -> Example:
    """
    Return an Example by filepath and filename.

    Args:
         filepath: path of the example's file.
         filename: name of the example's file.
         tag: tag of the example.

    Returns:
        Parsed Example object.
    """

    # Calculate context line with tag removed. Note: context_line is 1-based, line_start and line_finish are 0-based.
    context_line = tag.context_line if tag.context_line <= tag.line_start else tag.context_line - (tag.line_finish - tag.line_start)
    
    return Example(
        sdk=SdkEnum(sdk),
        tag=tag,
        filepath=filepath,
        status=STATUS_UNSPECIFIED,
        type=_get_object_type(filename, filepath),
        code=_get_content(filepath, tag.line_start, tag.line_finish),
        url_vcs=_get_url_vcs(filepath),  # type: ignore
        context_line=context_line,
    )


async def update_example_status(example: Example, client: GRPCClient):
    """
    Receive status for examples and update example.status and pipeline_id

    Use client to send requests to the backend:
    1. Start code processing.
    2. Ping the backend while status is STATUS_VALIDATING/
      STATUS_PREPARING/STATUS_COMPILING/STATUS_EXECUTING
    Update example.status with resulting status.

    Args:
        example: beam example for processing and updating status and pipeline_id.
        client: client to send requests to the server.
    """
    datasets: List[api_pb2.Dataset] = []
    for emulator in example.tag.emulators:
        dataset: Dataset = example.tag.datasets[emulator.topic.source_dataset]

        datasets.append(
            api_pb2.Dataset(
                type=api_pb2.EmulatorType.Value(
                    f"EMULATOR_TYPE_{emulator.type.upper()}"
                ),
                options={"topic": emulator.topic.id},
                dataset_path=dataset.file_name,
            )
        )
    files: List[api_pb2.SnippetFile] = [
        api_pb2.SnippetFile(name=example.filepath, content=example.code, is_main=True)
    ]
    for file in example.tag.files:
        files.append(
            api_pb2.SnippetFile(name=file.name, content=file.content, is_main=False)
        )

    pipeline_id = await client.run_code(
        example.code, example.sdk, example.tag.pipeline_options, datasets, files=files,
    )
    example.pipeline_id = pipeline_id
    status = await client.check_status(pipeline_id)
    while status in [
        STATUS_VALIDATING,
        STATUS_PREPARING,
        STATUS_COMPILING,
        STATUS_EXECUTING,
    ]:
        await asyncio.sleep(Config.PAUSE_DELAY)
        status = await client.check_status(pipeline_id)
    example.status = status


def _get_object_type(filename, filepath):
    """
    Get type of an object based on it filename/filepath

    Args:
        filename: object's filename
        filepath: object's filepath

    Returns: type of the object (example, kata, unit-test)
    """
    filename_no_ext = (os.path.splitext(filename)[0]).lower()
    if filename_no_ext.endswith(PrecompiledExampleType.test_ends):
        object_type = PRECOMPILED_OBJECT_TYPE_UNIT_TEST
    elif PrecompiledExampleType.katas in filepath.split(os.sep):
        object_type = PRECOMPILED_OBJECT_TYPE_KATA
    elif PrecompiledExampleType.examples in filepath.split(os.sep):
        object_type = PRECOMPILED_OBJECT_TYPE_EXAMPLE
    else:
        object_type = PRECOMPILED_OBJECT_TYPE_UNSPECIFIED
    return object_type


class DuplicatesError(Exception):
    pass


class ConflictingDatasetsError(Exception):
    pass


def validate_examples_for_duplicates_by_name(examples: List[Example]):
    """
    Validate examples for duplicates by example name to avoid duplicates in the Cloud Datastore
    :param examples: examples from the repository for saving to the Cloud Datastore
    """
    duplicates: Dict[str, Example] = {}
    for example in examples:
        if example.tag.name not in duplicates.keys():
            duplicates[example.tag.name] = example
        else:
            err_msg = f"Examples have duplicate names.\nDuplicates: \n - path #1: {duplicates[example.tag.name].filepath} \n - path #2: {example.filepath}"
            logging.error(err_msg)
            raise DuplicatesError(err_msg)


def validate_examples_for_conflicting_datasets(examples: List[Example]):
    """
    Validate examples for conflicting datasets to avoid conflicts in the Cloud Datastore
    :param examples: examples from the repository for saving to the Cloud Datastore
    """
    datasets: Dict[str, Dataset] = {}
    for example in examples:
        for k, v in example.tag.datasets.items():
            if k not in datasets:
                datasets[k] = v
            elif datasets[k].file_name != v.file_name or \
                    datasets[k].format != v.format or \
                    datasets[k].location != v.location:
                err_msg = f"Examples have conflicting datasets.\n" \
                          f"Conflicts: \n" \
                          f" - file_name #1: {datasets[k].file_name} \n" \
                          f" - format #1: {datasets[k].format} \n" \
                          f"  - location #1: {datasets[k].location} \n" \
                          f" - file_name #2: {v.file_name}\n" \
                          f" - format #2: {v.format}\n" \
                          f" - location #2: {v.location}\n" \
                          f"Dataset name: {k}"
                logging.error(err_msg)
                raise ConflictingDatasetsError(err_msg)
