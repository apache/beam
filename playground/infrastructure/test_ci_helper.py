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

import copy
import uuid

import mock
import pytest

from api.v1.api_pb2 import SDK_JAVA, STATUS_FINISHED, STATUS_ERROR, \
    STATUS_VALIDATION_ERROR, STATUS_PREPARATION_ERROR, STATUS_RUN_TIMEOUT, \
    STATUS_COMPILE_ERROR, STATUS_RUN_ERROR
from ci_helper import CIHelper, VerifyException
from helper import Example, Tag


@pytest.mark.asyncio
@mock.patch("ci_helper.CIHelper._verify_examples")
@mock.patch("ci_helper.get_statuses")
async def test_verify_examples(mock_get_statuses, mock_verify_examples):
    helper = CIHelper()
    await helper.verify_examples([])

    mock_get_statuses.assert_called_once_with([])
    mock_verify_examples.assert_called_once_with([])


@pytest.mark.asyncio
@mock.patch("grpc_client.GRPCClient.get_run_error")
@mock.patch("grpc_client.GRPCClient.get_compile_output")
async def test__verify_examples(mock_get_compile_output, mock_get_run_output):
    helper = CIHelper()
    object_meta = {
        "name": "name",
        "description": "description",
        "multifile": False,
        "categories": ["category-1", "category-2"],
        "pipeline_options": "--option option",
        "default_example": False
    }
    object_meta_def_ex = copy.copy(object_meta)
    object_meta_def_ex["default_example"] = True
    pipeline_id = str(uuid.uuid4())
    default_example = Example(
        name="name",
        complexity="MEDIUM",
        pipeline_id=pipeline_id,
        sdk=SDK_JAVA,
        filepath="filepath",
        code="code_of_example",
        output="output_of_example",
        status=STATUS_FINISHED,
        tag=Tag(**object_meta_def_ex),
        link="link")
    finished_example = Example(
        name="name",
        complexity="MEDIUM",
        pipeline_id=pipeline_id,
        sdk=SDK_JAVA,
        filepath="filepath",
        code="code_of_example",
        output="output_of_example",
        status=STATUS_FINISHED,
        tag=Tag(**object_meta),
        link="link")
    examples_without_def_ex = [
        finished_example,
        finished_example,
    ]
    examples_with_several_def_ex = [
        default_example,
        default_example,
    ]
    examples_without_errors = [
        default_example,
        finished_example,
    ]
    examples_with_errors = [
        Example(
            name="name",
            complexity="MEDIUM",
            pipeline_id=pipeline_id,
            sdk=SDK_JAVA,
            filepath="filepath",
            code="code_of_example",
            output="output_of_example",
            status=STATUS_VALIDATION_ERROR,
            tag=Tag(**object_meta_def_ex),
            link="link"),
        Example(
            name="name",
            complexity="MEDIUM",
            pipeline_id=pipeline_id,
            sdk=SDK_JAVA,
            filepath="filepath",
            code="code_of_example",
            output="output_of_example",
            status=STATUS_ERROR,
            tag=Tag(**object_meta),
            link="link"),
        Example(
            name="name",
            complexity="MEDIUM",
            pipeline_id=pipeline_id,
            sdk=SDK_JAVA,
            filepath="filepath",
            code="code_of_example",
            output="output_of_example",
            status=STATUS_COMPILE_ERROR,
            tag=Tag(**object_meta),
            link="link"),
        Example(
            name="name",
            complexity="MEDIUM",
            pipeline_id=pipeline_id,
            sdk=SDK_JAVA,
            filepath="filepath",
            code="code_of_example",
            output="output_of_example",
            status=STATUS_PREPARATION_ERROR,
            tag=Tag(**object_meta),
            link="link"),
        Example(
            name="name",
            complexity="MEDIUM",
            pipeline_id=pipeline_id,
            sdk=SDK_JAVA,
            filepath="filepath",
            code="code_of_example",
            output="output_of_example",
            status=STATUS_RUN_TIMEOUT,
            tag=Tag(**object_meta),
            link="link"),
        Example(
            name="name",
            complexity="MEDIUM",
            pipeline_id=pipeline_id,
            sdk=SDK_JAVA,
            filepath="filepath",
            code="code_of_example",
            output="output_of_example",
            status=STATUS_VALIDATION_ERROR,
            tag=Tag(**object_meta),
            link="link"),
        Example(
            name="name",
            complexity="MEDIUM",
            pipeline_id=pipeline_id,
            sdk=SDK_JAVA,
            filepath="filepath",
            code="code_of_example",
            output="output_of_example",
            status=STATUS_RUN_ERROR,
            tag=Tag(**object_meta),
            link="link"),
    ]

    with pytest.raises(VerifyException):
        await helper._verify_examples(examples_with_errors)
    with pytest.raises(VerifyException):
        await helper._verify_examples(examples_without_def_ex)
    with pytest.raises(VerifyException):
        await helper._verify_examples(examples_with_several_def_ex)
    await helper._verify_examples(examples_without_errors)
