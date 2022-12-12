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

import mock
import pytest

from api.v1.api_pb2 import SDK_JAVA, STATUS_FINISHED, STATUS_ERROR, \
    STATUS_VALIDATION_ERROR, STATUS_PREPARATION_ERROR, STATUS_RUN_TIMEOUT, \
    STATUS_COMPILE_ERROR, STATUS_RUN_ERROR
from ci_helper import CIHelper, VerifyException
from config import Origin


@pytest.mark.asyncio
@mock.patch("ci_helper.CIHelper._verify_examples")
@mock.patch("ci_helper.get_statuses")
async def test_verify_examples(mock_get_statuses, mock_verify_examples):
    helper = CIHelper()
    await helper.verify_examples([], Origin.PG_EXAMPLES)

    mock_get_statuses.assert_called_once_with(mock.ANY, [])
    mock_verify_examples.assert_called_once_with(mock.ANY, [], Origin.PG_EXAMPLES)


@pytest.mark.asyncio
async def test__verify_examples(create_test_example):
    helper = CIHelper()
    default_example = create_test_example(tag_meta=dict(default_example=True))
    finished_example = create_test_example(tag_meta=dict(default_example=True))
    finished_example = create_test_example(status=STATUS_FINISHED)
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
        create_test_example(status=STATUS_VALIDATION_ERROR),
        create_test_example(status=STATUS_ERROR),
        create_test_example(status=STATUS_COMPILE_ERROR),
        create_test_example(status=STATUS_PREPARATION_ERROR),
        create_test_example(status=STATUS_RUN_TIMEOUT),
        create_test_example(status=STATUS_RUN_ERROR),
    ]
    client = mock.AsyncMock()
    with pytest.raises(VerifyException):
        await helper._verify_examples(client, examples_with_errors, Origin.PG_EXAMPLES)
    with pytest.raises(VerifyException):
        await helper._verify_examples(client, examples_without_def_ex, Origin.PG_EXAMPLES)
    with pytest.raises(VerifyException):
        await helper._verify_examples(client, examples_with_several_def_ex, Origin.PG_EXAMPLES)
    await helper._verify_examples(client, examples_without_errors, Origin.PG_EXAMPLES)
