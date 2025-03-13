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
from mock.mock import AsyncMock

from api.v1.api_pb2 import (
    SDK_JAVA,
    STATUS_FINISHED,
    STATUS_ERROR,
    STATUS_VALIDATION_ERROR,
    STATUS_PREPARATION_ERROR,
    STATUS_RUN_TIMEOUT,
    STATUS_COMPILE_ERROR,
    STATUS_RUN_ERROR,
)

from config import Origin
from models import SdkEnum
from verify import Verifier, VerifyException


@pytest.mark.asyncio
async def test__verify_examples(create_test_example):
    verifier = Verifier(SdkEnum.JAVA, Origin.PG_EXAMPLES)
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
        await verifier._verify_examples(
            client, examples_with_errors, Origin.PG_EXAMPLES
        )
    with pytest.raises(VerifyException):
        await verifier._verify_examples(
            client, examples_without_def_ex, Origin.PG_EXAMPLES
        )
    with pytest.raises(VerifyException):
        await verifier._verify_examples(
            client, examples_with_several_def_ex, Origin.PG_EXAMPLES
        )
    await verifier._verify_examples(client, examples_without_errors, Origin.PG_EXAMPLES)


@pytest.mark.asyncio
@mock.patch("verify.update_example_status")
async def test_get_statuses(mock_update_example_status, create_test_example):
    example = create_test_example()
    client = mock.sentinel
    verifier = Verifier(SdkEnum.JAVA, Origin.PG_EXAMPLES)

    verifier._populate_fields = AsyncMock()

    await verifier._get_statuses(client, [example])

    mock_update_example_status.assert_called_once_with(example, client)