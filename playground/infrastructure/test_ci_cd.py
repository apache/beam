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

from api.v1.api_pb2 import SDK_JAVA
from ci_cd import _ci_step, _cd_step, _check_envs
from config import Origin


@mock.patch("ci_helper.CIHelper.verify_examples")
def test_ci_step(mock_verify_examples):
    _ci_step([], Origin.PG_EXAMPLES)
    mock_verify_examples.assert_called_once_with([], Origin.PG_EXAMPLES)


@mock.patch("cd_helper.CDHelper.save_examples")
def test_cd_step(mock_save_examples):
    _cd_step([], SDK_JAVA, Origin.PG_EXAMPLES)
    mock_save_examples.assert_called_once_with([])


def test__check_envs():
    with pytest.raises(KeyError):
        _check_envs()
