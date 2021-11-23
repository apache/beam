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

from ci_cd import ci_step, cd_step


@mock.patch('ci_helper.CIHelper.verify_examples')
@mock.patch('ci_cd.find_examples')
@mock.patch('ci_cd.os.getenv')
def test_ci_step(mock_os_getenv, mock_find_examples, mock_verify_examples):
    mock_os_getenv.return_value = "MOCK_VALUE"
    mock_find_examples.return_value = []

    ci_step()

    mock_os_getenv.assert_called_once_with("BEAM_ROOT_DIR")
    mock_find_examples.assert_called_once_with("MOCK_VALUE")
    mock_verify_examples.assert_called_once_with([])


@mock.patch('cd_helper.CDHelper.store_examples')
@mock.patch('ci_cd.find_examples')
@mock.patch('ci_cd.os.getenv')
def test_cd_step(mock_os_getenv, mock_find_examples, mock_store_examples):
    mock_os_getenv.return_value = "MOCK_VALUE"
    mock_find_examples.return_value = []

    cd_step()

    mock_os_getenv.assert_called_once_with("BEAM_ROOT_DIR")
    mock_find_examples.assert_called_once_with("MOCK_VALUE")
    mock_store_examples.assert_called_once_with([])
