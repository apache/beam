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

from ci_helper import CIHelper


@mock.patch('ci_helper.CIHelper._verify_examples_status')
@mock.patch('ci_helper.get_statuses')
def test_verify_examples(mock_get_statuses, mock_verify_examples_statuses):
    helper = CIHelper()
    helper.verify_examples([])

    mock_get_statuses.assert_called_once_with([])
    mock_verify_examples_statuses.assert_called_once_with([])
