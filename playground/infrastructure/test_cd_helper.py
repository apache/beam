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

from cd_helper import CDHelper


@mock.patch('cd_helper.CDHelper._save_to_cloud')
@mock.patch('cd_helper.CDHelper._run_code')
def test_store_examples(mock_run_code, mock_save_to_cloud):
    helper = CDHelper()
    helper.store_examples([])

    mock_run_code.assert_called_once_with([])
    mock_save_to_cloud.assert_called_once_with([])
