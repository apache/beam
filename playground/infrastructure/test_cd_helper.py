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

import unittest
import mock

import test_utils
from api.v1.api_pb2 import SDK_JAVA
from cd_helper import CDHelper

"""
Unit tests for the CD helper
"""


class TestCDHelper(unittest.TestCase):

    @mock.patch("cd_helper.CDHelper._save_to_datastore")
    @mock.patch("cd_helper.CDHelper._get_outputs")
    def test_save_examples(self, mock_get_outputs, mock_save_to_datastore):
        examples = test_utils._get_examples(1)
        helper = CDHelper()
        helper.save_examples(examples, SDK_JAVA)
        mock_get_outputs.assert_called_once()
        mock_save_to_datastore.assert_called_once_with(examples, SDK_JAVA)
