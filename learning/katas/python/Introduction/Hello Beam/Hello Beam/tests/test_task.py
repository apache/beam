#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import unittest

from test_helper import get_file_output, test_is_not_empty


class TestCase(unittest.TestCase):
    def test_not_empty(self):
        self.assertTrue(test_is_not_empty(), 'The output is empty')

    def test_output(self):
        output = get_file_output(path='task.py')

        # Remove warning line about docker and Python versions
        output = [x for x in output if not x.startswith("WARNING")]

        self.assertIn('Hello Beam', output, 'The input element should contain "Hello Beam".')
        self.assertEqual(1, len(output), 'The output should contain a single element.')

