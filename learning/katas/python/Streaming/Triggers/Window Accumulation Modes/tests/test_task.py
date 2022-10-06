#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import unittest

from test_helper import test_is_not_empty, get_file_output


class TestCase(unittest.TestCase):
    def test_not_empty(self):
        self.assertTrue(test_is_not_empty(), 'The output is empty')

    def test_output(self):
        output = get_file_output(path='task.py')

        answers = ["1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "2, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "3, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "4, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "5, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "6, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "7, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "8, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "9, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "10, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "11, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "12, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "13, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "14, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "15, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "16, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "17, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "18, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "19, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
                   "20, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)"]

        for ans in answers:
            self.assertIn(ans, output, "Incorrect output. Try using a count early trigger, with accumulating mode.")
