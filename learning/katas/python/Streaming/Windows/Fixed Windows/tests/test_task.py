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

        answers = [
            "('event', 4), window(start=2020-03-01T00:00:00Z, end=2020-03-02T00:00:00Z)",
            "('event', 2), window(start=2020-03-05T00:00:00Z, end=2020-03-06T00:00:00Z)",
            "('event', 3), window(start=2020-03-08T00:00:00Z, end=2020-03-09T00:00:00Z)",
            "('event', 1), window(start=2020-03-10T00:00:00Z, end=2020-03-11T00:00:00Z)"
        ]

        for ans in answers:
            self.assertIn(ans, output, "Incorrect output. Count the number of events per 1-day fixed window.")
