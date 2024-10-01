#
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
#

"""Unit tests for thread utilities."""

import unittest
import threading

from apache_beam.utils.threads import ParentAwareThread


class ParentAwareThreadTest(unittest.TestCase):
  def test_child_tread_can_access_parent_thread_id(self):
    expected_parent_thread_id = threading.get_ident()
    actual_parent_thread_id = None

    def get_parent_thread_id():
      nonlocal actual_parent_thread_id
      actual_parent_thread_id = threading.current_thread().parent_thread_id

    thread = ParentAwareThread(target=get_parent_thread_id)
    thread.start()
    thread.join()

    self.assertEqual(expected_parent_thread_id, actual_parent_thread_id)
