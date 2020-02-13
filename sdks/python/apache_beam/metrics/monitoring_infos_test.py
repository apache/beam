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

# pytype: skip-file

from __future__ import absolute_import

import unittest

from apache_beam.metrics import monitoring_infos


class MonitoringInfosTest(unittest.TestCase):
  def test_parse_namespace_and_name_for_nonuser_metric(self):
    input = monitoring_infos.create_monitoring_info(
        "beam:dummy:metric", "typeurn", None)
    namespace, name = monitoring_infos.parse_namespace_and_name(input)
    self.assertEqual(namespace, "beam")
    self.assertEqual(name, "dummy:metric")

  def test_parse_namespace_and_name_for_user_metric(self):
    urn = monitoring_infos.USER_COUNTER_URN
    labels = {}
    labels[monitoring_infos.NAMESPACE_LABEL] = "counternamespace"
    labels[monitoring_infos.NAME_LABEL] = "countername"
    input = monitoring_infos.create_monitoring_info(
        urn, "typeurn", None, labels)
    namespace, name = monitoring_infos.parse_namespace_and_name(input)
    self.assertEqual(namespace, "counternamespace")
    self.assertEqual(name, "countername")

  def test_parse_namespace_and_name_for_user_distribution_metric(self):
    urn = monitoring_infos.USER_DISTRIBUTION_COUNTER_URN
    labels = {}
    labels[monitoring_infos.NAMESPACE_LABEL] = "counternamespace"
    labels[monitoring_infos.NAME_LABEL] = "countername"
    input = monitoring_infos.create_monitoring_info(
        urn, "typeurn", None, labels)
    namespace, name = monitoring_infos.parse_namespace_and_name(input)
    self.assertEqual(namespace, "counternamespace")
    self.assertEqual(name, "countername")


if __name__ == '__main__':
  unittest.main()
