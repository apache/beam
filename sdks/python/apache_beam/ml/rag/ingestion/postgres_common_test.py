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

import unittest

from apache_beam.ml.rag.ingestion.postgres_common import ConflictResolution


class ConflictResolutionTest(unittest.TestCase):
  def test_update_with_empty_conflict_fields_raises(self):
    resolution = ConflictResolution(on_conflict_fields=[], action="UPDATE")
    resolution.maybe_set_default_update_fields(["id", "embedding"])
    with self.assertRaises(ValueError):
      resolution.get_conflict_clause()

  def test_update_with_str_conflict_field(self):
    resolution = ConflictResolution(on_conflict_fields="id", action="UPDATE")
    resolution.maybe_set_default_update_fields(["id", "embedding"])
    self.assertEqual(
        resolution.get_conflict_clause(),
        "ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding")

  def test_update_with_list_conflict_fields(self):
    resolution = ConflictResolution(
        on_conflict_fields=["id", "source"], action="UPDATE")
    resolution.maybe_set_default_update_fields(
        ["id", "source", "embedding"])
    self.assertEqual(
        resolution.get_conflict_clause(),
        "ON CONFLICT (id, source) DO UPDATE SET "
        "embedding = EXCLUDED.embedding")

  def test_ignore_with_empty_conflict_fields(self):
    resolution = ConflictResolution(on_conflict_fields=[], action="IGNORE")
    self.assertEqual(
        resolution.get_conflict_clause(), "ON CONFLICT DO NOTHING")


if __name__ == "__main__":
  unittest.main()
  
