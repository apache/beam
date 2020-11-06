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

from __future__ import absolute_import

import unittest

import apache_beam as beam
from apache_beam.runners.interactive.derivation_tree import DerivationTree


class DerivationTreeTest(unittest.TestCase):
  def test_getting_unknown_pid_returns_none(self):
    dt = DerivationTree()

    p = beam.Pipeline()

    self.assertIsNone(dt.get_pipeline(str(id(p))))

  def test_getting_unknown_pipeline_returns_none(self):
    dt = DerivationTree()

    p = beam.Pipeline()

    self.assertIsNone(dt.get_user_pipeline(p))

  def test_no_parent_returns_none(self):
    dt = DerivationTree()

    user = beam.Pipeline()
    derived = beam.Pipeline()
    orphan = beam.Pipeline()

    dt.add_derived_pipeline(user, derived)

    self.assertIsNone(dt.get_user_pipeline(orphan))

  def test_get_user_pipeline_is_same(self):
    dt = DerivationTree()

    p = beam.Pipeline()
    dt.add_user_pipeline(p)

    self.assertIs(dt.get_user_pipeline(p), p)

  def test_can_add_derived(self):
    dt = DerivationTree()

    user = beam.Pipeline()
    derived = beam.Pipeline()

    dt.add_derived_pipeline(user, derived)

    self.assertIs(dt.get_user_pipeline(derived), user)

  def test_can_add_multiple_derived(self):
    """Tests that there can be many user pipelines with many derived
    pipelines.
    """
    dt = DerivationTree()

    # Add the first set of user and derived pipelines.
    user1 = beam.Pipeline()
    derived11 = beam.Pipeline()
    derived12 = beam.Pipeline()

    dt.add_derived_pipeline(user1, derived11)
    dt.add_derived_pipeline(user1, derived12)

    # Add the second set of user and derived pipelines.
    user2 = beam.Pipeline()
    derived21 = beam.Pipeline()
    derived22 = beam.Pipeline()

    dt.add_derived_pipeline(user2, derived21)
    dt.add_derived_pipeline(user2, derived22)

    # Assert that the user pipelines are correct.
    self.assertIs(dt.get_user_pipeline(derived11), user1)
    self.assertIs(dt.get_user_pipeline(derived12), user1)
    self.assertIs(dt.get_user_pipeline(derived21), user2)
    self.assertIs(dt.get_user_pipeline(derived22), user2)

  def test_cannot_have_multiple_parents(self):
    dt = DerivationTree()

    user1 = beam.Pipeline()
    user2 = beam.Pipeline()
    derived = beam.Pipeline()

    dt.add_derived_pipeline(user1, derived)
    dt.add_derived_pipeline(user2, derived)

    self.assertIs(dt.get_user_pipeline(derived), user1)

  def test_adding_derived_with_derived_gets_user_pipeline(self):
    """Tests that one can correctly add a derived pipeline from a derived
    pipeline and still get the correct user pipeline.
    """
    dt = DerivationTree()

    user = beam.Pipeline()
    derived1 = beam.Pipeline()
    derived2 = beam.Pipeline()

    # Add the first derived pipeline to the user pipelne.
    dt.add_derived_pipeline(user, derived1)

    # Add the second derived pipeline to the first derived pipeline. This should
    # get the user pipeline of the first and add the second to it.
    dt.add_derived_pipeline(derived1, derived2)

    # Asserts that both derived pipelines are under the same user pipeline.
    self.assertIs(dt.get_user_pipeline(derived1), user)
    self.assertIs(dt.get_user_pipeline(derived2), user)

  def test_can_get_pipeline_from_id(self):
    """Tests the pid -> pipeline memoization."""
    dt = DerivationTree()

    user = beam.Pipeline()
    derived = beam.Pipeline()

    dt.add_user_pipeline(user)
    dt.add_derived_pipeline(user, derived)

    self.assertIs(dt.get_pipeline(str(id(user))), user)
    self.assertIs(dt.get_pipeline(str(id(derived))), derived)

  def test_clear(self):
    dt = DerivationTree()

    user = beam.Pipeline()
    derived = beam.Pipeline()

    dt.add_derived_pipeline(user, derived)

    self.assertIs(dt.get_user_pipeline(derived), user)

    dt.clear()

    self.assertIsNone(dt.get_user_pipeline(user))
    self.assertIsNone(dt.get_user_pipeline(derived))

  def test_can_iterate(self):
    dt = DerivationTree()

    user1 = beam.Pipeline()
    derived11 = beam.Pipeline()
    derived12 = beam.Pipeline()

    dt.add_derived_pipeline(user1, derived11)
    dt.add_derived_pipeline(user1, derived12)

    user2 = beam.Pipeline()
    derived21 = beam.Pipeline()
    derived22 = beam.Pipeline()

    dt.add_derived_pipeline(user2, derived21)
    dt.add_derived_pipeline(user2, derived22)

    user_pipelines = set(p for p in dt)
    self.assertSetEqual(set([user1, user2]), user_pipelines)


if __name__ == '__main__':
  unittest.main()
