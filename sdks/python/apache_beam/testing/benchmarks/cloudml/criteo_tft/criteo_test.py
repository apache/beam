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

import unittest

try:
  import tensorflow as tf

  from apache_beam.testing.benchmarks.cloudml.criteo_tft import criteo
except ImportError:
  raise unittest.SkipTest('Dependencies are not installed')


class CriteoTest(tf.test.TestCase):
  def test_fill_in_missing_int_feature(self):
    feature = tf.SparseTensor(
        indices=[[0, 0], [2, 0]],
        values=tf.constant([10, 30], dtype=tf.int64),
        dense_shape=[3, 1])

    result = criteo.fill_in_missing(feature, -1)

    self.assertAllEqual(result, [10, -1, 30])
    self.assertEqual(result.shape.rank, 1)

  def test_fill_in_missing_int_feature_traces_with_dynamic_shape(self):
    @tf.function(
        input_signature=[
            tf.SparseTensorSpec(shape=[None, None], dtype=tf.int64)
        ])
    def fill_in_missing(feature):
      return criteo.fill_in_missing(feature, -1)

    feature = tf.SparseTensor(
        indices=[[0, 0], [2, 0]],
        values=tf.constant([10, 30], dtype=tf.int64),
        dense_shape=[3, 1])

    result = fill_in_missing(feature)

    self.assertAllEqual(result, [10, -1, 30])
    self.assertEqual(result.shape.rank, 1)

  def test_fill_in_missing_all_missing_int_feature(self):
    feature = tf.SparseTensor(
        indices=tf.zeros([0, 2], dtype=tf.int64),
        values=tf.constant([], dtype=tf.int64),
        dense_shape=[3, 0])

    result = criteo.fill_in_missing(feature, -1)

    self.assertAllEqual(result, [-1, -1, -1])
    self.assertEqual(result.shape.rank, 1)

  def test_fill_in_missing_string_feature(self):
    feature = tf.SparseTensor(
        indices=[[0, 0], [2, 0]],
        values=tf.constant(['a', 'c'], dtype=tf.string),
        dense_shape=[3, 1])

    result = criteo.fill_in_missing(feature, '')

    self.assertAllEqual(result, [b'a', b'', b'c'])
    self.assertEqual(result.shape.rank, 1)

  def test_fill_in_missing_all_missing_string_feature(self):
    feature = tf.SparseTensor(
        indices=tf.zeros([0, 2], dtype=tf.int64),
        values=tf.constant([], dtype=tf.string),
        dense_shape=[3, 0])

    result = criteo.fill_in_missing(feature, '')

    self.assertAllEqual(result, [b'', b'', b''])
    self.assertEqual(result.shape.rank, 1)


if __name__ == '__main__':
  unittest.main()
