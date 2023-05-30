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
import pytest
import numpy as np
try:
  import tensorflow_transform as tft
  import tensorflow as tf
  from apache_beam.testing.benchmarks.cloudml.criteo_tft.criteo import fill_in_missing
except ImportError:
  tft = None
  tf=None
  fill_in_missing=None

@pytest.mark.uses_tft
@unittest.skipIf(tft is None or tf is None, 'Missing dependencies. ')
class FillInMissingTest(unittest.TestCase):
  def test_fill_in_missing(self):
    # Create a rank 2 sparse tensor with missing values
    indices = np.array([[0, 0], [0, 2], [1, 1], [2, 0]])
    values = np.array([1, 2, 3, 4])
    dense_shape = np.array([3, 3])
    sparse_tensor = tf.sparse.SparseTensor(indices, values, dense_shape)

    # Fill in missing values with -1
    filled_tensor=[]
    if fill_in_missing!=None:
      filled_tensor = fill_in_missing(sparse_tensor, -1)

    # Convert to a dense tensor and check the values
    expected_output = np.array([1, -1, 2, -1, -1, -1, 4, -1, -1])
    actual_output = filled_tensor.numpy()
    self.assertEqual(expected_output, actual_output)
