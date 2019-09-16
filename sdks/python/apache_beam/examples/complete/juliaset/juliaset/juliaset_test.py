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

"""Test for the juliaset example."""

from __future__ import absolute_import

import logging
import os
import re
import tempfile
import unittest

from apache_beam.examples.complete.juliaset.juliaset import juliaset
from apache_beam.testing.util import open_shards


class JuliaSetTest(unittest.TestCase):

  def setUp(self):
    self.test_files = {}
    self.test_files['output_coord_file_name'] = self.generate_temp_file()
    self.test_files['output_image_file_name'] = self.generate_temp_file()

  def tearDown(self):
    for test_file in self.test_files.values():
      if os.path.exists(test_file):
        os.remove(test_file)

  def generate_temp_file(self):
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
      return temp_file.name

  def run_example(self, grid_size, image_file_name=None):
    args = [
        '--coordinate_output=%s' % self.test_files['output_coord_file_name'],
        '--grid_size=%s' % grid_size,
    ]

    if image_file_name is not None:
      args.append('--image_output=%s' % image_file_name)

    juliaset.run(args)

  def test_output_file_format(self):
    grid_size = 5
    self.run_example(grid_size)

    # Parse the results from the file, and ensure it was written in the proper
    # format.
    with open_shards(self.test_files['output_coord_file_name'] +
                     '-*-of-*') as result_file:
      output_lines = result_file.readlines()

      # Should have a line for each x-coordinate.
      self.assertEqual(grid_size, len(output_lines))
      for line in output_lines:
        coordinates = re.findall(r'(\(\d+, \d+, \d+\))', line)

        # Should have 5 coordinates on each line.
        self.assertTrue(coordinates)
        self.assertEqual(grid_size, len(coordinates))

  def test_generate_fractal_image(self):
    temp_image_file = self.test_files['output_image_file_name']
    self.run_example(10, image_file_name=temp_image_file)

    # Ensure that the image was saved properly.
    # TODO(silviuc): Reactivate the test when --image_output is supported.
    # self.assertTrue(os.stat(temp_image_file).st_size > 0)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
