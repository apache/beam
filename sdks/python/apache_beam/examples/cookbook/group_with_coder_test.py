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

"""Test for the custom coders example."""

import logging
import tempfile
import unittest

from apache_beam.examples.cookbook import group_with_coder


# Patch group_with_coder.PlayerCoder.decode(). To test that the PlayerCoder was
# used, we do not strip the prepended 'x:' string when decoding a Player object.
group_with_coder.PlayerCoder.decode = lambda self, s: group_with_coder.Player(s)


class GroupWithCoderTest(unittest.TestCase):

  SAMPLE_RECORDS = [
      'joe,10', 'fred,3', 'mary,7',
      'joe,20', 'fred,6', 'ann,5',
      'joe,30', 'ann,10', 'mary,1']

  def create_temp_file(self, records):
    with tempfile.NamedTemporaryFile(delete=False) as f:
      for record in records:
        f.write('%s\n' % record)
      return f.name

  def test_basics_with_type_check(self):
    # Run the workflow with --pipeline_type_check option. This will make sure
    # the typehints associated with all transforms will have non-default values
    # and therefore any custom coders will be used. In our case we want to make
    # sure the coder for the Player class will be used.
    temp_path = self.create_temp_file(self.SAMPLE_RECORDS)
    group_with_coder.run([
        '--pipeline_type_check',
        '--input=%s*' % temp_path,
        '--output=%s.result' % temp_path])
    # Parse result file and compare.
    results = []
    with open(temp_path + '.result-00000-of-00001') as result_file:
      for line in result_file:
        name, points = line.split(',')
        results.append((name, int(points)))
      logging.info('result: %s', results)
    self.assertEqual(
        sorted(results),
        sorted([('x:ann', 15), ('x:fred', 9), ('x:joe', 60), ('x:mary', 8)]))

  def test_basics_without_type_check(self):
    # Run the workflow without --pipeline_type_check option. This will make sure
    # the typehints associated with all transforms will have default values and
    # therefore any custom coders will not be used. The default coder (pickler)
    # will be used instead.
    temp_path = self.create_temp_file(self.SAMPLE_RECORDS)
    group_with_coder.run([
        '--no_pipeline_type_check',
        '--input=%s*' % temp_path,
        '--output=%s.result' % temp_path])
    # Parse result file and compare.
    results = []
    with open(temp_path + '.result-00000-of-00001') as result_file:
      for line in result_file:
        name, points = line.split(',')
        results.append((name, int(points)))
      logging.info('result: %s', results)
    self.assertEqual(
        sorted(results),
        sorted([('ann', 15), ('fred', 9), ('joe', 60), ('mary', 8)]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
