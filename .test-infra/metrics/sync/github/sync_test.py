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
'''Tests for syncing metrics related data from GitHub.'''

import unittest
from ddt import ddt, data
import ghutilities


@ddt
class GhutilitiesTestCase(unittest.TestCase):

  @data(("sample text with mention @mention", ["mention"]),
        ("Data without mention", []),
        ("sample text with several mentions @first, @second @third", ["first", "second", "third"]))
  def test_findMentions_finds_mentions_by_pattern(self, params):
    input, expectedResult = params
    result = ghutilities.findMentions(input)
    self.assertEqual(expectedResult, result)

  def test_findCommentReviewers(self):
    result = "some tesxt \n body"

if __name__ == '__main__':
  unittest.main()
