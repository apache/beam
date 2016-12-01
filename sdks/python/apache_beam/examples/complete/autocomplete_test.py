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

"""Test for the autocomplete example."""

import unittest

import apache_beam as beam
from apache_beam.examples.complete import autocomplete
from apache_beam.transforms.util import assert_that
from apache_beam.transforms.util import equal_to


class AutocompleteTest(unittest.TestCase):

  WORDS = ['this', 'this', 'that', 'to', 'to', 'to']

  def test_top_prefixes(self):
    p = beam.Pipeline('DirectPipelineRunner')
    words = p | beam.Create(self.WORDS)
    result = words | autocomplete.TopPerPrefix(5)
    # values must be hashable for now
    result = result | beam.Map(lambda (k, vs): (k, tuple(vs)))
    assert_that(result, equal_to(
        [
            ('t', ((3, 'to'), (2, 'this'), (1, 'that'))),
            ('to', ((3, 'to'), )),
            ('th', ((2, 'this'), (1, 'that'))),
            ('thi', ((2, 'this'), )),
            ('this', ((2, 'this'), )),
            ('tha', ((1, 'that'), )),
            ('that', ((1, 'that'), )),
        ]))
    p.run()

if __name__ == '__main__':
  unittest.main()
