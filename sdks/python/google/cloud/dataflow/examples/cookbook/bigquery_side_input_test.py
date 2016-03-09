# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test for the BigQuery side input example."""

import logging
import unittest

import google.cloud.dataflow as df
from google.cloud.dataflow.examples.cookbook import bigquery_side_input


class BigQuerySideInputTest(unittest.TestCase):

  def test_create_groups(self):
    p = df.Pipeline('DirectPipelineRunner')

    group_ids_pcoll = p | df.Create('create_group_ids', ['A', 'B', 'C'])
    corpus_pcoll = p | df.Create('create_corpus',
                                 [{'f': 'corpus1'},
                                  {'f': 'corpus2'},
                                  {'f': 'corpus3'}])
    words_pcoll = p | df.Create('create_words', [{'f': 'word1'},
                                                 {'f': 'word2'},
                                                 {'f': 'word3'}])
    ignore_corpus_pcoll = p | df.Create('create_ignore_corpus', ['corpus1'])
    ignore_word_pcoll = p | df.Create('create_ignore_word', ['word1'])

    groups = bigquery_side_input.create_groups(group_ids_pcoll, corpus_pcoll,
                                               words_pcoll, ignore_corpus_pcoll,
                                               ignore_word_pcoll)

    def group_matcher(actual):
      self.assertEqual(len(actual), 3)
      for group in actual:
        self.assertEqual(len(group), 3)
        self.assertTrue(group[1].startswith('corpus'))
        self.assertNotEqual(group[1], 'corpus1')
        self.assertTrue(group[2].startswith('word'))
        self.assertNotEqual(group[2], 'word1')

    df.assert_that(groups, group_matcher)
    p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
