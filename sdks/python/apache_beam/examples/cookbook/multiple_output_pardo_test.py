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

"""Test for the multiple_output_pardo example."""

import logging
import unittest

import apache_beam as beam
from apache_beam.examples.cookbook import multiple_output_pardo
#from apache_beam.transforms.util import assert_that
#from apache_beam.transforms.util import DataflowAssertException

class MultipleOutputParDoTest(unittest.TestCase):

  SAMPLE_TEXT = 'A whole new world\nA new point'
  text_len = len(' '.join(SAMPLE_TEXT.split('\n')))
  SAMPLE_TEXT_Iterable = [SAMPLE_TEXT]
  EXPECTED_SHORT_WORDS = ['A: 2', 'new: 2']
  EXP_WORDS = ['point: 1', 'whole: 1', 'world: 1']

  def test_multiple_output_pardo(self):
    p = beam.Pipeline('DirectPipelineRunner')
    sample_text = p | beam.Create(self.SAMPLE_TEXT_Iterable)
    res = (sample_text
           | beam.ParDo(multiple_output_pardo.SplitLinesToWordsFn())
           .with_outputs('tag_short_words','tag_character_count',main='words'))
    res_cnt = (res.tag_character_count
               | 'pair_with_key' >> beam.Map(lambda x: ('chars_temp_key', x))
               | beam.GroupByKey()
               | 'count chars' >> beam.Map(lambda (_, counts): sum(counts)))
    res_words = (res.words
                 | 'count words' >> multiple_output_pardo.CountWords())
    res_sh_wrd = (res.tag_short_words
                  | 'count short words' >> multiple_output_pardo.CountWords())
    beam.assert_that(res_words, beam.equal_to(self.EXP_WORDS))
    beam.assert_that(res_sh_wrd, beam.equal_to(self.EXPECTED_SHORT_WORDS),
                     label='assert:tag_short_words')
    beam.assert_that(res_cnt, beam.equal_to([self.text_len]),
                     label='assert:tag_character_count')
    p.run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
