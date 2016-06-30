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

"""A Dataflow job that uses BigQuery sources as a side inputs.

Illustrates how to insert side-inputs into transforms in three different forms,
as a singleton, as a iterator, and as a list.

This workflow generate a set of tuples of the form (groupId, corpus, word) where
groupId is a generated identifier for the group and corpus and word are randomly
selected from corresponding rows in BQ dataset 'publicdata:samples.shakespeare'.
Users should specify the number of groups to form and optionally a corpus and/or
a word that should be ignored when forming groups.
"""

import argparse
import logging
from random import randrange

import apache_beam as beam

from apache_beam.pvalue import AsIter
from apache_beam.pvalue import AsList
from apache_beam.pvalue import AsSingleton


def create_groups(group_ids, corpus, word, ignore_corpus, ignore_word):
  """Generate groups given the input PCollections."""

  def attach_corpus_fn(group, corpus, ignore):
    selected = None
    len_corpus = len(corpus)
    while not selected:
      c = corpus[randrange(0, len_corpus - 1)].values()[0]
      if c != ignore:
        selected = c

    yield (group, selected)

  def attach_word_fn(group, words, ignore):
    selected = None
    len_words = len(words)
    while not selected:
      c = words[randrange(0, len_words - 1)].values()[0]
      if c != ignore:
        selected = c

    yield group + (selected,)

  return (group_ids
          | beam.FlatMap(
              'attach corpus',
              attach_corpus_fn,
              AsList(corpus),
              AsSingleton(ignore_corpus))
          | beam.FlatMap(
              'attach word',
              attach_word_fn,
              AsIter(word),
              AsSingleton(ignore_word)))


def run(argv=None):
  """Run the workflow."""

  parser = argparse.ArgumentParser()
  parser.add_argument('--output')
  parser.add_argument('--ignore_corpus', default='')
  parser.add_argument('--ignore_word', default='')
  parser.add_argument('--num_groups')

  known_args, pipeline_args = parser.parse_known_args(argv)
  p = beam.Pipeline(argv=pipeline_args)

  group_ids = []
  for i in xrange(0, int(known_args.num_groups)):
    group_ids.append('id' + str(i))

  query_corpus = 'select UNIQUE(corpus) from publicdata:samples.shakespeare'
  query_word = 'select UNIQUE(word) from publicdata:samples.shakespeare'
  ignore_corpus = known_args.ignore_corpus
  ignore_word = known_args.ignore_word

  pcoll_corpus = p | beam.Read('read corpus',
                               beam.io.BigQuerySource(query=query_corpus))
  pcoll_word = p | beam.Read('read words',
                             beam.io.BigQuerySource(query=query_word))
  pcoll_ignore_corpus = p | beam.Create('create_ignore_corpus', [ignore_corpus])
  pcoll_ignore_word = p | beam.Create('create_ignore_word', [ignore_word])
  pcoll_group_ids = p | beam.Create('create groups', group_ids)

  pcoll_groups = create_groups(pcoll_group_ids, pcoll_corpus, pcoll_word,
                               pcoll_ignore_corpus, pcoll_ignore_word)

  # pylint:disable=expression-not-assigned
  pcoll_groups | beam.io.Write('WriteToText',
                               beam.io.TextFileSink(known_args.output))
  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
