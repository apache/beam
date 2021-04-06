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

# pytype: skip-file

import argparse
import logging
from random import randrange

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.pvalue import AsList
from apache_beam.pvalue import AsSingleton


def create_groups(group_ids, corpus, word, ignore_corpus, ignore_word):
  """Generate groups given the input PCollections."""
  def attach_corpus_fn(group, corpus, ignore):
    selected = None
    len_corpus = len(corpus)
    while not selected:
      c = list(corpus[randrange(0, len_corpus)].values())[0]
      if c != ignore:
        selected = c

    yield (group, selected)

  def attach_word_fn(group, words, ignore):
    selected = None
    len_words = len(words)
    while not selected:
      c = list(words[randrange(0, len_words)].values())[0]
      if c != ignore:
        selected = c

    yield group + (selected, )

  return (
      group_ids
      | 'attach corpus' >> beam.FlatMap(
          attach_corpus_fn, AsList(corpus), AsSingleton(ignore_corpus))
      | 'attach word' >> beam.FlatMap(
          attach_word_fn, AsList(word), AsSingleton(ignore_word)))


def run(argv=None):
  """Run the workflow."""

  parser = argparse.ArgumentParser()
  parser.add_argument('--output')
  parser.add_argument('--ignore_corpus', default='')
  parser.add_argument('--ignore_word', default='')
  parser.add_argument('--num_groups')

  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  with beam.Pipeline(options=pipeline_options) as p:

    group_ids = []
    for i in range(0, int(known_args.num_groups)):
      group_ids.append('id' + str(i))

    query_corpus = 'select UNIQUE(corpus) from publicdata:samples.shakespeare'
    query_word = 'select UNIQUE(word) from publicdata:samples.shakespeare'
    ignore_corpus = known_args.ignore_corpus
    ignore_word = known_args.ignore_word

    pcoll_corpus = p | 'read corpus' >> beam.io.ReadFromBigQuery(
        query=query_corpus)
    pcoll_word = p | 'read_words' >> beam.io.ReadFromBigQuery(query=query_word)
    pcoll_ignore_corpus = p | 'create_ignore_corpus' >> beam.Create(
        [ignore_corpus])
    pcoll_ignore_word = p | 'create_ignore_word' >> beam.Create([ignore_word])
    pcoll_group_ids = p | 'create groups' >> beam.Create(group_ids)

    pcoll_groups = create_groups(
        pcoll_group_ids,
        pcoll_corpus,
        pcoll_word,
        pcoll_ignore_corpus,
        pcoll_ignore_word)

    # pylint:disable=expression-not-assigned
    pcoll_groups | WriteToText(known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
