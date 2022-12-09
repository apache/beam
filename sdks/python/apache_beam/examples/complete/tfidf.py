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

"""A TF-IDF workflow (term frequency - inverse document frequency).

For an explanation of the TF-IDF algorithm see the following link:
http://en.wikipedia.org/wiki/Tf-idf
"""

# pytype: skip-file

import argparse
import math
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.pvalue import AsSingleton


def read_documents(pipeline, uris):
  """Read the documents at the provided uris and returns (uri, line) pairs."""
  pcolls = []
  for uri in uris:
    pcolls.append(
        pipeline
        | 'Read: %s' % uri >> ReadFromText(uri)
        | 'WithKey: %s' % uri >> beam.Map(lambda v, uri: (uri, v), uri))
  return pcolls | 'FlattenReadPColls' >> beam.Flatten()


class TfIdf(beam.PTransform):
  """A transform containing a basic TF-IDF pipeline.

  The input consists of KV objects where the key is the document's URI and
  the value is a piece of the document's content.
  The output is mapping from terms to scores for each document URI.
  """
  def expand(self, uri_to_content):

    # Compute the total number of documents, and prepare a singleton
    # PCollection to use as side input.
    total_documents = (
        uri_to_content
        | 'GetUris 1' >> beam.Keys()
        | 'GetUniqueUris' >> beam.Distinct()
        | 'CountUris' >> beam.combiners.Count.Globally())

    # Create a collection of pairs mapping a URI to each of the words
    # in the document associated with that that URI.

    def split_into_words(uri_line):
      (uri, line) = uri_line
      return [(uri, w.lower()) for w in re.findall(r'[A-Za-z\']+', line)]

    uri_to_words = (
        uri_to_content
        | 'SplitWords' >> beam.FlatMap(split_into_words))

    # Compute a mapping from each word to the total number of documents
    # in which it appears.
    word_to_doc_count = (
        uri_to_words
        | 'GetUniqueWordsPerDoc' >> beam.Distinct()
        | 'GetWords' >> beam.Values()
        | 'CountDocsPerWord' >> beam.combiners.Count.PerElement())

    # Compute a mapping from each URI to the total number of words in the
    # document associated with that URI.
    uri_to_word_total = (
        uri_to_words
        | 'GetUris 2' >> beam.Keys()
        | 'CountWordsInDoc' >> beam.combiners.Count.PerElement())

    # Count, for each (URI, word) pair, the number of occurrences of that word
    # in the document associated with the URI.
    uri_and_word_to_count = (
        uri_to_words
        | 'CountWord-DocPairs' >> beam.combiners.Count.PerElement())

    # Adjust the above collection to a mapping from (URI, word) pairs to counts
    # into an isomorphic mapping from URI to (word, count) pairs, to prepare
    # for a join by the URI key.
    def shift_keys(uri_word_count):
      return (uri_word_count[0][0], (uri_word_count[0][1], uri_word_count[1]))

    uri_to_word_and_count = (
        uri_and_word_to_count
        | 'ShiftKeys' >> beam.Map(shift_keys))

    # Perform a CoGroupByKey (a sort of pre-join) on the prepared
    # uri_to_word_total and uri_to_word_and_count tagged by 'word totals' and
    # 'word counts' strings. This yields a mapping from URI to a dictionary
    # that maps the above mentioned tag strings to an iterable containing the
    # word total for that URI and word and count respectively.
    #
    # A diagram (in which '[]' just means 'iterable'):
    #
    #   URI: {'word totals': [count],  # Total words within this URI's document.
    #         'word counts': [(word, count),  # Counts of specific words
    #                         (word, count),  # within this URI's document.
    #                         ... ]}
    uri_to_word_and_count_and_total = ({
        'word totals': uri_to_word_total, 'word counts': uri_to_word_and_count
    }
                                       | 'CoGroupByUri' >> beam.CoGroupByKey())

    # Compute a mapping from each word to a (URI, term frequency) pair for each
    # URI. A word's term frequency for a document is simply the number of times
    # that word occurs in the document divided by the total number of words in
    # the document.

    def compute_term_frequency(uri_count_and_total):
      (uri, count_and_total) = uri_count_and_total
      word_and_count = count_and_total['word counts']
      # We have an iterable for one element that we want extracted.
      [word_total] = count_and_total['word totals']
      for word, count in word_and_count:
        yield word, (uri, float(count) / word_total)

    word_to_uri_and_tf = (
        uri_to_word_and_count_and_total
        | 'ComputeTermFrequencies' >> beam.FlatMap(compute_term_frequency))

    # Compute a mapping from each word to its document frequency.
    # A word's document frequency in a corpus is the number of
    # documents in which the word appears divided by the total
    # number of documents in the corpus.
    #
    # This calculation uses a side input, a Dataflow-computed auxiliary value
    # presented to each invocation of our MapFn lambda. The second argument to
    # the function (called total---note that the first argument is a tuple)
    # receives the value we listed after the lambda in Map(). Additional side
    # inputs (and ordinary Python values, too) can be provided to MapFns and
    # DoFns in this way.
    def div_word_count_by_total(word_count, total):
      (word, count) = word_count
      return (word, float(count) / total)

    word_to_df = (
        word_to_doc_count
        | 'ComputeDocFrequencies' >> beam.Map(
            div_word_count_by_total, AsSingleton(total_documents)))

    # Join the term frequency and document frequency collections,
    # each keyed on the word.
    word_to_uri_and_tf_and_df = ({
        'tf': word_to_uri_and_tf, 'df': word_to_df
    }
                                 | 'CoGroupWordsByTf-df' >> beam.CoGroupByKey())

    # Compute a mapping from each word to a (URI, TF-IDF) score for each URI.
    # There are a variety of definitions of TF-IDF
    # ("term frequency - inverse document frequency") score; here we use a
    # basic version that is the term frequency divided by the log of the
    # document frequency.

    def compute_tf_idf(word_tf_and_df):
      (word, tf_and_df) = word_tf_and_df
      [docf] = tf_and_df['df']
      for uri, tf in tf_and_df['tf']:
        yield word, (uri, tf * math.log(1 / docf))

    word_to_uri_and_tfidf = (
        word_to_uri_and_tf_and_df
        | 'ComputeTf-idf' >> beam.FlatMap(compute_tf_idf))

    return word_to_uri_and_tfidf


def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the tfidf pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--uris', required=True, help='URIs to process.')
  parser.add_argument(
      '--output', required=True, help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  with beam.Pipeline(options=pipeline_options) as p:

    # Read documents specified by the uris command line option.
    metadata_list = FileSystems.match([known_args.uris])[0].metadata_list
    uris = [metadata.path for metadata in metadata_list]
    pcoll = read_documents(p, uris)
    # Compute TF-IDF information for each word.
    output = pcoll | TfIdf()
    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | 'write' >> WriteToText(known_args.output)
    # Execute the pipeline and wait until it is completed.


if __name__ == '__main__':
  run()
