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

# pytype: skip-file

import argparse
import logging
import time

from pymongo import MongoClient

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

_LOGGER = logging.getLogger(__name__)


class GenerateDocs(beam.DoFn):
  def process(self, num_docs, *args, **kwargs):
    for i in range(num_docs):
      yield {'number': i, 'number_mod_2': i % 2, 'number_mod_3': i % 3}


def run(argv=None):
  default_db = 'beam_mongodbio_it_db'
  default_coll = 'integration_test_%d' % time.time()
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--mongo_uri',
      default='mongodb://localhost:27017',
      help='mongo uri string for connection')
  parser.add_argument(
      '--mongo_db', default=default_db, help='mongo uri string for connection')
  parser.add_argument(
      '--mongo_coll',
      default=default_coll,
      help='mongo uri string for connection')
  parser.add_argument(
      '--num_documents',
      default=100000,
      help='The expected number of documents to be generated '
      'for write or read',
      type=int)
  parser.add_argument(
      '--batch_size',
      default=10000,
      type=int,
      help=('batch size for writing to mongodb'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  # Test Write to MongoDB
  with TestPipeline(options=PipelineOptions(pipeline_args)) as p:
    start_time = time.time()
    _LOGGER.info('Writing %d documents to mongodb', known_args.num_documents)

    _ = (
        p | beam.Create([known_args.num_documents])
        | 'Create documents' >> beam.ParDo(GenerateDocs())
        | 'WriteToMongoDB' >> beam.io.WriteToMongoDB(
            known_args.mongo_uri,
            known_args.mongo_db,
            known_args.mongo_coll,
            known_args.batch_size))
  elapsed = time.time() - start_time
  _LOGGER.info(
      'Writing %d documents to mongodb finished in %.3f seconds' %
      (known_args.num_documents, elapsed))

  # Test Read from MongoDB
  total_sum = sum(range(known_args.num_documents))
  mod_3_sum = sum(
      num for num in range(known_args.num_documents) if num % 3 == 0)
  mod_3_count = sum(
      1 for num in range(known_args.num_documents) if num % 3 == 0)
  # yapf: disable
  read_cases = [
      # (reader_params, expected)
      (
          {
              'projection': ['number']
          },
          {
              'number_sum': total_sum,
              'docs_count': known_args.num_documents
          }
      ),
      (
          {
              'filter': {'number_mod_3': 0},
              'projection': ['number']
          },
          {
              'number_sum': mod_3_sum,
              'docs_count': mod_3_count
          }
      ),
      (
          {
              'projection': ['number'],
              'bucket_auto': True
          },
          {
              'number_sum': total_sum,
              'docs_count': known_args.num_documents
          }
      ),
      (
          {
              'filter': {'number_mod_3': 0},
              'projection': ['number'],
              'bucket_auto': True
          },
          {
              'number_sum': mod_3_sum,
              'docs_count': mod_3_count
          }
      ),
  ]
  # yapf: enable
  for reader_params, expected in read_cases:
    with TestPipeline(options=PipelineOptions(pipeline_args)) as p:
      start_time = time.time()
      _LOGGER.info('=' * 80)
      _LOGGER.info(
          'Reading from mongodb %s:%s',
          known_args.mongo_db,
          known_args.mongo_coll)
      _LOGGER.info('reader params   : %s', reader_params)
      _LOGGER.info('expected results: %s', expected)
      docs = (
          p | 'ReadFromMongoDB' >> beam.io.ReadFromMongoDB(
              known_args.mongo_uri,
              known_args.mongo_db,
              known_args.mongo_coll,
              **reader_params)
          | 'Map' >> beam.Map(lambda doc: doc['number']))
      number_sum = (docs | 'Combine' >> beam.CombineGlobally(sum))
      docs_count = (docs | 'Count' >> beam.combiners.Count.Globally())
      r = ([number_sum, docs_count] | 'Flatten' >> beam.Flatten())
      assert_that(r, equal_to([expected['number_sum'], expected['docs_count']]))

    elapsed = time.time() - start_time
    _LOGGER.info(
        'Reading documents from mongodb finished in %.3f seconds', elapsed)

  # Clean-up
  with MongoClient(host=known_args.mongo_uri) as client:
    client.drop_database(known_args.mongo_db)


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
