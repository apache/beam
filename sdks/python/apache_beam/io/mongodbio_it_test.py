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

from __future__ import absolute_import

import argparse
import logging
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


def run(argv=None):
  default_db = 'beam_mongodbio_it_db'
  default_coll = 'integration_test_%d' % time.time()
  parser = argparse.ArgumentParser()
  parser.add_argument('--mongo_uri',
                      default='mongodb://localhost:27017',
                      help='mongo uri string for connection')
  parser.add_argument('--mongo_db',
                      default=default_db,
                      help='mongo uri string for connection')
  parser.add_argument('--mongo_coll',
                      default=default_coll,
                      help='mongo uri string for connection')
  parser.add_argument('--num_documents',
                      default=100000,
                      help='The expected number of documents to be generated '
                      'for write or read',
                      type=int)
  parser.add_argument('--batch_size',
                      default=10000,
                      help=('batch size for writing to mongodb'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  # Test Write to MongoDB
  with TestPipeline(options=PipelineOptions(pipeline_args)) as p:
    start_time = time.time()
    logging.info('Writing %d documents to mongodb' % known_args.num_documents)
    docs = [{
        'number': x,
        'number_mod_2': x % 2,
        'number_mod_3': x % 3
    } for x in range(known_args.num_documents)]

    _ = p | 'Create documents' >> beam.Create(docs) \
          | 'WriteToMongoDB' >> beam.io.WriteToMongoDB(known_args.mongo_uri,
                                                       known_args.mongo_db,
                                                       known_args.mongo_coll,
                                                       known_args.batch_size)
  elapsed = time.time() - start_time
  logging.info('Writing %d documents to mongodb finished in %.3f seconds' %
               (known_args.num_documents, elapsed))

  # Test Read from MongoDB
  with TestPipeline(options=PipelineOptions(pipeline_args)) as p:
    start_time = time.time()
    logging.info('Reading from mongodb %s:%s' %
                 (known_args.mongo_db, known_args.mongo_coll))
    r = p | 'ReadFromMongoDB' >> \
                beam.io.ReadFromMongoDB(known_args.mongo_uri,
                                        known_args.mongo_db,
                                        known_args.mongo_coll,
                                        projection=['number']) \
          | 'Map' >> beam.Map(lambda doc: doc['number'])
    assert_that(
        r, equal_to([number for number in range(known_args.num_documents)]))

  elapsed = time.time() - start_time
  logging.info('Read %d documents from mongodb finished in %.3f seconds' %
               (known_args.num_documents, elapsed))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
