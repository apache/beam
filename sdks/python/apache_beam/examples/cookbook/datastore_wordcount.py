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

"""A word-counting workflow that uses Google Cloud Datastore.

This example shows how to use ``datastoreio`` to read from and write to
Google Cloud Datastore. Note that running this example may incur charge for
Cloud Datastore operations.

See https://developers.google.com/datastore/ for more details on Google Cloud
Datastore.
See http://beam.incubator.apache.org/get-started/quickstart on
how to run a Beam pipeline.

Read-only Mode: In this mode, this example reads Cloud Datastore entities using
the ``datastoreio.ReadFromDatastore`` transform, extracts the words,
counts them and write the output to a set of files.

The following options must be provided to run this pipeline in read-only mode:
``
--project YOUR_PROJECT_ID
--kind YOUR_DATASTORE_KIND
--output [YOUR_LOCAL_FILE *or* gs://YOUR_OUTPUT_PATH]
--read-only
``

Read-write Mode: In this mode, this example reads words from an input file,
converts them to Cloud Datastore ``Entity`` objects and writes them to
Cloud Datastore using the ``datastoreio.Write`` transform. The second pipeline
will then read these Cloud Datastore entities using the
``datastoreio.ReadFromDatastore`` transform, extract the words, count them and
write the output to a set of files.

The following options must be provided to run this pipeline in read-write mode:
``
--project YOUR_PROJECT_ID
--kind YOUR_DATASTORE_KIND
--output [YOUR_LOCAL_FILE *or* gs://YOUR_OUTPUT_PATH]
``

Note: We are using the Cloud Datastore protobuf objects directly because
that is the interface that the ``datastoreio`` exposes.
See the following links on more information about these protobuf messages.
https://cloud.google.com/datastore/docs/reference/rpc/google.datastore.v1 and
https://github.com/googleapis/googleapis/tree/master/google/datastore/v1
"""

from __future__ import absolute_import

import argparse
import logging
import re
import uuid

from google.datastore.v1 import entity_pb2
from google.datastore.v1 import query_pb2
from googledatastore import helper as datastore_helper, PropertyFilter

import apache_beam as beam
from apache_beam.io.datastore.v1.datastoreio import ReadFromDatastore
from apache_beam.io.datastore.v1.datastoreio import WriteToDatastore
from apache_beam.utils.options import GoogleCloudOptions
from apache_beam.utils.options import PipelineOptions
from apache_beam.utils.options import SetupOptions

empty_line_aggregator = beam.Aggregator('emptyLines')
average_word_size_aggregator = beam.Aggregator('averageWordLength',
                                               beam.combiners.MeanCombineFn(),
                                               float)


class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""

  def process(self, context):
    """Returns an iterator over words in contents of Cloud Datastore entity.
    The element is a line of text.  If the line is blank, note that, too.
    Args:
      context: the call-specific context: data and aggregator.
    Returns:
      The processed element.
    """
    content_value = context.element.properties.get('content', None)
    text_line = ''
    if content_value:
      text_line = content_value.string_value

    if not text_line:
      context.aggregate_to(empty_line_aggregator, 1)
    words = re.findall(r'[A-Za-z\']+', text_line)
    for w in words:
      context.aggregate_to(average_word_size_aggregator, len(w))
    return words


class EntityWrapper(object):
  """Create a Cloud Datastore entity from the given string."""
  def __init__(self, namespace, kind, ancestor):
    self._namespace = namespace
    self._kind = kind
    self._ancestor = ancestor

  def make_entity(self, content):
    entity = entity_pb2.Entity()
    if self._namespace is not None:
      entity.key.partition_id.namespace_id = self._namespace

    # All entities created will have the same ancestor
    datastore_helper.add_key_path(entity.key, self._kind, self._ancestor,
                                  self._kind, str(uuid.uuid4()))

    datastore_helper.add_properties(entity, {"content": unicode(content)})
    return entity


def write_to_datastore(project, user_options, pipeline_options):
  """Creates a pipeline that writes entities to Cloud Datastore."""
  p = beam.Pipeline(options=pipeline_options)

  # pylint: disable=expression-not-assigned
  (p
   | 'read' >> beam.io.Read(beam.io.TextFileSource(user_options.input))
   | 'create entity' >> beam.Map(
       EntityWrapper(user_options.namespace, user_options.kind,
                     user_options.ancestor).make_entity)
   | 'write to datastore' >> WriteToDatastore(project))

  # Actually run the pipeline (all operations above are deferred).
  p.run()


def make_ancestor_query(kind, namespace, ancestor):
  """Creates a Cloud Datastore ancestor query.

  The returned query will fetch all the entities that have the parent key name
  set to the given `ancestor`.
  """
  ancestor_key = entity_pb2.Key()
  datastore_helper.add_key_path(ancestor_key, kind, ancestor)
  if namespace is not None:
    ancestor_key.partition_id.namespace_id = namespace

  query = query_pb2.Query()
  query.kind.add().name = kind

  datastore_helper.set_property_filter(
      query.filter, '__key__', PropertyFilter.HAS_ANCESTOR, ancestor_key)

  return query


def read_from_datastore(project, user_options, pipeline_options):
  """Creates a pipeline that reads entities from Cloud Datastore."""
  p = beam.Pipeline(options=pipeline_options)
  # Create a query to read entities from datastore.
  query = make_ancestor_query(user_options.kind, user_options.namespace,
                              user_options.ancestor)

  # Read entities from Cloud Datastore into a PCollection.
  lines = p | 'read from datastore' >> ReadFromDatastore(
      project, query, user_options.namespace)

  # Count the occurrences of each word.
  counts = (lines
            | 'split' >> (beam.ParDo(WordExtractingDoFn())
                          .with_output_types(unicode))
            | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
            | 'group' >> beam.GroupByKey()
            | 'count' >> beam.Map(lambda (word, ones): (word, sum(ones))))

  # Format the counts into a PCollection of strings.
  output = counts | 'format' >> beam.Map(lambda (word, c): '%s: %s' % (word, c))

  # Write the output using a "Write" transform that has side effects.
  # pylint: disable=expression-not-assigned
  output | 'write' >> beam.io.WriteToText(file_path_prefix=user_options.output,
                                          num_shards=user_options.num_shards)

  # Actually run the pipeline (all operations above are deferred).
  return p.run()


def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://dataflow-samples/shakespeare/kinglear.txt',
                      help='Input file to process.')
  parser.add_argument('--kind',
                      dest='kind',
                      required=True,
                      help='Datastore Kind')
  parser.add_argument('--namespace',
                      dest='namespace',
                      help='Datastore Namespace')
  parser.add_argument('--ancestor',
                      dest='ancestor',
                      default='root',
                      help='The ancestor key name for all entities.')
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')
  parser.add_argument('--read_only',
                      action='store_true',
                      help='Read an existing dataset, do not write first')
  parser.add_argument('--num_shards',
                      dest='num_shards',
                      type=int,
                      # If the system should choose automatically.
                      default=0,
                      help='Number of output shards')

  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  gcloud_options = pipeline_options.view_as(GoogleCloudOptions)

  # Write to Datastore if `read_only` options is not specified.
  if not known_args.read_only:
    write_to_datastore(gcloud_options.project, known_args, pipeline_options)

  # Read entities from Datastore.
  result = read_from_datastore(gcloud_options.project, known_args,
                               pipeline_options)

  empty_line_values = result.aggregated_values(empty_line_aggregator)
  logging.info('number of empty lines: %d', sum(empty_line_values.values()))
  word_length_values = result.aggregated_values(average_word_size_aggregator)
  logging.info('average word lengths: %s', word_length_values.values())

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
