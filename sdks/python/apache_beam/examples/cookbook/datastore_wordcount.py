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
See https://beam.apache.org/get-started/quickstart on
how to run a Beam pipeline.

Read-only Mode: In this mode, this example reads Cloud Datastore entities using
the ``datastoreio.ReadFromDatastore`` transform, extracts the words,
counts them and write the output to a set of files.

The following options must be provided to run this pipeline in read-only mode:
``
--project GCP_PROJECT
--kind YOUR_DATASTORE_KIND
--output [YOUR_LOCAL_FILE *or* gs://YOUR_OUTPUT_PATH]
--read_only
``

Read-write Mode: In this mode, this example reads words from an input file,
converts them to Beam ``Entity`` objects and writes them to Cloud Datastore
using the ``datastoreio.WriteToDatastore`` transform. The second pipeline
will then read these Cloud Datastore entities using the
``datastoreio.ReadFromDatastore`` transform, extract the words, count them and
write the output to a set of files.

The following options must be provided to run this pipeline in read-write mode:
``
--project GCP_PROJECT
--kind YOUR_DATASTORE_KIND
--output [YOUR_LOCAL_FILE *or* gs://YOUR_OUTPUT_PATH]
``
"""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import print_function

import argparse
import logging
import re
import sys
from typing import Iterable
from typing import Optional
from typing import Text
import uuid
from builtins import object

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.datastore.v1new.datastoreio import ReadFromDatastore
from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore
from apache_beam.io.gcp.datastore.v1new.types import Entity
from apache_beam.io.gcp.datastore.v1new.types import Key
from apache_beam.io.gcp.datastore.v1new.types import Query
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions


@beam.typehints.with_input_types(Entity)
@beam.typehints.with_output_types(Text)
class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""
  def __init__(self):
    self.empty_line_counter = Metrics.counter('main', 'empty_lines')
    self.word_length_counter = Metrics.counter('main', 'word_lengths')
    self.word_counter = Metrics.counter('main', 'total_words')
    self.word_lengths_dist = Metrics.distribution('main', 'word_len_dist')

  def process(self, element):
    # type: (Entity) -> Optional[Iterable[Text]]

    """Extract words from the 'content' property of Cloud Datastore entities.

    The element is a line of text.  If the line is blank, note that, too.
    Args:
      element: the input entity to be processed
    Returns:
      A list of words found.
    """
    text_line = element.properties.get('content', '')
    if not text_line:
      self.empty_line_counter.inc()
      return None

    words = re.findall(r'[A-Za-z\']+', text_line)
    for w in words:
      self.word_length_counter.inc(len(w))
      self.word_lengths_dist.update(len(w))
      self.word_counter.inc()
    return words


class EntityWrapper(object):
  """Create a Cloud Datastore entity from the given string."""
  def __init__(self, project, namespace, kind, ancestor):
    self._project = project
    self._namespace = namespace
    self._kind = kind
    self._ancestor = ancestor

  def make_entity(self, content):
    ancestor_key = Key([self._kind, self._ancestor],
                       self._namespace,
                       self._project)
    # Namespace and project are inherited from parent key.
    key = Key([self._kind, str(uuid.uuid4())], parent=ancestor_key)
    entity = Entity(key)
    entity.set_properties({'content': content})
    return entity


def write_to_datastore(project, user_options, pipeline_options):
  """Creates a pipeline that writes entities to Cloud Datastore."""
  with beam.Pipeline(options=pipeline_options) as p:
    _ = (
        p
        | 'read' >> ReadFromText(user_options.input)
        | 'create entity' >> beam.Map(
            EntityWrapper(
                project,
                user_options.namespace,
                user_options.kind,
                user_options.ancestor).make_entity)
        | 'write to datastore' >> WriteToDatastore(project))


def make_ancestor_query(project, kind, namespace, ancestor):
  """Creates a Cloud Datastore ancestor query.

  The returned query will fetch all the entities that have the parent key name
  set to the given `ancestor`.
  """
  ancestor_key = Key([kind, ancestor], project=project, namespace=namespace)
  return Query(kind, project, namespace, ancestor_key)


def read_from_datastore(project, user_options, pipeline_options):
  """Creates a pipeline that reads entities from Cloud Datastore."""
  p = beam.Pipeline(options=pipeline_options)
  # Create a query to read entities from datastore.
  query = make_ancestor_query(
      project, user_options.kind, user_options.namespace, user_options.ancestor)

  # Read entities from Cloud Datastore into a PCollection.
  lines = p | 'read from datastore' >> ReadFromDatastore(query)

  # Count the occurrences of each word.
  def count_ones(word_ones):
    (word, ones) = word_ones
    return word, sum(ones)

  counts = (
      lines
      | 'split' >> beam.ParDo(WordExtractingDoFn())
      | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
      | 'group' >> beam.GroupByKey()
      | 'count' >> beam.Map(count_ones))

  # Format the counts into a PCollection of strings.
  def format_result(word_count):
    (word, count) = word_count
    return '%s: %s' % (word, count)

  output = counts | 'format' >> beam.Map(format_result)

  # Write the output using a "Write" transform that has side effects.
  # pylint: disable=expression-not-assigned
  output | 'write' >> beam.io.WriteToText(
      file_path_prefix=user_options.output, num_shards=user_options.num_shards)

  result = p.run()
  # Wait until completion, main thread would access post-completion job results.
  result.wait_until_finish()
  return result


def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
  parser.add_argument(
      '--kind', dest='kind', required=True, help='Datastore Kind')
  parser.add_argument(
      '--namespace', dest='namespace', help='Datastore Namespace')
  parser.add_argument(
      '--ancestor',
      dest='ancestor',
      default='root',
      help='The ancestor key name for all entities.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  parser.add_argument(
      '--read_only',
      action='store_true',
      help='Read an existing dataset, do not write first')
  parser.add_argument(
      '--num_shards',
      dest='num_shards',
      type=int,
      # If the system should choose automatically.
      default=0,
      help='Number of output shards')

  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  project = pipeline_options.view_as(GoogleCloudOptions).project
  if project is None:
    parser.print_usage()
    print(sys.argv[0] + ': error: argument --project is required')
    sys.exit(1)

  # Write to Datastore if `read_only` options is not specified.
  if not known_args.read_only:
    write_to_datastore(project, known_args, pipeline_options)

  # Read entities from Datastore.
  result = read_from_datastore(project, known_args, pipeline_options)

  empty_lines_filter = MetricsFilter().with_name('empty_lines')
  query_result = result.metrics().query(empty_lines_filter)
  if query_result['counters']:
    empty_lines_counter = query_result['counters'][0]
    logging.info('number of empty lines: %d', empty_lines_counter.committed)
  else:
    logging.warning('unable to retrieve counter metrics from runner')

  word_lengths_filter = MetricsFilter().with_name('word_len_dist')
  query_result = result.metrics().query(word_lengths_filter)
  if query_result['distributions']:
    word_lengths_dist = query_result['distributions'][0]
    logging.info('average word length: %d', word_lengths_dist.committed.mean)
  else:
    logging.warning('unable to retrieve distribution metrics from runner')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
