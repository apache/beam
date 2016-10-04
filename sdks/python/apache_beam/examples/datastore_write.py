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

"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import logging
import re
import uuid

from functools import partial

from google.cloud.datastore.key import Key
from google.cloud.datastore.entity import Entity

import apache_beam as beam
from apache_beam.utils.options import GoogleCloudOptions
from apache_beam.utils.options import PipelineOptions
from apache_beam.utils.options import SetupOptions

def toEntity(content, kind, project, namespace):
  id = str(uuid.uuid4())
  key = Key(kind, id, project=project, namespace=namespace)
  entity = Entity(key)
  entity['content'] = content
  return entity


def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument('--kind',
                      dest='kind',
                      default='shakespeare-demo',
                      help='Datastore Kind')
  parser.add_argument('--namespace',
                      dest='namespace',
                      help='Datastore Namespace')
  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  gcloud_options = pipeline_options.view_as(GoogleCloudOptions)
  p = beam.Pipeline(options=pipeline_options)

  input = [u"hello", u"how", u"are", u"you"]
  output = (p | 'input' >> beam.Create(input).with_output_types(unicode)
              | 'to entity' >> beam.Map(partial(toEntity,
                kind=known_args.kind, project=gcloud_options.project,
                namespace=known_args.namespace))
              | 'write to datastore' >> beam.io.WriteToDatastore(
                gcloud_options.project, known_args.namespace))


  # Actually run the pipeline (all operations above are deferred).
  result = p.run()
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
