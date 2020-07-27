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

"""Messaging mechanism to inspect the interactive environment.

A singleton instance is accessible from
interactive_environment.current_env().inspector.
"""
# pytype: skip-file

from __future__ import absolute_import

import apache_beam as beam
from apache_beam.runners.interactive.utils import as_json
from apache_beam.runners.interactive.utils import obfuscate


class InteractiveEnvironmentInspector(object):
  """Inspector that converts information of the current interactive environment
  including pipelines and pcollections into JSON data suitable for messaging
  with applications within/outside the Python kernel.

  The usage is always that the application side reads the inspectables or
  list_inspectables first then communicates back to the kernel and get_val for
  usage on the kernel side.
  """
  def __init__(self):
    self._inspectables = {}
    self._anonymous = {}

  @property
  def inspectables(self):
    """Lists pipelines and pcollections assigned to variables as inspectables.
    """
    self._inspectables = inspect()
    return self._inspectables

  @as_json
  def list_inspectables(self):
    """Lists inspectables in JSON format.

    When listing, pcollections are organized by the pipeline they belong to.
    If a pipeline is no longer assigned to a variable but its pcollections
    assigned to variables are still in scope, the pipeline will be given a name
    as 'anonymous_pipeline[id:$inMemoryId]'.
    The listing doesn't contain object values of the pipelines or pcollections.
    The obfuscated identifier can be used to trace back to those values in the
    kernel.
    The listing includes anonymous pipelines that are not assigned to variables
    but still containing inspectable PCollections.
    """
    listing = {}
    pipelines = inspect_pipelines()
    for pipeline, name in pipelines.items():
      metadata = meta(name, pipeline)
      listing[obfuscate(metadata)] = {'metadata': metadata, 'pcolls': {}}
    for identifier, inspectable in self.inspectables.items():
      if inspectable['metadata']['type'] == 'pcollection':
        pipeline = inspectable['value'].pipeline
        if pipeline not in list(pipelines.keys()):
          pipeline_name = synthesize_pipeline_name(pipeline)
          pipelines[pipeline] = pipeline_name
          pipeline_metadata = meta(pipeline_name, pipeline)
          pipeline_identifier = obfuscate(pipeline_metadata)
          self._anonymous[pipeline_identifier] = {
              'metadata': pipeline_metadata, 'value': pipeline
          }
          listing[pipeline_identifier] = {
              'metadata': pipeline_metadata,
              'pcolls': {
                  identifier: inspectable['metadata']
              }
          }
        else:
          pipeline_identifier = obfuscate(meta(pipelines[pipeline], pipeline))
          listing[pipeline_identifier]['pcolls'][identifier] = inspectable[
              'metadata']
    return listing

  def get_val(self, identifier):
    """Retrieves the in memory object itself by identifier.

    The retrieved object could be a pipeline or a pcollection. If the
    identifier is not recognized, return None.
    The identifier can refer to an anonymous pipeline and the object will still
    be retrieved.
    """
    inspectable = self._inspectables.get(identifier, None)
    if inspectable:
      return inspectable['value']
    inspectable = self._anonymous.get(identifier, None)
    if inspectable:
      return inspectable['value']
    return None

  def get_pcoll_data(self, identifier, include_window_info=False):
    """Retrieves the json formatted PCollection data.

    If no PCollection value can be retieved from the given identifier, an empty
    json string will be returned.
    """
    value = self.get_val(identifier)
    if isinstance(value, beam.pvalue.PCollection):
      from apache_beam.runners.interactive import interactive_beam as ib
      dataframe = ib.collect(value, include_window_info)
      return dataframe.to_json(orient='table')
    return {}


def inspect():
  """Inspects current interactive environment to track metadata and values of
  pipelines and pcollections.

  Each pipeline and pcollections tracked is given a unique identifier.
  """
  from apache_beam.runners.interactive import interactive_environment as ie

  inspectables = {}
  for watching in ie.current_env().watching():
    for name, value in watching:
      # Ignore synthetic vars created by Interactive Beam itself.
      if name.startswith('synthetic_var_'):
        continue
      metadata = meta(name, value)
      identifier = obfuscate(metadata)
      if isinstance(value, (beam.pipeline.Pipeline, beam.pvalue.PCollection)):
        inspectables[identifier] = {'metadata': metadata, 'value': value}
  return inspectables


def inspect_pipelines():
  """Inspects current interactive environment to track all pipelines assigned
  to variables. The keys are pipeline objects and values are pipeline names.
  """
  from apache_beam.runners.interactive import interactive_environment as ie

  pipelines = {}
  for watching in ie.current_env().watching():
    for name, value in watching:
      if isinstance(value, beam.pipeline.Pipeline):
        pipelines[value] = name
  return pipelines


def meta(name, val):
  """Generates meta data for the given name and value."""
  return {
      'name': name, 'inMemoryId': id(val), 'type': type(val).__name__.lower()
  }


def synthesize_pipeline_name(val):
  """Synthesizes a pipeline name for the given pipeline object."""
  return 'anonymous_pipeline[id:{}]'.format(id(val))
