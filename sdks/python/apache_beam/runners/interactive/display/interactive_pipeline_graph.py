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

"""Helper to render pipeline graph in IPython when running interactively.

This module is experimental. No backwards-compatibility guarantees.
"""

# pytype: skip-file

import re

from apache_beam.runners.interactive.display import pipeline_graph


def nice_str(o):
  s = repr(o)
  s = s.replace('"', "'")
  s = s.replace('\\', '|')
  s = re.sub(r'[^\x20-\x7F]', ' ', s)
  assert '"' not in s
  if len(s) > 35:
    s = s[:35] + '...'
  return s


def format_sample(contents, count=1000):
  contents = list(contents)
  elems = ', '.join([nice_str(o) for o in contents[:count]])
  if len(contents) > count:
    elems += ', ...'
  assert '"' not in elems
  return '{%s}' % elems


class InteractivePipelineGraph(pipeline_graph.PipelineGraph):
  """Creates the DOT representation of an interactive pipeline. Thread-safe."""
  def __init__(
      self,
      pipeline,
      required_transforms=None,
      referenced_pcollections=None,
      cached_pcollections=None):
    """Constructor of PipelineGraph.

    Args:
      pipeline: (Pipeline proto) or (Pipeline) pipeline to be rendered.
      required_transforms: (list/set of str) ID of top level PTransforms that
          lead to visible results.
      referenced_pcollections: (list/set of str) ID of PCollections that are
          referenced by top level PTransforms executed (i.e.
          required_transforms)
      cached_pcollections: (set of str) a set of PCollection IDs of those whose
          cached results are used in the execution.
    """
    self._required_transforms = required_transforms or set()
    self._referenced_pcollections = referenced_pcollections or set()
    self._cached_pcollections = cached_pcollections or set()

    super().__init__(
        pipeline=pipeline,
        default_vertex_attrs={
            'color': 'gray', 'fontcolor': 'gray'
        },
        default_edge_attrs={'color': 'gray'})

    transform_updates, pcollection_updates = self._generate_graph_update_dicts()
    self._update_graph(transform_updates, pcollection_updates)

  def update_pcollection_stats(self, pcollection_stats):
    """Updates PCollection stats.

    Args:
      pcollection_stats: (dict of dict) maps PCollection IDs to informations. In
          particular, we only care about the field 'sample' which should be a
          the PCollection result in as a list.
    """
    edge_dict = {}
    for pcoll_id, stats in pcollection_stats.items():
      attrs = {}
      pcoll_list = stats['sample']
      if pcoll_list:
        attrs['label'] = format_sample(pcoll_list, 1)
        attrs['labeltooltip'] = format_sample(pcoll_list, 10)
      else:
        attrs['label'] = '?'
      edge_dict[pcoll_id] = attrs

    self._update_graph(edge_dict=edge_dict)

  def _generate_graph_update_dicts(self):
    """Generate updates specific to interactive pipeline.

    Returns:
      vertex_dict: (Dict[str, Dict[str, str]]) maps vertex name to attributes
      edge_dict: (Dict[str, Dict[str, str]]) maps vertex name to attributes
    """
    transform_dict = {}  # maps PTransform IDs to properties
    pcoll_dict = {}  # maps PCollection IDs to properties

    for transform_id, transform_proto in self._top_level_transforms():
      transform_dict[transform_proto.unique_name] = {
          'required': transform_id in self._required_transforms
      }

      for pcoll_id in transform_proto.outputs.values():
        pcoll_dict[pcoll_id] = {
            'cached': pcoll_id in self._cached_pcollections,
            'referenced': pcoll_id in self._referenced_pcollections
        }

    def vertex_properties_to_attributes(vertex):
      """Converts PCollection properties to DOT vertex attributes."""
      attrs = {}
      if 'leaf' in vertex:
        attrs['style'] = 'invis'
      elif vertex.get('required'):
        attrs['color'] = 'blue'
        attrs['fontcolor'] = 'blue'
      else:
        attrs['color'] = 'grey'
      return attrs

    def edge_properties_to_attributes(edge):
      """Converts PTransform properties to DOT edge attributes."""
      attrs = {}
      if edge.get('cached'):
        attrs['color'] = 'red'
      elif edge.get('referenced'):
        attrs['color'] = 'black'
      else:
        attrs['color'] = 'grey'
      return attrs

    vertex_dict = {}  # maps vertex names to attributes
    edge_dict = {}  # maps edge names to attributes

    for transform_name, transform_properties in transform_dict.items():
      vertex_dict[transform_name] = vertex_properties_to_attributes(
          transform_properties)

    for pcoll_id, pcoll_properties in pcoll_dict.items():
      edge_dict[pcoll_id] = edge_properties_to_attributes(pcoll_properties)

    return vertex_dict, edge_dict
