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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re

from apache_beam.runners.interactive import pipeline_graph


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

  def __init__(self,
               pipeline_proto,
               required_transforms=None,
               referenced_pcollections=None,
               cached_pcollections=None,
               pcollection_stats=None):
    """Constructor of PipelineGraph.

    Examples:
      pipeline_graph = PipelineGraph(pipeline_proto)
      print(pipeline_graph.get_dot())
      pipeline_graph.display_graph()

    Args:
      pipeline_proto: (Pipeline proto) Pipeline to be rendered.
      required_transforms: (dict from str to PTransform proto) Mapping from
          transform ID to transforms that leads to visible results.
      referenced_pcollections: (dict from str to PCollection proto) PCollection
          ID mapped to PCollection referenced during pipeline execution.
      cached_pcollections: (set of str) A set of PCollection IDs of those whose
          cached results are used in the execution.
    """
    self._pipeline_proto = pipeline_proto
    self._required_transforms = required_transforms or {}
    self._referenced_pcollections = referenced_pcollections or {}
    self._cached_pcollections = cached_pcollections or set()
    self._pcollection_stats = pcollection_stats or {}

    super(InteractivePipelineGraph, self).__init__(
        pipeline_proto=pipeline_proto,
        default_vertex_attrs={'color': 'gray', 'fontcolor': 'gray'},
        default_edge_attrs={'color': 'gray'}
    )

    transform_updates, pcollection_updates = self._generate_graph_update_dicts()
    self._update_graph(transform_updates, pcollection_updates)

  def display_graph(self):
    """Displays graph via IPython or prints DOT if not possible."""
    try:
      from IPython.core import display  # pylint: disable=import-error
      display.display(display.HTML(self._get_graph().create_svg()))  # pylint: disable=protected-access
    except ImportError:
      print(str(self._get_graph()))

  def _generate_graph_update_dicts(self):
    transforms = self._pipeline_proto.components.transforms

    transform_dict = {}  # maps PTransform IDs to properties
    pcoll_dict = {}  # maps PCollection IDs to properties

    def leaf_transform_ids(parent_id):
      parent = transforms[parent_id]
      if parent.subtransforms:
        for child in parent.subtransforms:
          for leaf in leaf_transform_ids(child):
            yield leaf
      else:
        yield parent_id

    for transform_id, transform in transforms.items():
      if not super(
          InteractivePipelineGraph, self)._is_top_level_transform(transform):
        continue

      transform_dict[transform.unique_name] = {
          'required':
              all(
                  leaf in self._required_transforms
                  for leaf in leaf_transform_ids(transform_id))
      }

      for pcoll_id in transform.outputs.values():
        properties = {
            'cached': pcoll_id in self._cached_pcollections,
            'referenced': pcoll_id in self._referenced_pcollections
        }

        # TODO(qinyeli): Enable updating pcollection_stats instead of creating a
        # new instance every time when pcollection_stats changes.
        properties.update(self._pcollection_stats.get(pcoll_id, {}))

        if pcoll_id not in self._consumers:
          invisible_leaf = 'leaf%s' % (hash(pcoll_id) % 10000)
          pcoll_dict[(transform.unique_name, invisible_leaf)] = properties
        else:
          for consumer in self._consumers[pcoll_id]:
            producer_name = transform.unique_name
            consumer_name = transforms[consumer].unique_name
            pcoll_dict[(producer_name, consumer_name)] = properties

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

      if 'sample' in edge:
        attrs['label'] = format_sample(edge['sample'], 1)
        attrs['labeltooltip'] = format_sample(edge['sample'], 10)
      else:
        attrs['label'] = '?'
      return attrs

    vertex_dict = {}  # maps vertex names to attributes
    edge_dict = {}  # maps edge names to attributes

    for transform_name, transform_properties in transform_dict.items():
      vertex_dict[transform_name] = vertex_properties_to_attributes(
          transform_properties)

    for pcoll_name, pcoll_properties in pcoll_dict.items():
      edge_dict[pcoll_name] = edge_properties_to_attributes(pcoll_properties)

    return vertex_dict, edge_dict
