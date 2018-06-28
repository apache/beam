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

"""Helper to render pipeline graph in IPython when running interactively."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import re
import threading

import graphviz


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


def construct_digraph(vertex_dict, edge_dict):
  """Given vertices, edges and their attributes, construct graphviz.Digraph.

  Args:
    vertex_dict: (dict(str->(dict(str->str))) dict mapping vertex
        names to their attributes.
    edge_dict: (dict(str, str)->(dict(str -> str))) dict mapping edge names to
        their attributes.
  Returns:
    (graphviz.Digraph)
  """
  digraph = graphviz.Digraph()
  for vertex, vertex_attributes in vertex_dict.items():
    digraph.node(vertex, **vertex_attributes)
  for edge, edge_attributes in edge_dict.items():
    digraph.edge(edge[0], edge[1], **edge_attributes)
  return digraph


class PipelineGraph(object):
  """Creates a DOT graph representation of a pipeline. Thread-safe."""

  def __init__(self,
               pipeline_proto,
               required_transforms=None,
               referenced_pcollections=None,
               cached_pcollections=None,
               pcollection_stats=None):
    """Constructor of PipelineGraph.

    All fields except for pipeline_proto should be left as None unless used by
    interactive runner.

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
      pcollection_stats: Stats of PCollections.
    """
    self._pipeline_proto = pipeline_proto
    self._required_transforms = required_transforms or {}
    self._referenced_pcollections = referenced_pcollections or {}
    self._cached_pcollections = cached_pcollections or set()
    self._pcollection_stats = pcollection_stats or {}

    self._lock = threading.Lock()
    self._digraph = None  # of type graphviz.Digraph

  def get_dot(self):
    """Returns DOT representation of the pipeline."""
    return str(self._get_graph())

  def display_graph(self):
    """Displays graph via IPython or prints DOT if not possible."""
    try:
      from IPython.core import display  # pylint: disable=g-import-not-at-top
      display.display(display.HTML(self._get_graph()._repr_svg_()))  # pylint: disable=protected-access
    except ImportError:
      print(str(self._get_graph()))

  def _get_graph(self):
    """Returns graphviz.Digraph of the pipeline."""
    with self._lock:
      if self._digraph:
        return self._digraph

      vertex_dict, edge_dict = self._generate_graph_dicts()
      self._digraph = construct_digraph(vertex_dict, edge_dict)
      return self._digraph

  def _generate_graph_dicts(self):
    """From pipeline_proto and other info, generate the graph.

    Returns:
      vertex_dict: dict(str->dict(str, str)) vertex mapped to attributes.
      edge_dict: dict((str, str)->dict(str, str)) vertex pair mapped to the
          edge's attribute.
    """

    transforms = self._pipeline_proto.components.transforms

    def is_top_level_transform(transform):
      return transform.unique_name and '/' not in transform.unique_name

    def leaf_transforms(parent_id):
      parent = transforms[parent_id]
      if parent.subtransforms:
        for child in parent.subtransforms:
          for leaf in leaf_transforms(child):
            yield leaf
      else:
        yield parent_id

    consumers = collections.defaultdict(list)
    for transform_id, transform in transforms.items():
      if not is_top_level_transform(transform):
        continue
      for pcoll_id in transform.inputs.values():
        consumers[pcoll_id].append(transform_id)

    transform_dict = {}  # mapping transform names to properties
    pcoll_dict = {}  # mapping pcollection names to properties

    for transform_id, transform in transforms.items():
      if not is_top_level_transform(transform):
        continue
      source = transform.unique_name
      transform_dict[source] = {
          'required':
              all(
                  leaf in self._required_transforms
                  for leaf in leaf_transforms(transform_id))
      }

      for pcollection_id in transform.outputs.values():
        if pcollection_id not in consumers:
          invisible_leaf = 'leaf%s' % (hash(pcollection_id) % 10000)
          pcoll_consumers = [invisible_leaf]
          transform_dict[invisible_leaf] = {'leaf': True}
        else:
          pcoll_consumers = [
              transforms[consumer].unique_name
              for consumer in consumers[pcollection_id]
          ]
        data = {
            'cached': pcollection_id in self._cached_pcollections,
            'referenced': pcollection_id in self._referenced_pcollections
        }
        data.update(self._pcollection_stats.get(pcollection_id, {}))
        for consumer in pcoll_consumers:
          pcoll_dict[(source, consumer)] = data

    def vertex_properties_to_attributes(vertex):
      attrs = {}
      if 'leaf' in vertex:
        attrs['style'] = 'invis'
      elif vertex.get('required'):
        attrs['color'] = 'blue'
      else:
        attrs['color'] = 'grey'
      return attrs

    def edge_properties_to_attributes(edge):
      """Converts edge properties to Dot graph attributes for display."""
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

    vertex_dict = {}  # mapping vertex names to attributes
    edge_dict = {}  # mapping edge names to attributes

    for transform_name, transform_properties in transform_dict.items():
      vertex_dict[transform_name] = vertex_properties_to_attributes(
          transform_properties)

    for pcoll_name, pcoll_properties in pcoll_dict.items():
      edge_dict[pcoll_name] = edge_properties_to_attributes(pcoll_properties)

    return vertex_dict, edge_dict
