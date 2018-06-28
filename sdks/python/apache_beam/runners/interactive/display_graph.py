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


def render_dot(line):
  """Renders an SVG."""
  from graphviz import Source  # pylint: disable=g-import-not-at-top
  source = Source(line)
  return source._repr_svg_()  # pylint: disable=protected-access


def display_dot(dot_graph):
  try:
    from IPython.core.display import display  # pylint: disable=g-import-not-at-top
    from IPython.core.display import HTML  # pylint: disable=g-import-not-at-top
    return display(HTML(render_dot(dot_graph)))
  except ImportError:
    print(dot_graph)
    return


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


def display_dot_beam(graph_label, vertex_dict, edge_dict):
  """Creates a Dot graph based on the input vertices and edges, with tooltips.

  Renders an SVG.

  Args:
    graph_label: A name for the graph being rendered.
    vertex_dict: A map of vertex names to attributes.
    edge_dict: A map of edges (source-target pairs) to edge attributes.
  """
  dot_header = 'digraph G { rankdir=TD labelloc=t  label="' + graph_label + '" '
  dot_footer = '}'

  def format_attributes(attr_dict):
    return '[ %s ]' % ', '.join(
        '%s="%s"' % (key, value) for key, value in attr_dict.items())

  def vertex_properties_to_attributes(vertex):
    if 'leaf' in vertex:
      color = 'white'
    elif vertex.get('required'):
      color = 'blue'
    else:
      color = 'grey'
    return {
        'color': color,
        'fontcolor': color,
    }

  def edge_properties_to_attributes(edge):
    """Converts edge properties to Dot graph attributes for display."""
    if edge.get('cached'):
      color = 'red'
    elif edge.get('referenced'):
      color = 'black'
    else:
      color = 'grey'
    data = {
        'color': color,
    }
    if 'sample' in edge:
      data['label'] = format_sample(edge['sample'], 1)
      data['labeltooltip'] = format_sample(edge['sample'], 10)
    else:
      data['label'] = '?'
    return data

  vertex_str = ''
  for vertex, attributes in vertex_dict.items():
    vertex_str += '\n  "%s" %s' % (
        vertex, format_attributes(vertex_properties_to_attributes(attributes)))

  edge_str = ''
  for edge, attributes in edge_dict.items():
    edge_str += '\n  "%s" -> "%s" %s' % (
        edge[0], edge[1],
        format_attributes(edge_properties_to_attributes(attributes)))

  display_dot(dot_header + vertex_str + edge_str + dot_footer)


def display_pipeline(pipeline_proto, required_transforms,
                     referenced_pcollections, cached_pcollections,
                     pcollection_stats):
  """Creates a Dot graph based on the input pipeline, with tooltips.

  Renders an SVG.

  Args:
    pipeline_proto: The pipeline proto to be rendered.
    required_transforms: Set of transforms in the pipeline.
    referenced_pcollections: Set of PCollections referenced in the pipeline.
    cached_pcollections: Set of PCollections with previously cached values.
    pcollection_stats: Samples of PCollections.
  """
  vertex_dict = {}
  edge_dict = {}

  def is_top_level_transform(transform):
    return transform.unique_name and '/' not in transform.unique_name

  def leaf_transforms(parent_id):
    parent = pipeline_proto.components.transforms[parent_id]
    if parent.subtransforms:
      for child in parent.subtransforms:
        for leaf in leaf_transforms(child):
          yield leaf
    else:
      yield parent_id

  consumers = collections.defaultdict(list)
  for transform_id, transform in pipeline_proto.components.transforms.items():
    if not is_top_level_transform(transform):
      continue
    for pcoll_id in transform.inputs.values():
      consumers[pcoll_id].append(transform_id)

  for transform_id, transform in pipeline_proto.components.transforms.items():
    if not is_top_level_transform(transform):
      continue
    source = transform.unique_name
    vertex_dict[source] = {
        'required':
            all(
                leaf in required_transforms
                for leaf in leaf_transforms(transform_id))
    }

    for pcollection_id in transform.outputs.values():
      if pcollection_id not in consumers:
        invisible_leaf = 'leaf%s' % (hash(pcollection_id) % 10000)
        pcoll_consumers = [invisible_leaf]
        vertex_dict[invisible_leaf] = {'leaf': True}
      else:
        pcoll_consumers = [
            pipeline_proto.components.transforms[consumer].unique_name
            for consumer in consumers[pcollection_id]
        ]
      data = {
          'cached': pcollection_id in cached_pcollections,
          'referenced': pcollection_id in referenced_pcollections
      }
      data.update(pcollection_stats.get(pcollection_id, {}))
      for consumer in pcoll_consumers:
        edge_dict[(source, consumer)] = data

  display_dot_beam('', vertex_dict, edge_dict)
