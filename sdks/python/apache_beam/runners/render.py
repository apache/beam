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

"""A portable "runner" that renders a beam graph.

This runner can either render the graph to a (set of) output path(s), as
designated by (possibly repeated) --render_output, or serve the pipeline as
an interactive graph, if --render_port is set.

In Python, this runner can be passed directly at pipeline construction, e.g.::

   with beam.Pipeline(runner=beam.runners.render.RenderRunner(), options=...)

For other languages, start this service a by running::

  python -m apache_beam.runners.render --job_port=PORT ...

and then run your pipline with the PortableRunner setting the job endpoint
to `localhost:PORT`.

If any `--render_output=path.ext` flags are passed, each submitted job will
get written to the given output (overwriting any previously existing file).

If `--render_port` is set to a non-negative value, a local http server will
be started which allows for interactive exploration of the pipeline graph.

As an alternative to starting a job server, a single pipeline can be rendered
by passing a pipeline proto file to `--pipeline_proto`.  For example

  python -m apache_beam.runners.render  \\
      --pipeline_proto gs://<staging_location>/pipeline.pb  \\
      --render_output=/tmp/pipeline.svg

Requires the graphviz dot executable to be available in the path.
"""

import argparse
import base64
import collections
import http.server
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse

from google.protobuf import json_format
from google.protobuf import text_format

from apache_beam.options import pipeline_options
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import runner
from apache_beam.runners.portability import local_job_service
from apache_beam.runners.portability import local_job_service_main
from apache_beam.runners.portability.fn_api_runner import translations

try:
  from apache_beam.io.gcp import gcsio
except ImportError:
  gcsio = None  # type: ignore

_LOGGER = logging.getLogger(__name__)

# From the Beam site, circa November 2022.
DEFAULT_EDGE_STYLE = 'color="#ff570b"'
DEFAULT_TRANSFORM_STYLE = (
    'shape=rect style="rounded, filled" color="#ff570b" fillcolor="#fff6dd"')
DEFAULT_HIGHLIGHT_STYLE = (
    'shape=rect style="rounded, filled" color="#ff570b" fillcolor="#ffdb97"')


class RenderOptions(pipeline_options.PipelineOptions):
  """Rendering options."""
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--render_port',
        type=int,
        default=-1,
        help='The port at which to serve the graph. '
        'If 0, an unused port will be chosen. '
        'If -1, the server will not be started.')
    parser.add_argument(
        '--render_output',
        action='append',
        help='A path or paths to which to write rendered output. '
        'The output type will be deduced from the file extension.')
    parser.add_argument(
        '--render_leaf_composite_nodes',
        action='append',
        help='A set of regular expressions for transform names that should '
        'not be expanded.  For example, one could pass "\bRead.*" to indicate '
        'the inner structure of read nodes should not be expanded. '
        'If not given, defaults to the top-level nodes if interactively '
        'serving the graph and expanding all nodes otherwise.')
    parser.add_argument(
        '--render_edge_attributes',
        default='',
        help='Graphviz attributes to add to all edges.')
    parser.add_argument(
        '--render_node_attributes',
        default='',
        help='Graphviz attributes to add to all nodes.')
    parser.add_argument(
        '--render_highlight_attributes',
        default='',
        help='Graphviz attributes to add to all highlighted nodes.')
    parser.add_argument(
        '--log_proto',
        default=False,
        action='store_true',
        help='Set to also log input pipeline proto to stdout.')
    return parser


class PipelineRenderer:
  def __init__(self, pipeline, options):
    self.pipeline = pipeline
    self.options = options

    # Drill down into any uninteresting, top-level transforms that contain
    # the whole pipeline (often added by the SDK).
    roots = self.pipeline.root_transform_ids
    while len(roots) == 1:
      root = self.pipeline.components.transforms[roots[0]]
      if not root.subtransforms:
        break
      roots = root.subtransforms
    self.roots = roots

    # Figure out at what point to stop rendering composite internals.
    if options.render_leaf_composite_nodes:
      is_leaf = lambda transform_id: any(
          re.match(
              pattern,
              self.pipeline.components.transforms[transform_id].unique_name)
          for patterns in options.render_leaf_composite_nodes
          for pattern in patterns.split(','))
      self.leaf_composites = set()

      def mark_leaves(transform_ids):
        for transform_id in transform_ids:
          if is_leaf(transform_id):
            self.leaf_composites.add(transform_id)
          else:
            mark_leaves(
                self.pipeline.components.transforms[transform_id].subtransforms)

      mark_leaves(self.roots)

    elif options.render_port >= 0:
      # Start interactive with no unfolding.
      self.leaf_composites = set(self.roots)
    else:
      # For non-interactive, expand fully.
      self.leaf_composites = set()

    # Useful for attempting graph layout consistency.
    self.latest_positions = {}
    self.highlighted = []

  def update(self, toggle=None):
    if toggle:
      transform_id = toggle[0]
      self.highlighted = [transform_id]
      if transform_id in self.leaf_composites:
        transform = self.pipeline.components.transforms[transform_id]
        if transform.subtransforms:
          self.leaf_composites.remove(transform_id)
          for subtransform in transform.subtransforms:
            self.leaf_composites.add(subtransform)
            if transform_id in self.latest_positions:
              self.latest_positions[subtransform] = self.latest_positions[
                  transform_id]
      else:
        self.leaf_composites.add(transform_id)

  def style(self, transform_id):
    base = ' '.join(
        [DEFAULT_TRANSFORM_STYLE, self.options.render_node_attributes])
    if transform_id in self.highlighted:
      return ' '.join([
          base,
          DEFAULT_HIGHLIGHT_STYLE,
          self.options.render_highlight_attributes
      ])
    else:
      return base

  def to_dot(self):
    return '\n'.join(self.to_dot_iter())

  def to_dot_iter(self):
    yield 'digraph G {'
    # Defer drawing any edges until the end lest we declare nodes too early.
    edges_out = []
    for transform_id in self.roots:
      yield from self.transform_to_dot(
          transform_id, self.pcoll_leaf_consumers(), edges_out)
    yield from edges_out
    yield '}'

  def transform_to_dot(self, transform_id, pcoll_leaf_consumers, edges_out):
    transform = self.pipeline.components.transforms[transform_id]
    if self.is_leaf(transform_id):
      yield self.transform_node(transform_id)
      transform_inputs = set(transform.inputs.values())
      for name, output in transform.outputs.items():
        # For outputs that are also inputs, it's ambiguous whether they are
        # consumed as the outputs of this transform, or of the upstream
        # transform. Render the latter.
        if output in transform_inputs:
          continue
        output_label = name if len(transform.outputs) > 1 else ''
        for consumer, is_side_input in pcoll_leaf_consumers[output]:
          # Can't yield this here as the consumer might not be in this cluster.
          edge_style = 'dashed' if is_side_input else 'solid'
          edge_attributes = ' '.join([
              f'label="{output_label}" style={edge_style}',
              DEFAULT_EDGE_STYLE,
              self.options.render_edge_attributes
          ])
          edges_out.append(
              f'"{transform_id}" -> "{consumer}" [{edge_attributes}]')
    else:
      yield f'subgraph "cluster_{transform_id}" {{'
      yield self.transform_attributes(transform_id)
      for subtransform in transform.subtransforms:
        yield from self.transform_to_dot(
            subtransform, pcoll_leaf_consumers, edges_out)
      yield '}'

  def transform_node(self, transform_id):
    return f'"{transform_id}" [{self.transform_attributes(transform_id)}]'

  def transform_attributes(self, transform_id):
    transform = self.pipeline.components.transforms[transform_id]
    local_name = transform.unique_name.split('/')[-1]
    if transform_id in self.latest_positions:
      pos_str = f'pos="{self.latest_positions[transform_id]}"'
    else:
      pos_str = ''
    return (
        f'label="{local_name}" {self.style(transform_id)} '
        f'URL="javascript:click(\'{transform_id}\')" {pos_str}')

  def pcoll_leaf_consumers_iter(self, transform_id):
    transform = self.pipeline.components.transforms[transform_id]
    transform_inputs = set(transform.inputs.values())
    side_inputs = set(translations.side_inputs(transform).values())
    if self.is_leaf(transform_id):
      for pcoll in transform.inputs.values():
        yield pcoll, (transform_id, pcoll in side_inputs)
    for subtransform in transform.subtransforms:
      for pcoll, (consumer,
                  annotation) in self.pcoll_leaf_consumers_iter(subtransform):
        if self.is_leaf(transform_id):
          if pcoll not in transform_inputs:
            yield pcoll, (transform_id, annotation)
        else:
          yield pcoll, (consumer, annotation)

  def pcoll_leaf_consumers(self):
    result = collections.defaultdict(list)
    for transform_id in self.roots:
      for pcoll, consumer_info in self.pcoll_leaf_consumers_iter(transform_id):
        result[pcoll].append(consumer_info)
    return result

  def is_leaf(self, transform_id):
    return (
        transform_id in self.leaf_composites or
        not self.pipeline.components.transforms[transform_id].subtransforms)

  def info(self):
    if len(self.highlighted) != 1:
      return ''
    transform_id = self.highlighted[0]
    return f'<pre>{self.pipeline.components.transforms[transform_id]}</pre>'

  def layout_dot(self):
    layout = subprocess.run(['dot', '-Tdot'],
                            input=self.to_dot().encode('utf-8'),
                            capture_output=True,
                            check=True).stdout

    # Try to capture the positions for layout consistency.
    json_out = json.loads(
        subprocess.run(['dot', '-n2', '-Kneato', '-Tjson'],
                       input=layout,
                       capture_output=True,
                       check=True).stdout)
    for box in json_out['objects']:
      name = box.get('name', None)
      if name in self.pipeline.components.transforms:
        if 'pos' in box:
          self.latest_positions[name] = box['pos']
        elif 'bb' in box:
          x0, y0, x1, y1 = [float(r) for r in box['bb'].split(',')]
          self.latest_positions[name] = f'{(x0+x1)/2},{(y0+y1)/2}'

    return layout

  def page_callback_data(self, layout):
    svg = subprocess.run(['dot', '-Kneato', '-n2', '-Tsvg'],
                         input=layout,
                         capture_output=True,
                         check=True).stdout
    cmapx = subprocess.run(['dot', '-Kneato', '-n2', '-Tcmapx'],
                           input=layout,
                           capture_output=True,
                           check=True).stdout

    return {
        'src': 'data:image/svg+xml;base64,' +
        base64.b64encode(svg).decode('utf-8'),
        'cmapx': cmapx.decode('utf-8'),
        'info': self.info(),
    }

  def render_data(self):
    _LOGGER.info("Re-rendering pipeline...")
    layout = self.layout_dot()
    if self.options.render_output:
      for path in self.options.render_output:
        format = os.path.splitext(path)[-1][1:]
        result = subprocess.run(
            ['dot', '-Kneato', '-n2', '-T' + format, '-o', path],
            input=layout,
            check=False)
        if result.returncode:
          _LOGGER.error(
              "Failed render pipeline as %r: exit %s", path, result.returncode)
        else:
          _LOGGER.info("Rendered pipeline as %r", path)
    return self.page_callback_data(layout)

  def render_json(self):
    return json.dumps(self.render_data())

  def page(self):
    data = self.render_data()
    src = data['src']
    cmapx = data['cmapx']
    return """
        <html>
          <head>
          <script>
            function click(transform_id) {
              var xhttp = new XMLHttpRequest();
              xhttp.onreadystatechange = function() {
                render_data = JSON.parse(this.responseText);
                document.getElementById('image_map_holder').innerHTML =
                    render_data.cmapx;
                document.getElementById('image_tag').src = render_data.src
                document.getElementById('info').innerHTML = render_data.info
              };
              xhttp.open("GET", "render?toggle=" + transform_id, true);
              xhttp.send();
            }

          </script>
          </head>
          """ + f"""
          <body>
            Click on a composite transform to expand.
            <br>
            <img id='image_tag' src='{src}' usemap='#G'>
            <hr>
            <div id='info'></div>
            <div id='image_map_holder'>
            {cmapx}
            </div>
          </body>
        </html>
    """


class RenderRunner(runner.PipelineRunner):
  # TODO(robertwb): Consider making this a runner wrapper, where live status
  # (such as counters, stage completion status, or possibly even PCollection
  # samples) queryable and/or displayed.  This could evolve into a full Beam
  # UI.
  def run_pipeline(self, pipeline_object, options):
    return self.run_portable_pipeline(pipeline_object.to_runner_api(), options)

  def run_portable_pipeline(self, pipeline_proto, options):
    render_options = options.view_as(RenderOptions)
    if render_options.render_port < 0 and not render_options.render_output:
      raise ValueError(
          'At least one of --render_port or --render_output must be provided.')
    if render_options.log_proto:
      _LOGGER.info(pipeline_proto)
    renderer = PipelineRenderer(pipeline_proto, render_options)
    try:
      subprocess.run(['dot', '-V'], capture_output=True, check=True)
    except FileNotFoundError as exn:
      # If dot is not available, we can at least output the raw .dot files.
      dot_files = [
          output for output in render_options.render_output
          if output.endswith('.dot')
      ]
      for output in dot_files:
        with open(output, 'w') as fout:
          fout.write(renderer.to_dot())
          _LOGGER.info("Wrote pipeline as %s", output)

      non_dot_files = set(render_options.render_output) - set(dot_files)
      if non_dot_files:
        raise RuntimeError(
            "Graphviz dot executable not available "
            f"for rendering non-dot output files {non_dot_files}") from exn
      elif render_options.render_port >= 0:
        raise RuntimeError(
            "Graphviz dot executable not available for serving") from exn

      return RenderPipelineResult(None)

    renderer.page()

    if render_options.render_port >= 0:
      # TODO: If this gets more complex, we could consider taking on a
      # framework like Flask as a dependency.
      class RequestHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
          parts = urllib.parse.urlparse(self.path)
          args = urllib.parse.parse_qs(parts.query)
          renderer.update(**args)

          if parts.path == '/':
            response = renderer.page()
          elif parts.path == '/render':
            response = renderer.render_json()
          else:
            self.send_response(400)
            return

          self.send_response(200)
          self.send_header("Content-type", "text/html")
          self.end_headers()
          self.wfile.write(response.encode('utf-8'))

      server = http.server.HTTPServer(('localhost', render_options.render_port),
                                      RequestHandler)
      server_thread = threading.Thread(target=server.serve_forever, daemon=True)
      server_thread.start()
      print('Serving at http://%s:%s' % server.server_address)
      return RenderPipelineResult(server)

    else:
      return RenderPipelineResult(None)


class RenderPipelineResult(runner.PipelineResult):
  def __init__(self, server):
    super().__init__(runner.PipelineState.RUNNING)
    self.server = server

  def wait_until_finish(self, duration=None):
    if self.server:
      time.sleep(duration or 1e8)
      self.server.shutdown()
    self._state = runner.PipelineState.DONE

  def monitoring_infos(self):
    return []


def run(argv):
  if argv[0] == __file__:
    argv = argv[1:]
  parser = argparse.ArgumentParser(
      description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument(
      '--job_port',
      type=int,
      default=0,
      help='port on which to serve the job api')
  parser.add_argument(
      '--pipeline_proto', help='file containing the beam pipeline definition')
  RenderOptions._add_argparse_args(parser)
  options = parser.parse_args(argv)

  if options.pipeline_proto:
    if not options.render_output and options.render_port < 0:
      options.render_port = 0

    render_one(options)

    if options.render_output:
      return

  run_server(options)


def render_one(options):
  if options.pipeline_proto == '-':
    content = sys.stdin.buffer.read()
    if content[0] == b'{':
      ext = '.json'
    else:
      try:
        content.decode('utf-8')
        ext = '.textproto'
      except UnicodeDecodeError:
        ext = '.pb'
  else:
    if options.pipeline_proto.startswith('gs://'):
      if gcsio is None:
        raise ImportError('GCS not available; please install apache_beam[gcp]')
      open_fn = gcsio.GcsIO().open
    else:
      open_fn = open

    with open_fn(options.pipeline_proto, 'rb') as fin:
      content = fin.read()
    ext = os.path.splitext(options.pipeline_proto)[-1]

  if ext == '.textproto':
    pipeline_proto = text_format.Parse(content, beam_runner_api_pb2.Pipeline())
  elif ext == '.json':
    pipeline_proto = json_format.Parse(content, beam_runner_api_pb2.Pipeline())
  else:
    pipeline_proto = beam_runner_api_pb2.Pipeline()
    pipeline_proto.ParseFromString(content)

  RenderRunner().run_portable_pipeline(
      pipeline_proto, pipeline_options.PipelineOptions(**vars(options)))


def run_server(options):
  class RenderBeamJob(local_job_service.BeamJob):
    def _invoke_runner(self):
      return RenderRunner().run_portable_pipeline(
          self._pipeline_proto,
          pipeline_options.PipelineOptions(**vars(options)))

  with tempfile.TemporaryDirectory() as staging_dir:
    job_servicer = local_job_service.LocalJobServicer(
        staging_dir, beam_job_type=RenderBeamJob)
    port = job_servicer.start_grpc_server(options.job_port)
    try:
      local_job_service_main.serve("Listening for beam jobs on port %d." % port)
    finally:
      job_servicer.stop()


if __name__ == '__main__':
  logging.basicConfig()
  logging.getLogger().setLevel(logging.INFO)
  run(sys.argv)
