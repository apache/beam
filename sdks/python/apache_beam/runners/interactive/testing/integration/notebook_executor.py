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

"""Module to execute jupyter notebooks and gather the output into renderable
HTML files."""

# pytype: skip-file

from __future__ import absolute_import

import os
import shutil
import subprocess

from apache_beam.runners.interactive.utils import obfuscate

try:
  import nbformat
  from jupyter_client.kernelspec import KernelSpecManager
  from nbconvert.preprocessors import ExecutePreprocessor
  _interactive_integration_ready = True
except ImportError:
  _interactive_integration_ready = False


class NotebookExecutor(object):
  """Executor that reads notebooks, executes it and gathers outputs into static
  HTML pages that can be served."""
  def __init__(self, path):
    # type: (str) -> None

    assert _interactive_integration_ready, (
        '[interactive_test] dependency is not installed.')
    assert os.path.exists(path), '{} does not exist.'.format(path)
    self._paths = []
    if os.path.isdir(path):
      for root, _, files in os.walk(path):
        for filename in files:
          if filename.endswith('.ipynb'):
            self._paths.append(os.path.join(root, filename))
    elif path.endswith('.ipynb'):
      self._paths.append(path)
    assert len(
        self._paths) > 0, ('No notebooks to be executed under{}'.format(path))
    self._dir = os.path.dirname(self._paths[0])
    self._output_html_dir = os.path.join(self._dir, 'output')
    self.cleanup()
    self._output_html_paths = {}
    self._notebook_path_to_execution_id = {}
    kernel_specs = KernelSpecManager().get_all_specs()
    if 'test' not in kernel_specs:
      # Install a test ipython kernel in current runtime environment. If this
      # errors out, it means the test env is broken and should fail the test.
      process = subprocess.run(
          ['python', '-m', 'ipykernel', 'install', '--user', '--name', 'test'],
          check=True)
      process.check_returncode()

  def cleanup(self):
    """Cleans up the output folder."""
    _cleanup(self._output_html_dir)

  def execute(self):
    """Executes all notebooks found in the scoped path and gathers their
    outputs into HTML pages stored in the output folder."""
    for path in self._paths:
      with open(path, 'r') as nb_f:
        nb = nbformat.read(nb_f, as_version=4)
        ep = ExecutePreprocessor(allow_errors=True, kernel_name='test')
        ep.preprocess(nb, {'metadata': {'path': os.path.dirname(path)}})

      execution_id = obfuscate(path)
      output_html_path = os.path.join(
          self._output_html_dir, execution_id + '.html')
      with open(output_html_path, 'a+') as sink:
        sink.write('<html>\n')
        sink.write('<head>\n')
        sink.write('</head>\n')
        sink.write('<body>\n')
        for cell in nb['cells']:
          if cell['cell_type'] == 'code':
            for output in cell['outputs']:
              _extract_html(output, sink)
        sink.write('</body>\n')
        sink.write('</html>\n')
      self._output_html_paths[execution_id] = output_html_path
      self._notebook_path_to_execution_id[path] = execution_id

  @property
  def output_html_paths(self):
    """Mapping from execution ids to output html page paths.

    An execution/test id is an obfuscated value from the executed notebook path.
    It identifies the input notebook, the output html, the screenshot of the
    output html, and the golden screenshot for comparison.
    """
    return self._output_html_paths

  @property
  def output_html_dir(self):
    """The directory's path to all the output html pages generated."""
    return self._output_html_dir

  @property
  def notebook_path_to_execution_id(self):
    """Mapping from input notebook paths to their obfuscated execution ids."""
    return self._notebook_path_to_execution_id


def _cleanup(output_dir):
  """Cleans up the given output_dir."""
  if os.path.exists(output_dir):
    shutil.rmtree(output_dir)
  os.makedirs(output_dir)


def _extract_html(output, sink):
  """Extracts html elements from the output of an executed notebook node and
  writes them into a file sink."""
  if output['output_type'] == 'display_data':
    data = output['data']
    if 'application/javascript' in data:
      sink.write('<script>\n')
      sink.write(data['application/javascript'])
      sink.write('</script>\n')
    if 'text/html' in data:
      sink.write(data['text/html'])
