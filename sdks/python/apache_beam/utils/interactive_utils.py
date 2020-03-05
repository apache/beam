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

"""Common interactive utility module.

For experimental usage only; no backwards-compatibility guarantees.
"""
# pytype: skip-file

from __future__ import absolute_import

import logging

_LOGGER = logging.getLogger(__name__)


def is_in_ipython():
  """Determines if current code is executed within an ipython session."""
  is_in_ipython = False
  # Check if the runtime is within an interactive environment, i.e., ipython.
  try:
    from IPython import get_ipython  # pylint: disable=import-error
    if get_ipython():
      is_in_ipython = True
  except ImportError:
    pass  # If dependencies are not available, then not interactive for sure.
  return is_in_ipython


def is_in_notebook():
  """Determines if current code is executed from an ipython notebook.

  If is_in_notebook() is True, then is_in_ipython() must also be True.
  """
  is_in_notebook = False
  if is_in_ipython():
    # The import and usage must be valid under the execution path.
    from IPython import get_ipython
    if 'IPKernelApp' in get_ipython().config:
      is_in_notebook = True
  return is_in_notebook


def alter_label_if_ipython(transform, pvalueish):
  """Alters the label to an interactive label with ipython prompt metadata
  prefixed for the given transform if the given pvalueish belongs to a
  user-defined pipeline and current code execution is within an ipython kernel.
  Otherwise, noop.

  A label is either a user-defined or auto-generated str name of a PTransform
  that is unique within a pipeline. If current environment is_in_ipython(), Beam
  can implicitly create interactive labels to replace labels of top-level
  PTransforms to be applied. The label is formatted as:
  `Cell {prompt}: {original_label}`.
  """
  if is_in_ipython():
    from apache_beam.runners.interactive import interactive_environment as ie
    # Tracks user defined pipeline instances in watched scopes so that we only
    # alter labels for any transform to pvalueish belonging to those pipeline
    # instances, excluding any transform to be applied in other pipeline
    # instances the Beam SDK creates implicitly.
    ie.current_env().track_user_pipelines()
    from IPython import get_ipython
    prompt = get_ipython().execution_count
    pipeline = _extract_pipeline_of_pvalueish(pvalueish)
    if not pipeline:
      _LOGGER.warning(
          'Failed to alter the label of a transform with the '
          'ipython prompt metadata. Cannot figure out the pipeline '
          'that the given pvalueish %s belongs to. Thus noop.',
          pvalueish)
    if (pipeline
        # We only alter for transforms to be applied to user-defined pipelines
        # at pipeline construction time.
        and pipeline in ie.current_env().tracked_user_pipelines):
      transform.label = 'Cell {}: {}'.format(prompt, transform.label)


def _extract_pipeline_of_pvalueish(pvalueish):
  """Extracts the pipeline that the given pvalueish belongs to."""
  if isinstance(pvalueish, tuple):
    pvalue = pvalueish[0]
  elif isinstance(pvalueish, dict):
    pvalue = next(iter(pvalueish.values()))
  else:
    pvalue = pvalueish
  if hasattr(pvalue, 'pipeline'):
    return pvalue.pipeline
  return None


# TODO(BEAM-8335): Move this function and the IPythonLogHandler to a util class
# under interactive package when streaming cache changes are merged.
def register_ipython_log_handler():
  """Adds the IPython handler to a dummy parent logger (named
  'apache_beam.runners.interactive') of all interactive modules' loggers so that
  if is_in_notebook, logging displays the logs as HTML in frontends."""
  # apache_beam.runners.interactive is not a module, thus this "root" logger is
  # a dummy one created to hold the IPython log handler. When children loggers
  # have propagate as True (by default) and logging level as NOTSET (by default,
  # so the "root" logger's logging level takes effect), the IPython log handler
  # will be triggered at the "root"'s own logging level. And if a child logger
  # sets its logging level, it can take control back.
  interactive_root_logger = logging.getLogger('apache_beam.runners.interactive')
  if any([isinstance(h, IPythonLogHandler)
          for h in interactive_root_logger.handlers]):
    return
  interactive_root_logger.setLevel(logging.INFO)
  interactive_root_logger.addHandler(IPythonLogHandler())
  # Disable the propagation so that logs emitted from interactive modules should
  # only be handled by loggers and handlers defined within interactive packages.
  interactive_root_logger.propagate = False


class IPythonLogHandler(logging.Handler):
  """A logging handler to display logs as HTML in IPython backed frontends."""
  log_template = """
            <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css" integrity="sha384-Vkoo8x4CGsO3+Hhxv8T/Q5PaXtkKtu6ug5TOeNV6gBiFeWPGFN9MuhOf23Q9Ifjh" crossorigin="anonymous">
            <div class="alert alert-{level}">{msg}</div>"""

  logging_to_alert_level_map = {
      logging.CRITICAL: 'danger',
      logging.ERROR: 'danger',
      logging.WARNING: 'warning',
      logging.INFO: 'info',
      logging.DEBUG: 'dark',
      logging.NOTSET: 'light'
  }

  def emit(self, record):
    try:
      from html import escape
      from IPython.core.display import HTML
      from IPython.core.display import display
      display(
          HTML(
              self.log_template.format(
                  level=self.logging_to_alert_level_map[record.levelno],
                  msg=escape(record.msg))))
    except ImportError:
      pass  # NOOP when dependencies are not available.
